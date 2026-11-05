"""Sending and receiving messages."""

from __future__ import annotations

import enum
import socket
import threading
import weakref
from functools import wraps
from itertools import count
from threading import local
from time import monotonic
from typing import TYPE_CHECKING

from .common import maybe_declare
from .compression import compress
from .connection import PooledConnection, is_connection, maybe_channel
from .entity import Exchange, Queue, maybe_delivery_mode
from .exceptions import ContentDisallowed
from .serialization import dumps, prepare_accept_content
from .utils.functional import ChannelPromise, maybe_list

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    'Exchange', 'Queue', 'Producer', 'Consumer',
    'ConsumerShutdown', 'ConsumerShutdownResult',
)

DEFAULT_BATCH_SIZE = 1000


class _ProducerBatchState:
    """State shared by nested batch contexts in one producer thread."""

    def __init__(self, max_size):
        self.max_size = max_size
        self.aborted = False
        self.session = None
        self.immediate = False

    def _get_session(self, channel):
        if self.immediate:
            return None
        if self.session is None:
            create_batch = getattr(channel, 'create_publish_batch', None)
            supports_batch = getattr(channel, 'supports_batch_publish', True)
            if create_batch is None or not supports_batch:
                self.immediate = True
                return None
            self.session = create_batch(max_size=self.max_size)
        elif self.session.channel is not channel:
            raise RuntimeError(
                'Producer channel changed while a publish batch was active',
            )
        return self.session

    def publish(self, channel, message, **kwargs):
        if self.aborted:
            raise RuntimeError('Cannot publish through an aborted batch')
        session = self._get_session(channel)
        if session is None:
            return channel.basic_publish(message, **kwargs)
        return session.publish(message, **kwargs)

    def flush(self):
        if self.aborted:
            raise RuntimeError('Cannot flush an aborted batch')
        if self.session is not None:
            self.session.flush()

    def abort(self):
        if self.aborted:
            return
        self.aborted = True
        if self.session is not None:
            self.session.discard()

    def close(self):
        if self.session is not None:
            self.session.close()


class _ProducerBatch:
    """Context manager returned by :meth:`Producer.batch`."""

    def __init__(self, producer, max_size):
        self.producer = producer
        self.max_size = max_size
        self.state = None
        self.is_outermost = False
        self.entered = False
        self.active = False

    def __enter__(self):
        if self.entered:
            raise RuntimeError('Publish batch context cannot be re-entered')
        self.entered = True
        self.active = True

        state = getattr(self.producer._batch_local, 'current', None)
        if state is None:
            state = _ProducerBatchState(self.max_size)
            self.producer._batch_local.current = state
            self.is_outermost = True
        self.state = state
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        state = self.state
        if state is None:
            return None

        cleanup_error = None
        try:
            if self.is_outermost:
                del self.producer._batch_local.current

            if exc_type is not None:
                try:
                    state.abort()
                except BaseException as exc:
                    cleanup_error = exc

            if self.is_outermost:
                if exc_type is None and not state.aborted:
                    try:
                        state.flush()
                    except BaseException as exc:
                        cleanup_error = exc
                try:
                    state.close()
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
        finally:
            self.active = False

        if exc_type is None and cleanup_error is not None:
            raise cleanup_error
        return None

    def flush(self):
        """Flush messages buffered by the active batch."""
        if not self.active:
            raise RuntimeError('Publish batch context is not active')
        self.state.flush()


class Producer:
    """Message Producer.

    Arguments:
    ---------
        channel (kombu.Connection, ChannelT): Connection or channel.
        exchange (kombu.entity.Exchange, str): Optional default exchange.
        routing_key (str): Optional default routing key.
        serializer (str): Default serializer. Default is `"json"`.
        compression (str): Default compression method.
            Default is no compression.
        auto_declare (bool): Automatically declare the default exchange
            at instantiation. Default is :const:`True`.
        on_return (Callable): Callback to call for undeliverable messages,
            when the `mandatory` or `immediate` arguments to
            :meth:`publish` is used. This callback needs the following
            signature: `(exception, exchange, routing_key, message)`.
            Note that the producer needs to drain events to use this feature.
    """

    #: Default exchange
    exchange = None

    #: Default routing key.
    routing_key = ''

    #: Default serializer to use. Default is JSON.
    serializer = None

    #: Default compression method.  Disabled by default.
    compression = None

    #: By default, if a default exchange is set,
    #: that exchange will be declare when publishing a message.
    auto_declare = True

    #: Basic return callback.
    on_return = None

    #: Set if channel argument was a Connection instance (using
    #: default_channel).
    __connection__ = None

    def __init__(self, channel, exchange=None, routing_key=None,
                 serializer=None, auto_declare=None, compression=None,
                 on_return=None):
        self._channel = channel
        self.exchange = exchange
        self.routing_key = routing_key or self.routing_key
        self.serializer = serializer or self.serializer
        self.compression = compression or self.compression
        self.on_return = on_return or self.on_return
        self._channel_promise = None
        self._batch_local = local()
        if self.exchange is None:
            self.exchange = Exchange('')
        if auto_declare is not None:
            self.auto_declare = auto_declare

        if self._channel:
            self.revive(self._channel)

    def __repr__(self):
        return f'<Producer: {self._channel}>'

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return (None, self.exchange, self.routing_key, self.serializer,
                self.auto_declare, self.compression)

    def declare(self):
        """Declare the exchange.

        Note:
        ----
            This happens automatically at instantiation when
            the :attr:`auto_declare` flag is enabled.
        """
        if self.exchange.name:
            self.exchange.declare()

    def maybe_declare(self, entity, retry=False, **retry_policy):
        """Declare exchange if not already declared during this session."""
        if entity:
            return maybe_declare(entity, self.channel, retry, **retry_policy)

    @property
    def supports_batch_publish(self):
        """Return whether the active transport can defer batched publishes."""
        connection = self.connection
        if connection is None:
            return False
        try:
            transport_support = connection.transport.implements.batch_publish
        except AttributeError:
            return False
        channel = self._channel
        if channel is not None and not isinstance(channel, ChannelPromise):
            configured_support = getattr(
                channel,
                'supports_batch_publish',
                transport_support,
            )
        else:
            configured_support = getattr(
                connection.transport,
                'supports_batch_publish',
                transport_support,
            )
        return bool(transport_support and configured_support)

    def batch(self, max_size=DEFAULT_BATCH_SIZE):
        """Group normal :meth:`publish` calls into transport-owned batches.

        Unsupported transports retain immediate publication.  A successful
        outermost context exit flushes pending messages; an exceptional exit
        discards messages that have not already been flushed.

        :param max_size: Maximum transport operations to buffer before an
            automatic flush. Must be a positive integer.
        :type max_size: int
        """
        if (isinstance(max_size, bool) or
                not isinstance(max_size, int) or max_size <= 0):
            raise ValueError('max_size must be a positive integer')
        return _ProducerBatch(self, max_size)

    def _delivery_details(self, exchange, delivery_mode=None,
                          maybe_delivery_mode=maybe_delivery_mode,
                          Exchange=Exchange):
        if isinstance(exchange, Exchange):
            return exchange.name, maybe_delivery_mode(
                delivery_mode or exchange.delivery_mode,
            )
        # exchange is string, so inherit the delivery
        # mode of our default exchange.
        return exchange, maybe_delivery_mode(
            delivery_mode or self.exchange.delivery_mode,
        )

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=None, expiration=None, timeout=None,
                confirm_timeout=None,
                **properties):
        """Publish message to the specified exchange.

        Arguments:
        ---------
            body (Any): Message body.
            routing_key (str): Message routing key.
            delivery_mode (enum): See :attr:`delivery_mode`.
            mandatory (bool): Currently not supported.
            immediate (bool): Currently not supported.
            priority (int): Message priority. A number between 0 and 9.
            content_type (str): Content type. Default is auto-detect.
            content_encoding (str): Content encoding. Default is auto-detect.
            serializer (str): Serializer to use. Default is auto-detect.
            compression (str): Compression method to use.  Default is none.
            headers (Dict): Mapping of arbitrary headers to pass along
                with the message body.
            exchange (kombu.entity.Exchange, str): Override the exchange.
                Note that this exchange must have been declared.
            declare (Sequence[EntityT]): Optional list of required entities
                that must have been declared before publishing the message.
                The entities will be declared using
                :func:`~kombu.common.maybe_declare`.
            retry (bool): Retry publishing, or declaring entities if the
                connection is lost.
            retry_policy (Dict): Retry configuration, this is the keywords
                supported by :meth:`~kombu.Connection.ensure`.
            expiration (float): A TTL in seconds can be specified per message.
                Default is no expiration.
            timeout (float): Set timeout to wait maximum timeout second
                for message to publish.
            confirm_timeout (float): Set confirm timeout to wait maximum timeout second
                for message to confirm publishing if the channel is set to confirm publish mode.
            **properties (Any): Additional message properties, see AMQP spec.
        """
        _publish = self._publish

        declare = [] if declare is None else declare
        headers = {} if headers is None else headers
        retry_policy = {} if retry_policy is None else retry_policy
        routing_key = self.routing_key if routing_key is None else routing_key
        compression = self.compression if compression is None else compression

        exchange_name, properties['delivery_mode'] = self._delivery_details(
            exchange or self.exchange, delivery_mode,
        )

        if expiration is not None:
            properties['expiration'] = str(int(expiration * 1000))

        body, content_type, content_encoding = self._prepare(
            body, serializer, content_type, content_encoding,
            compression, headers)

        if self.auto_declare and self.exchange.name:
            if self.exchange not in declare:
                # XXX declare should be a Set.
                declare.append(self.exchange)

        if retry:
            self.connection.transport_options.update(retry_policy)
            _publish = self.connection.ensure(self, _publish, **retry_policy)
        return _publish(
            body, priority, content_type, content_encoding,
            headers, properties, routing_key, mandatory, immediate,
            exchange_name, declare, timeout, confirm_timeout, retry, retry_policy
        )

    def _publish(self, body, priority, content_type, content_encoding,
                 headers, properties, routing_key, mandatory,
                 immediate, exchange, declare, timeout=None, confirm_timeout=None, retry=False, retry_policy=None):
        retry_policy = {} if retry_policy is None else retry_policy
        channel = self.channel
        message = channel.prepare_message(
            body, priority, content_type,
            content_encoding, headers, properties,
        )
        if declare:
            maybe_declare = self.maybe_declare
            for entity in declare:
                maybe_declare(entity, retry=retry, **retry_policy)

        # handle autogenerated queue names for reply_to
        reply_to = properties.get('reply_to')
        if isinstance(reply_to, Queue):
            properties['reply_to'] = reply_to.name
        publish_kwargs = {
            'exchange': exchange,
            'routing_key': routing_key,
            'mandatory': mandatory,
            'immediate': immediate,
            'timeout': timeout,
            'confirm_timeout': confirm_timeout,
        }
        batch = getattr(self._batch_local, 'current', None)
        if batch is not None:
            return batch.publish(channel, message, **publish_kwargs)
        return channel.basic_publish(message, **publish_kwargs)

    def _get_channel(self):
        channel = self._channel
        if isinstance(channel, ChannelPromise):
            channel = self._channel = channel()
            self.exchange.revive(channel)
            if self.on_return:
                channel.events['basic_return'].add(self.on_return)
        return channel

    def _set_channel(self, channel):
        self._channel = channel

    channel = property(_get_channel, _set_channel)

    def revive(self, channel):
        """Revive the producer after connection loss."""
        if is_connection(channel):
            connection = channel
            self.__connection__ = connection
            channel = ChannelPromise(lambda: connection.default_channel)
        if isinstance(channel, ChannelPromise):
            self._channel = channel
            self.exchange = self.exchange(channel)
        else:
            # Channel already concrete
            self._channel = channel
            if self.on_return:
                self._channel.events['basic_return'].add(self.on_return)
            self.exchange = self.exchange(channel)

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        # In case the connection is part of a pool it needs to be
        # replaced in case of an exception
        if self.__connection__ is not None and exc_type is not None:
            if isinstance(self.__connection__, PooledConnection):
                self.__connection__._pool.replace(self.__connection__)

        self.release()

    def release(self):
        pass

    close = release

    def _prepare(self, body, serializer=None, content_type=None,
                 content_encoding=None, compression=None, headers=None):

        # No content_type? Then we're serializing the data internally.
        if not content_type:
            serializer = serializer or self.serializer
            (content_type, content_encoding,
             body) = dumps(body, serializer=serializer)
        else:
            # If the programmer doesn't want us to serialize,
            # make sure content_encoding is set.
            if isinstance(body, str):
                if not content_encoding:
                    content_encoding = 'utf-8'
                body = body.encode(content_encoding)

            # If they passed in a string, we can't know anything
            # about it. So assume it's binary data.
            elif not content_encoding:
                content_encoding = 'binary'

        if compression:
            body, headers['compression'] = compress(body, compression)

        return body, content_type, content_encoding

    @property
    def connection(self):
        try:
            return self.__connection__ or self.channel.connection.client
        except AttributeError:
            pass


class ConsumerShutdownResult(enum.Enum):
    """Outcome of a graceful :class:`ConsumerShutdown`.

    - ``COMPLETED``: every already-delivered message was acknowledged,
      rejected or requeued before the timeout elapsed.
    - ``TIMED_OUT``: the timeout elapsed with messages still unresolved.
    - ``CONNECTION_LOST``: the broker connection (or a channel on it)
      failed while stopping or waiting.
    """

    COMPLETED = 'completed'
    TIMED_OUT = 'timed_out'
    CONNECTION_LOST = 'connection_lost'


class _ChannelDeliveryState:
    """Tracks delivered-but-unresolved messages on one channel.

    AMQP delivery tags are scoped to the channel, and ``basic.ack`` /
    ``basic.nack`` with ``multiple=True`` resolve every outstanding tag up
    to and including the given tag.  To observe these terminal calls the
    state installs transparent wrappers around the channel's
    ``basic_ack``/``basic_reject``/``basic_nack`` methods exactly once and
    is shared by every consumer bound to that channel, so out-of-order
    acknowledgements and batch (``multiple``) acknowledgements both resolve
    exactly the messages that the broker considers settled.

    The wrappers are restored when all tracked consumers detach, which
    leaves no trace on channels that keep being used afterwards.
    """

    _ATTR = '_kombu_delivery_state'

    def __init__(self, channel):
        self.channel = channel
        self._consumers = weakref.WeakSet()
        # method name -> (original method, whether it was an instance attr)
        self._originals = {}
        try:
            self._wrap('basic_ack', multiple=True)
            self._wrap('basic_nack', multiple=True, optional=True)
            self._wrap('basic_reject', multiple=False, optional=True)
        except BaseException:
            self._restore()
            raise

    @classmethod
    def attach(cls, consumer, channel):
        """Return the shared state for ``channel``, registering consumer.

        Returns :const:`None` for channels that cannot have their methods
        wrapped, in which case delivery tracking is unavailable.
        """
        existing = getattr(channel, cls._ATTR, None)
        if isinstance(existing, cls):
            existing._consumers.add(consumer)
            return existing
        try:
            state = cls(channel)
            setattr(channel, cls._ATTR, state)
        except (AttributeError, TypeError):
            return None
        state._consumers.add(consumer)
        return state

    def detach(self, consumer):
        """Stop notifying ``consumer`` and unwrap once nobody is left."""
        self._consumers.discard(consumer)
        if not self._consumers:
            self._restore()

    def resolve(self, delivery_tag, multiple=False):
        if delivery_tag is None:
            return
        for consumer in list(self._consumers):
            consumer._resolve_delivery(delivery_tag, multiple)

    def _wrap(self, name, *, multiple, optional=False):
        original = getattr(self.channel, name, None)
        if original is None:
            if optional:
                return
            raise AttributeError(name)
        was_instance_attr = name in getattr(self.channel, '__dict__', {})
        state = self

        @wraps(original)
        def terminal(delivery_tag=None, *args, **kwargs):
            is_multiple = bool(kwargs.get('multiple', False))
            if multiple and not is_multiple and args:
                # e.g. basic_ack(tag, True)
                is_multiple = bool(args[0])
            result = original(delivery_tag, *args, **kwargs)
            state.resolve(delivery_tag, is_multiple)
            return result

        setattr(self.channel, name, terminal)
        self._originals[name] = (original, was_instance_attr)

    def _restore(self):
        channel = self.channel
        for name, (original, was_instance_attr) in self._originals.items():
            try:
                if was_instance_attr:
                    setattr(channel, name, original)
                else:
                    delattr(channel, name)
            except AttributeError:
                try:
                    setattr(channel, name, original)
                except AttributeError:
                    pass
        self._originals.clear()
        try:
            if getattr(channel, self._ATTR, None) is self:
                delattr(channel, self._ATTR)
        except AttributeError:
            pass


class Consumer:
    """Message consumer.

    Arguments:
    ---------
        channel (kombu.Connection, ChannelT): see :attr:`channel`.
        queues (Sequence[kombu.Queue]): see :attr:`queues`.
        no_ack (bool): see :attr:`no_ack`.
        auto_declare (bool): see :attr:`auto_declare`
        callbacks (Sequence[Callable]): see :attr:`callbacks`.
        on_message (Callable): See :attr:`on_message`
        on_decode_error (Callable): see :attr:`on_decode_error`.
        prefetch_count (int): see :attr:`prefetch_count`.
    """

    ContentDisallowed = ContentDisallowed

    #: The connection/channel to use for this consumer.
    channel = None

    #: A single :class:`~kombu.Queue`, or a list of queues to
    #: consume from.
    queues = None

    #: Flag for automatic message acknowledgment.
    #: If enabled the messages are automatically acknowledged by the
    #: broker.  This can increase performance but means that you
    #: have no control of when the message is removed.
    #:
    #: Disabled by default.
    no_ack = None

    #: By default all entities will be declared at instantiation, if you
    #: want to handle this manually you can set this to :const:`False`.
    auto_declare = True

    #: List of callbacks called in order when a message is received.
    #:
    #: The signature of the callbacks must take two arguments:
    #: `(body, message)`, which is the decoded message body and
    #: the :class:`~kombu.Message` instance.
    callbacks = None

    #: Optional function called whenever a message is received.
    #:
    #: When defined this function will be called instead of the
    #: :meth:`receive` method, and :attr:`callbacks` will be disabled.
    #:
    #: So this can be used as an alternative to :attr:`callbacks` when
    #: you don't want the body to be automatically decoded.
    #: Note that the message will still be decompressed if the message
    #: has the ``compression`` header set.
    #:
    #: The signature of the callback must take a single argument,
    #: which is the :class:`~kombu.Message` object.
    #:
    #: Also note that the ``message.body`` attribute, which is the raw
    #: contents of the message body, may in some cases be a read-only
    #: :class:`buffer` object.
    on_message = None

    #: Callback called when a message can't be decoded.
    #:
    #: The signature of the callback must take two arguments: `(message,
    #: exc)`, which is the message that can't be decoded and the exception
    #: that occurred while trying to decode it.
    on_decode_error = None

    #: List of accepted content-types.
    #:
    #: An exception will be raised if the consumer receives
    #: a message with an untrusted content type.
    #: By default all content-types are accepted, but not if
    #: :func:`kombu.disable_untrusted_serializers` was called,
    #: in which case only json is allowed.
    accept = None

    #: Initial prefetch count
    #:
    #: If set, the consumer will set the prefetch_count QoS value at startup.
    #: Can also be changed using :meth:`qos`.
    prefetch_count = None

    #: Mapping of queues we consume from.
    _queues = None

    _tags = count(1)  # global

    def __init__(self, channel, queues=None, no_ack=None, auto_declare=None,
                 callbacks=None, on_decode_error=None, on_message=None,
                 accept=None, prefetch_count=None, tag_prefix=None):
        self.channel = channel
        self.queues = maybe_list(queues or [])
        self.no_ack = self.no_ack if no_ack is None else no_ack
        self.callbacks = (self.callbacks or [] if callbacks is None
                          else callbacks)
        self.on_message = on_message
        self.tag_prefix = tag_prefix
        self._active_tags = {}
        self._active_queue_no_ack = {}
        # delivery tags handed to the application but not yet acknowledged,
        # rejected or requeued (populated lazily on the first delivery).
        self._outstanding_tags = set()
        self._delivery_state = None
        if auto_declare is not None:
            self.auto_declare = auto_declare
        if on_decode_error is not None:
            self.on_decode_error = on_decode_error
        self.accept = prepare_accept_content(accept)
        self.prefetch_count = prefetch_count

        if self.channel:
            self.revive(self.channel)

    @property
    def queues(self):  # noqa
        return list(self._queues.values())

    @queues.setter
    def queues(self, queues):
        self._queues = {q.name: q for q in queues}

    def revive(self, channel):
        """Revive consumer after connection loss."""
        previous_no_ack = self._active_queue_no_ack.copy()
        self._active_tags.clear()
        self._active_queue_no_ack.clear()
        # delivery tags are scoped to the old channel and die with it
        self._detach_delivery_state()
        self._outstanding_tags.clear()
        channel = self.channel = maybe_channel(channel)
        # modify dict size while iterating over it is not allowed
        for qname, queue in list(self._queues.items()):
            # name may have changed after declare
            self._queues.pop(qname, None)
            queue = self._queues[queue.name] = queue(self.channel)
            queue.revive(channel)

        if self.auto_declare:
            self.declare()

        if self.prefetch_count is not None:
            self.qos(prefetch_count=self.prefetch_count)

        # re-register the AMQP consumers, but only for queues that were
        # actually being consumed from before (not merely registered).
        self._consume_previously_active(previous_no_ack)

    def declare(self):
        """Declare queues, exchanges and bindings.

        Note:
        ----
            This is done automatically at instantiation
            when :attr:`auto_declare` is set.
        """
        for queue in self._queues.values():
            queue.declare()

    def register_callback(self, callback):
        """Register a new callback to be called when a message is received.

        Note:
        ----
            The signature of the callback needs to accept two arguments:
            `(body, message)`, which is the decoded message body
            and the :class:`~kombu.Message` instance.
        """
        self.callbacks.append(callback)

    def __enter__(self):
        self.consume()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        if self.channel and self.channel.connection:
            conn_errors = self.channel.connection.client.connection_errors
            if not isinstance(exc_val, conn_errors):
                try:
                    self.cancel()
                except Exception:
                    pass

    def add_queue(self, queue):
        """Add a queue to the list of queues to consume from.

        Note:
        ----
            This will not start consuming from the queue,
            for that you will have to call :meth:`consume` after.
        """
        queue = queue(self.channel)
        if self.auto_declare:
            queue.declare()
        self._queues[queue.name] = queue
        return queue

    def consume(self, no_ack=None):
        """Start consuming messages.

        Can be called multiple times, but note that while it
        will consume from new queues added since the last call,
        it will not cancel consuming from removed queues (
        use :meth:`cancel_by_queue`).

        Arguments:
        ---------
            no_ack (bool): See :attr:`no_ack`.
        """
        no_ack = self.no_ack if no_ack is None else no_ack
        self._consume_queues(
            {queue.name: no_ack for queue in self._queues.values()}
        )

    def _consume_previously_active(self, active_no_ack):
        """Start consuming only from the given, previously active queues.

        Unlike :meth:`consume`, this will not start consuming from
        queues that were registered (e.g. via :meth:`add_queue`) but
        never actually consumed from, and it restores each queue's
        own effective ``no_ack`` value instead of applying one value
        to every queue.

        Arguments:
        ---------
            active_no_ack (dict): Mapping of queue name to the
                ``no_ack`` value it was consumed with.
        """
        # thin wrapper kept for readability/intent at the revive() call site
        self._consume_queues(active_no_ack)

    def _consume_queues(self, no_ack_by_queue):
        queues = [
            q for q in self._queues.values() if q.name in no_ack_by_queue
        ]
        if not queues:
            return
        H, T = queues[:-1], queues[-1]
        for queue in H:
            self._basic_consume(
                queue, no_ack=no_ack_by_queue[queue.name], nowait=True
            )
        self._basic_consume(
            T, no_ack=no_ack_by_queue[T.name], nowait=False
        )

    def cancel(self):
        """End all active queue consumers.

        Note:
        ----
            This does not affect already delivered messages, but it does
            mean the server will not send any more messages for this consumer.
        """
        cancel = self.channel.basic_cancel
        for tag in self._active_tags.values():
            cancel(tag)
        self._active_tags.clear()
        self._active_queue_no_ack.clear()

    close = cancel

    def cancel_by_queue(self, queue):
        """Cancel consumer by queue name."""
        qname = queue.name if isinstance(queue, Queue) else queue
        self._active_queue_no_ack.pop(qname, None)
        try:
            tag = self._active_tags.pop(qname)
        except KeyError:
            pass
        else:
            self.channel.basic_cancel(tag)
        finally:
            self._queues.pop(qname, None)

    def consuming_from(self, queue):
        """Return :const:`True` if currently consuming from queue'."""
        name = queue
        if isinstance(queue, Queue):
            name = queue.name
        return name in self._active_tags

    def purge(self):
        """Purge messages from all queues.

        Warning:
        -------
            This will *delete all ready messages*, there is no undo operation.
        """
        return sum(queue.purge() for queue in self._queues.values())

    def flow(self, active):
        """Enable/disable flow from peer.

        This is a simple flow-control mechanism that a peer can use
        to avoid overflowing its queues or otherwise finding itself
        receiving more messages than it can process.

        The peer that receives a request to stop sending content
        will finish sending the current content (if any), and then wait
        until flow is reactivated.
        """
        self.channel.flow(active)

    def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
        """Specify quality of service.

        The client can request that messages should be sent in
        advance so that when the client finishes processing a message,
        the following message is already held locally, rather than needing
        to be sent down the channel. Prefetching gives a performance
        improvement.

        The prefetch window is Ignored if the :attr:`no_ack` option is set.

        Arguments:
        ---------
            prefetch_size (int): Specify the prefetch window in octets.
                The server will send a message in advance if it is equal to
                or smaller in size than the available prefetch size (and
                also falls within other prefetch limits). May be set to zero,
                meaning "no specific limit", although other prefetch limits
                may still apply.

            prefetch_count (int): Specify the prefetch window in terms of
                whole messages.

            apply_global (bool): Apply new settings globally on all channels.
        """
        return self.channel.basic_qos(prefetch_size,
                                      prefetch_count,
                                      apply_global)

    def recover(self, requeue=False):
        """Redeliver unacknowledged messages.

        Asks the broker to redeliver all unacknowledged messages
        on the specified channel.

        Arguments:
        ---------
            requeue (bool): By default the messages will be redelivered
                to the original recipient. With `requeue` set to true, the
                server will attempt to requeue the message, potentially then
                delivering it to an alternative subscriber.
        """
        return self.channel.basic_recover(requeue=requeue)

    def receive(self, body, message):
        """Method called when a message is received.

        This dispatches to the registered :attr:`callbacks`.

        Arguments:
        ---------
            body (Any): The decoded message body.
            message (~kombu.Message): The message instance.

        Raises
        ------
            NotImplementedError: If no consumer callbacks have been
                registered.
        """
        callbacks = self.callbacks
        if not callbacks:
            raise NotImplementedError('Consumer does not have any callbacks')
        [callback(body, message) for callback in callbacks]

    def _basic_consume(self, queue, consumer_tag=None,
                       no_ack=no_ack, nowait=True):
        tag = self._active_tags.get(queue.name)
        if tag is None:
            no_ack = queue.no_ack if no_ack is None else no_ack
            tag = self._add_tag(queue, consumer_tag)
            self._active_queue_no_ack[queue.name] = no_ack
            queue.consume(tag, self._receive_callback,
                          no_ack=no_ack, nowait=nowait)
        return tag

    def _add_tag(self, queue, consumer_tag=None):
        tag = consumer_tag or '{}{}'.format(
            self.tag_prefix, next(self._tags))
        self._active_tags[queue.name] = tag
        return tag

    def _receive_callback(self, message):
        accept = self.accept
        on_m, channel, decoded = self.on_message, self.channel, None
        try:
            m2p = getattr(channel, 'message_to_python', None)
            if m2p:
                message = m2p(message)
            if accept is not None:
                message.accept = accept
            if message.errors:
                return message._reraise_error(self.on_decode_error)
            decoded = None if on_m else message.decode()
        except Exception as exc:
            if not self.on_decode_error:
                raise
            self.on_decode_error(message, exc)
        else:
            # registered before dispatch, so a synchronous ack/reject from
            # inside the callback is observed by graceful shutdown.
            self._track_delivery(message)
            return on_m(message) if on_m else self.receive(decoded, message)

    def _track_delivery(self, message):
        """Record a delivered message awaiting a terminal ack/reject."""
        # the broker settles no_ack deliveries itself, nothing to wait for
        no_ack_consumers = getattr(self.channel, 'no_ack_consumers', None)
        if no_ack_consumers is not None:
            try:
                consumer_tag = message.delivery_info.get('consumer_tag')
                if consumer_tag in no_ack_consumers:
                    return
            except (AttributeError, TypeError):
                pass
        delivery_tag = message.delivery_tag
        if delivery_tag is None:
            return
        if self._delivery_state is None:
            state = _ChannelDeliveryState.attach(self, self.channel)
            if state is None:
                # channel cannot be observed; do not pretend we can tell
                # when its deliveries settle.
                return
            self._delivery_state = state
        self._outstanding_tags.add(delivery_tag)

    def _resolve_delivery(self, delivery_tag, multiple=False):
        """Mark a delivery tag settled by ack/reject/nack.

        With ``multiple`` the broker settled every outstanding tag up to
        and including ``delivery_tag``; otherwise only that single tag.
        """
        tags = self._outstanding_tags
        if not tags:
            return
        if multiple:
            tags.difference_update(
                {tag for tag in tags if tag <= delivery_tag}
            )
        else:
            tags.discard(delivery_tag)

    def _detach_delivery_state(self):
        state = self._delivery_state
        if state is not None:
            self._delivery_state = None
            state.detach(self)

    @property
    def outstanding_message_count(self):
        """Number of delivered messages not yet acked, rejected/requeued."""
        return len(self._outstanding_tags)

    def shutdown(self, timeout=None, cancel=True, poll_interval=1.0):
        """Gracefully stop this consumer and wait for in-flight messages.

        Convenience wrapper around :class:`ConsumerShutdown` for a single
        consumer.

        Arguments:
        ---------
            timeout (float): Maximum seconds to wait for already-delivered
                messages to be acknowledged or returned. :const:`None`
                waits indefinitely.
            cancel (bool): Stop new deliveries (``basic.cancel``) before
                waiting. Enabled by default.
            poll_interval (float): Seconds per ``drain_events`` tick while
                waiting; keeps broker heartbeats flowing.
        """
        return ConsumerShutdown(
            [self], timeout=timeout, cancel=cancel,
            poll_interval=poll_interval,
        ).stop()

    def __repr__(self):
        return f'<{type(self).__name__}: {self.queues}>'

    @property
    def connection(self):
        try:
            return self.channel.connection.client
        except AttributeError:
            pass


class ConsumerShutdown:
    """Gracefully stop consumers that share one broker connection.

    Coordinates several :class:`Consumer` instances, including consumers
    bound to different channels of the same connection, while they exit:

    1. each consumer is optionally canceled first, so no new messages are
       delivered (``cancel=True``, the default);
    2. the shared connection is drained until every message already
       delivered to any of the consumers has been acknowledged, rejected or
       requeued -- out-of-order acks and batch (``multiple=True``) acks are
       both understood -- or until ``timeout`` elapses;
    3. broker heartbeats are checked on every tick while waiting;
    4. clean completion, timeout and connection loss are reported as
       distinct :class:`ConsumerShutdownResult` values.

    The result is memoized, so :meth:`stop` may be called repeatedly; the
    tracking hooks installed on the channels are released exactly once.
    The object can also be used as a context manager, in which case
    :meth:`stop` runs on exit::

        with ConsumerShutdown([consumer1, consumer2], timeout=10) as stop:
            ...  # workers keep processing, heartbeats flow on exit
        assert stop.result is ConsumerShutdownResult.COMPLETED

    Arguments:
    ---------
        consumers (Sequence[Consumer]): Consumers to stop. They must all
            share the same broker :class:`~kombu.Connection`.
        timeout (float): Maximum seconds to wait for delivered messages to
            settle. :const:`None` (default) waits indefinitely.
        cancel (bool): Cancel the consumers (stop new deliveries) before
            waiting. Enabled by default.
        poll_interval (float): Seconds per ``drain_events`` tick while
            waiting. The broker connection is drained and heartbeats are
            checked at least this often. Default is 1 second.
        heartbeat_rate (int): Rate passed to
            :meth:`~kombu.Connection.heartbeat_check`.
    """

    def __init__(self, consumers, timeout=None, *, cancel=True,
                 poll_interval=1.0, heartbeat_rate=2):
        consumers = list(maybe_list(consumers))
        if not consumers:
            raise ValueError(
                'ConsumerShutdown requires at least one consumer')
        if timeout is not None and (
                isinstance(timeout, bool)
                or not isinstance(timeout, (int, float)) or timeout < 0):
            raise ValueError('timeout must be a non-negative number or None')
        if isinstance(poll_interval, bool) or not isinstance(
                poll_interval, (int, float)) or poll_interval <= 0:
            raise ValueError('poll_interval must be a positive number')
        self.consumers = consumers
        self.timeout = timeout
        self.cancel_on_stop = cancel
        self.poll_interval = float(poll_interval)
        self.heartbeat_rate = heartbeat_rate
        self.result = None
        self._released = False
        self._lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def stop(self):
        """Cancel the consumers and wait for their deliveries to settle.

        Repeated calls return the first :class:`ConsumerShutdownResult`
        without canceling or waiting again.
        """
        with self._lock:
            if self.result is None:
                self.result = self._run()
                self._release()
            return self.result

    def close(self):
        """Release channel tracking hooks without waiting.

        Safe to call more than once and after :meth:`stop`; consumers keep
        working and tracking is reinstalled lazily on the next delivery.
        """
        with self._lock:
            self._release()

    def _run(self):
        # nothing active to cancel and nothing outstanding to wait for:
        # this also covers consumers that never got a concrete channel
        # (a still-unresolved channel promise), so no connection is needed.
        if all(not consumer._active_tags and not consumer._outstanding_tags
               for consumer in self.consumers):
            return ConsumerShutdownResult.COMPLETED
        connection = self._shared_connection()
        if connection is None:
            return ConsumerShutdownResult.CONNECTION_LOST
        if self.cancel_on_stop and not self._cancel_consumers(connection):
            return ConsumerShutdownResult.CONNECTION_LOST
        return self._wait_until_settled(connection)

    def _shared_connection(self):
        connection = None
        for consumer in self.consumers:
            candidate = consumer.connection
            if candidate is None:
                return None
            if connection is None:
                connection = candidate
            elif candidate is not connection:
                raise ValueError(
                    'ConsumerShutdown only coordinates consumers that share '
                    'the same broker connection')
        return connection

    def _cancel_consumers(self, connection):
        conn_errors = self._connection_errors(connection)
        for consumer in self.consumers:
            try:
                consumer.cancel()
            except conn_errors:
                return False
        return True

    @staticmethod
    def _connection_errors(connection):
        # OSError covers socket failures even for transports that only
        # advertise their own exception classes.
        return tuple(connection.connection_errors or ()) + (OSError,)

    def _has_outstanding_messages(self):
        return any(consumer._outstanding_tags for consumer in self.consumers)

    def _wait_until_settled(self, connection):
        deadline = None
        if self.timeout is not None:
            deadline = monotonic() + self.timeout
        # an idle drain timeout is normal and must not be confused with a
        # dropped connection.
        timeout_errors = (socket.timeout, TimeoutError)
        conn_errors = self._connection_errors(connection)
        while self._has_outstanding_messages():
            wait = self.poll_interval
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return ConsumerShutdownResult.TIMED_OUT
                wait = min(wait, remaining)
            try:
                connection.drain_events(timeout=wait)
            except timeout_errors:
                pass
            except conn_errors:
                return ConsumerShutdownResult.CONNECTION_LOST
            try:
                connection.heartbeat_check(rate=self.heartbeat_rate)
            except conn_errors:
                return ConsumerShutdownResult.CONNECTION_LOST
        return ConsumerShutdownResult.COMPLETED

    def _release(self):
        if self._released:
            return
        self._released = True
        for consumer in self.consumers:
            consumer._detach_delivery_state()
