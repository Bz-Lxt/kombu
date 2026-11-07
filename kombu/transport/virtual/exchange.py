"""Virtual AMQ Exchange.

Implementations of the standard exchanges defined
by the AMQ protocol  (excluding the `headers` exchange).
"""

from __future__ import annotations

import re

from kombu.utils.text import escape_regex

#: Keyword carrying the routing trace used to deduplicate delivery in
#: exchange-to-exchange binding topologies (see ``_RoutingTrace`` in
#: :mod:`kombu.transport.virtual.base`).
ROUTING_TRACE_KW = '_routing_trace'


def _public_kwargs(kwargs):
    """Strip the internal routing trace before calling ``_put``."""
    if ROUTING_TRACE_KW in kwargs:
        return {k: v for k, v in kwargs.items() if k != ROUTING_TRACE_KW}
    return kwargs


class ExchangeType:
    """Base class for exchanges.

    Implements the specifics for an exchange type.

    Arguments:
    ---------
        channel (ChannelT): AMQ Channel.
    """

    type = None

    def __init__(self, channel):
        self.channel = channel

    def lookup(self, table, exchange, routing_key, default):
        """Lookup all queues matching `routing_key` in `exchange`.

        Returns
        -------
            str: queue name, or 'default' if no queues matched.
        """
        raise NotImplementedError('subclass responsibility')

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        """Prepare queue-binding.

        Returns
        -------
            Tuple[str, Pattern, str]: of `(routing_key, regex, queue)`
                to be stored for bindings to this exchange.
        """
        return routing_key, None, queue

    def equivalent(self, prev, exchange, type,
                   durable, auto_delete, arguments):
        """Return true if `prev` and `exchange` is equivalent."""
        return (type == prev['type'] and
                durable == prev['durable'] and
                auto_delete == prev['auto_delete'] and
                (arguments or {}) == (prev['arguments'] or {}))


class DirectExchange(ExchangeType):
    """Direct exchange.

    The `direct` exchange routes based on exact routing keys.
    """

    type = 'direct'

    def lookup(self, table, exchange, routing_key, default):
        return {
            queue for rkey, _, queue in table
            if rkey == routing_key
        }

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        trace = kwargs.get(ROUTING_TRACE_KW)
        put_kwargs = _public_kwargs(kwargs)
        for queue in _lookup(exchange, routing_key, default=None):
            # Skip queues already reached through another path of the
            # same exchange-to-exchange topology (diamonds/cycles).
            if trace is not None:
                if queue in trace.queues:
                    continue
                trace.queues.add(queue)
            _put(queue, message, **put_kwargs)


class TopicExchange(ExchangeType):
    """Topic exchange.

    The `topic` exchange routes messages based on words separated by
    dots, using wildcard characters ``*`` (any single word), and ``#``
    (one or more words).
    """

    type = 'topic'

    #: map of wildcard to regex conversions
    wildcards = {'*': r'[^\.]+',
                 '#': r'.*?'}

    #: compiled regex cache
    _compiled = {}

    def lookup(self, table, exchange, routing_key, default):
        return {
            queue for rkey, pattern, queue in table
            if self._match(pattern, routing_key)
        }

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        trace = kwargs.get(ROUTING_TRACE_KW)
        put_kwargs = _public_kwargs(kwargs)
        for queue in _lookup(exchange, routing_key, default=None):
            if not queue:
                continue
            # Skip queues already reached through another path of the
            # same exchange-to-exchange topology (diamonds/cycles).
            if trace is not None:
                if queue in trace.queues:
                    continue
                trace.queues.add(queue)
            _put(queue, message, **put_kwargs)

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        return routing_key, self.key_to_pattern(routing_key), queue

    def key_to_pattern(self, rkey):
        """Get the corresponding regex for any routing key."""
        return '^%s$' % (r'\.'.join(
            self.wildcards.get(word, word)
            for word in escape_regex(rkey, '.#*').split('.')
        ))

    def _match(self, pattern, string):
        """Match regular expression (cached).

        Same as :func:`re.match`, except the regex is compiled and cached,
        then reused on subsequent matches with the same pattern.
        """
        try:
            compiled = self._compiled[pattern]
        except KeyError:
            compiled = self._compiled[pattern] = re.compile(pattern, re.U)
        return compiled.match(string)


class FanoutExchange(ExchangeType):
    """Fanout exchange.

    The `fanout` exchange implements broadcast messaging by delivering
    copies of all messages to all queues bound to the exchange.

    To support fanout the virtual channel needs to store the table
    as shared state.  This requires that the `Channel.supports_fanout`
    attribute is set to true, and the `Channel._queue_bind` and
    `Channel.get_table` methods are implemented.

    See Also
    --------
        the redis backend for an example implementation of these methods.
    """

    type = 'fanout'

    def lookup(self, table, exchange, routing_key, default):
        return {queue for _, _, queue in table}

    def deliver(self, message, exchange, routing_key, **kwargs):
        if self.channel.supports_fanout:
            # Mark the broadcast hop so a fanout exchange with no local
            # subscribers is not mistaken for an unroutable message, and
            # never broadcast twice when reachable through several paths.
            trace = kwargs.get(ROUTING_TRACE_KW)
            if trace is not None:
                trace.fanout = True
            # The trace is passed through on purpose: shared-state fanout
            # implementations (e.g. the memory transport) use it to avoid
            # enqueueing the same queue twice.
            self.channel._put_fanout(
                exchange, message, routing_key, **kwargs)


#: Map of standard exchange types and corresponding classes.
STANDARD_EXCHANGE_TYPES = {
    'direct': DirectExchange,
    'topic': TopicExchange,
    'fanout': FanoutExchange,
}
