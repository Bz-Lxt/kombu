from __future__ import annotations

import socket

import pytest

from kombu import Connection, Consumer, Exchange, Producer, Queue


class test_MemoryTransport:

    def setup_method(self):
        self.c = Connection(transport='memory')
        self.e = Exchange('test_transport_memory')
        self.q = Queue('test_transport_memory',
                       exchange=self.e,
                       routing_key='test_transport_memory')
        self.q2 = Queue('test_transport_memory2',
                        exchange=self.e,
                        routing_key='test_transport_memory2')
        self.fanout = Exchange('test_transport_memory_fanout', type='fanout')
        self.q3 = Queue('test_transport_memory_fanout1',
                        exchange=self.fanout)
        self.q4 = Queue('test_transport_memory_fanout2',
                        exchange=self.fanout)

    def test_driver_version(self):
        assert self.c.transport.driver_version()

    def test_produce_consume_noack(self):
        channel = self.c.channel()
        producer = Producer(channel, self.e)
        consumer = Consumer(channel, self.q, no_ack=True)

        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory')

        _received = []

        def callback(message_data, message):
            _received.append(message)

        consumer.register_callback(callback)
        consumer.consume()

        while 1:
            if len(_received) == 10:
                break
            self.c.drain_events()

        assert len(_received) == 10

    def test_produce_consume_fanout(self):
        producer = self.c.Producer()
        consumer = self.c.Consumer([self.q3, self.q4])

        producer.publish(
            {'hello': 'world'},
            declare=consumer.queues,
            exchange=self.fanout,
        )

        assert self.q3(self.c).get().payload == {'hello': 'world'}
        assert self.q4(self.c).get().payload == {'hello': 'world'}
        assert self.q3(self.c).get() is None
        assert self.q4(self.c).get() is None

    def test_produce_consume(self):
        channel = self.c.channel()
        producer = Producer(channel, self.e)
        consumer1 = Consumer(channel, self.q)
        consumer2 = Consumer(channel, self.q2)
        self.q2(channel).declare()

        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory')
        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory2')

        _received1 = []
        _received2 = []

        def callback1(message_data, message):
            _received1.append(message)
            message.ack()

        def callback2(message_data, message):
            _received2.append(message)
            message.ack()

        consumer1.register_callback(callback1)
        consumer2.register_callback(callback2)

        consumer1.consume()
        consumer2.consume()

        while 1:
            if len(_received1) + len(_received2) == 20:
                break
            self.c.drain_events()

        assert len(_received1) + len(_received2) == 20

        # compression
        producer.publish({'compressed': True},
                         routing_key='test_transport_memory',
                         compression='zlib')
        m = self.q(channel).get()
        assert m.payload == {'compressed': True}

        # queue.delete
        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory')
        assert self.q(channel).get()
        self.q(channel).delete()
        self.q(channel).declare()
        assert self.q(channel).get() is None

        # queue.purge
        for i in range(10):
            producer.publish({'foo': i}, routing_key='test_transport_memory2')
        assert self.q2(channel).get()
        self.q2(channel).purge()
        assert self.q2(channel).get() is None

    def test_drain_events(self):
        with pytest.raises(socket.timeout):
            self.c.drain_events(timeout=0.1)

        c1 = self.c.channel()
        c2 = self.c.channel()

        with pytest.raises(socket.timeout):
            self.c.drain_events(timeout=0.1)

        del c1  # so pyflakes doesn't complain.
        del c2

    def test_drain_events_unregistered_queue(self):
        c1 = self.c.channel()
        producer = self.c.Producer()
        consumer = self.c.Consumer([self.q2])

        producer.publish(
            {'hello': 'world'},
            declare=consumer.queues,
            routing_key=self.q2.routing_key,
            exchange=self.q2.exchange,
        )
        message = consumer.queues[0].get()._raw

        class Cycle:

            def get(self, callback, timeout=None):
                return (message, 'foo'), c1

        self.c.transport.cycle = Cycle()
        self.c.drain_events()

    def test_queue_for(self):
        chan = self.c.channel()
        chan.queues.clear()

        x = chan._queue_for('foo')
        assert x
        assert chan._queue_for('foo') is x

    # see the issue
    # https://github.com/celery/kombu/issues/1050
    def test_producer_on_return(self):
        def on_return(_exception, _exchange, _routing_key, _message):
            pass
        channel = self.c.channel()
        producer = Producer(channel, on_return=on_return)
        consumer = self.c.Consumer([self.q3])

        producer.publish(
            {'hello': 'on return'},
            declare=consumer.queues,
            exchange=self.fanout,
        )

        assert self.q3(self.c).get().payload == {'hello': 'on return'}
        assert self.q3(self.c).get() is None


class test_ExchangeCascading:
    """Exchange-to-exchange bindings on the memory transport."""

    def setup_method(self):
        self.declarer = Connection(transport='memory')
        self.publisher = Connection(transport='memory')
        # The memory broker keeps global state between tests, so every
        # test needs unique entity names.
        self.prefix = f'test_exchange_cascade_{id(self)}'

    def teardown_method(self):
        self.declarer.close()
        self.publisher.close()

    def _name(self, n):
        return f'{self.prefix}_{n}'

    def _declare(self, name, type):
        chan = self.declarer.channel()
        chan.exchange_declare(self._name(name), type)
        return chan

    def _bind_queue(self, queue, exchange, routing_key):
        chan = self.declarer.channel()
        chan.queue_declare(self._name(queue))
        chan.queue_bind(
            self._name(queue), self._name(exchange), routing_key)

    def _bind_exchanges(self, destination, source, routing_key):
        self.declarer.channel().exchange_bind(
            self._name(destination), self._name(source), routing_key)

    def _publish(self, exchange, routing_key, payload):
        Producer(self.publisher.channel()).publish(
            payload, routing_key=routing_key,
            exchange=Exchange(self._name(exchange), durable=False),
            declare=[])

    def _get(self, queue):
        return Queue(self._name(queue))(self.declarer).get()

    def test_direct_topic_fanout_combination(self):
        self._declare('a', 'direct')
        self._declare('b', 'topic')
        self._declare('f', 'fanout')
        self._bind_queue('qb', 'b', 'news.#')
        self._bind_queue('qf1', 'f', '')
        self._bind_queue('qf2', 'f', '')
        # direct -> topic -> fanout
        self._bind_exchanges('b', 'a', 'news.us')
        self._bind_exchanges('f', 'b', 'news.#')

        self._publish('a', 'news.us', 'hello')

        assert self._get('qb').payload == 'hello'
        assert self._get('qf1').payload == 'hello'
        assert self._get('qf2').payload == 'hello'

    def test_cascade_declared_on_other_connection_is_reused(self):
        self._declare('a', 'direct')
        self._declare('b', 'direct')
        self._bind_queue('q', 'b', 'k')
        self._bind_exchanges('b', 'a', 'k')

        # Nothing is declared on the publisher connection; the topology
        # comes from the broker-wide shared state.
        with Connection(transport='memory') as fresh:
            Producer(fresh.channel()).publish(
                'remote', routing_key='k',
                exchange=Exchange(self._name('a'), durable=False),
                declare=[])

        assert self._get('q').payload == 'remote'

    def test_cycle_delivers_each_queue_once(self):
        self._declare('x', 'direct')
        self._declare('y', 'direct')
        self._bind_queue('qx', 'x', 'k')
        self._bind_queue('qy', 'y', 'k')
        self._bind_exchanges('y', 'x', 'k')
        self._bind_exchanges('x', 'y', 'k')  # cycle

        self._publish('x', 'k', 'once')

        assert self._get('qx').payload == 'once'
        assert self._get('qy').payload == 'once'
        assert self._get('qx') is None
        assert self._get('qy') is None

    def test_multiple_paths_deliver_once(self):
        # a -> b -> d and a -> c -> d; one queue on d.
        for name in ('a', 'b', 'c', 'd'):
            self._declare(name, 'direct')
        self._bind_queue('qd', 'd', 'k')
        self._bind_exchanges('b', 'a', 'k')
        self._bind_exchanges('c', 'a', 'k')
        self._bind_exchanges('d', 'b', 'k')
        self._bind_exchanges('d', 'c', 'k')

        self._publish('a', 'k', 'diamond')

        assert self._get('qd').payload == 'diamond'
        assert self._get('qd') is None

    def test_fanout_reachable_twice_is_broadcast_once(self):
        self._declare('a', 'direct')
        self._declare('b', 'direct')
        self._declare('f', 'fanout')
        self._bind_queue('qf', 'f', '')
        self._bind_exchanges('b', 'a', 'k')
        self._bind_exchanges('f', 'a', 'k')
        self._bind_exchanges('f', 'b', 'k')  # second path into the fanout

        self._publish('a', 'k', 'boom')

        assert self._get('qf').payload == 'boom'
        assert self._get('qf') is None

    def test_unbind_removes_route(self):
        self._declare('a', 'direct')
        self._declare('b', 'direct')
        self._bind_queue('q', 'b', 'k')
        self._bind_exchanges('b', 'a', 'k')

        self.declarer.channel().exchange_unbind(
            self._name('b'), self._name('a'), 'k')

        self._publish('a', 'k', 'nope')
        assert self._get('q') is None

    def test_unbind_is_idempotent(self):
        self._declare('a', 'direct')
        self._declare('b', 'direct')
        chan = self.declarer.channel()
        chan.exchange_unbind(self._name('b'), self._name('a'), 'k')

    def test_exchange_delete_removes_cascade_routes(self):
        self._declare('a', 'direct')
        self._declare('b', 'direct')
        self._bind_queue('q', 'b', 'k')
        self._bind_exchanges('b', 'a', 'k')

        self.declarer.channel().exchange_delete(self._name('b'))

        state = self.declarer.channel().state
        assert self._name('b') not in state.exchange_source_index
        assert self._name('b') not in state.exchange_destination_index
        assert state.exchanges[self._name('a')].get(
            'exchange_bindings', []) == []

        self._publish('a', 'k', 'nope')
        assert self._get('q') is None

    def test_redeclare_after_disconnect_keeps_delivery(self):
        self._declare('a', 'direct')
        self._declare('b', 'topic')
        self._bind_queue('q', 'b', 'k.#')
        self._bind_exchanges('b', 'a', 'k.x')
        self._publish('a', 'k.x', 'before')
        assert self._get('q').payload == 'before'

        # Simulate clients disconnecting; broker state outlives them.
        self.publisher.close()
        self.declarer.close()

        with Connection(transport='memory') as redeclarer:
            chan = redeclarer.channel()
            # Redeclaring the same topology must be idempotent.
            chan.exchange_declare(self._name('a'), 'direct')
            chan.exchange_declare(self._name('b'), 'topic')
            chan.exchange_bind(
                self._name('b'), self._name('a'), 'k.x')
            with Connection(transport='memory') as publisher:
                Producer(publisher.channel()).publish(
                    'after', routing_key='k.x',
                    exchange=Exchange(self._name('a'), durable=False),
                    declare=[])

        with Connection(transport='memory') as reader:
            assert Queue(self._name('q'))(reader).get().payload == 'after'
