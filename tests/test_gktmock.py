#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
from unittest import TestCase
from nose.tools import eq_, raises
import gevent
import gevent.socket
import gevent.monkey
gevent.monkey.patch_all()

from gktmock import KyotoTycoonMockServer, KTMockTimeOutError, DEFAULT_DB
from bkyototycoon import KyotoTycoonConnection


class KyotoTycoonMockServerTestCase(TestCase):

    def test_set_bulk(self):
        def _test_set_bulk():
            server = KyotoTycoonMockServer()
            server.start()
            eq_(server.data[DEFAULT_DB], {})
            client = KyotoTycoonConnection(pack=False)
            
            client.set_bulk({'a': 'foo', 'b': 'bar'})
            eq_(server.data[DEFAULT_DB], {'a': 'foo', 'b': 'bar'})
            eq_(
                list(server.command_logs),
                [dict(command='set_bulk', num_data=2, values={'a': 'foo', 'b': 'bar'})]
            )
            
            client.set_bulk({'c': 'foobar'})
            eq_(server.data[DEFAULT_DB], {'a': 'foo', 'b': 'bar', 'c': 'foobar'})
            eq_(
                list(server.command_logs),
                [
                    dict(command='set_bulk', num_data=2, values={'a': 'foo', 'b': 'bar'}),
                    dict(command='set_bulk', num_data=1, values={'c': 'foobar'}),
                ]
            )
            
            client.set_bulk({'a': 'FOO'})
            eq_(server.data[DEFAULT_DB], {'a': 'FOO', 'b': 'bar', 'c': 'foobar'})
            eq_(
                list(server.command_logs),
                [
                    dict(command='set_bulk', num_data=2, values={'a': 'foo', 'b': 'bar'}),
                    dict(command='set_bulk', num_data=1, values={'c': 'foobar'}),
                    dict(command='set_bulk', num_data=1, values={'a': 'FOO'}),
                ]
            )
            
            client.close()
            server.stop()

        thread = gevent.spawn(_test_set_bulk)
        thread.join()

    def test_get_bulk(self):
        def _test_get_bulk():
            server = KyotoTycoonMockServer()
            server.start()
            eq_(server.data[DEFAULT_DB], {})
            client = KyotoTycoonConnection(pack=False)
            server.data[DEFAULT_DB] = {'a': 'foo', 'b': 'bar', 'c': 'foobar'}

            r1 = client.get_bulk(['a', 'b'])
            eq_(r1, {'a': 'foo', 'b': 'bar'})
            eq_(
                list(server.command_logs),
                [dict(command='get_bulk', num_keys=2, keys=['a', 'b'])]
            )

            r2 = client.get_bulk(['c'])
            eq_(r2, {'c': 'foobar'})
            eq_(
                list(server.command_logs),
                [
                    dict(command='get_bulk', num_keys=2, keys=['a', 'b']),
                    dict(command='get_bulk', num_keys=1, keys=['c'])
                ]
            )

            r3 = client.get_bulk(['d', 'e'])
            eq_(r3, {})
            eq_(
                list(server.command_logs),
                [
                    dict(command='get_bulk', num_keys=2, keys=['a', 'b']),
                    dict(command='get_bulk', num_keys=1, keys=['c']),
                    dict(command='get_bulk', num_keys=2, keys=['d', 'e'])
                ]
            )

            client.close()
            server.stop()

        thread = gevent.spawn(_test_get_bulk)
        thread.join()

    def test_remove_bulk(self):
        def _test_remove_bulk():
            server = KyotoTycoonMockServer()
            server.start()
            client = KyotoTycoonConnection(pack=False)
            server.data[DEFAULT_DB] = {'a': 'foo', 'b': 'bar', 'c': 'foobar'}

            r1 = client.remove_bulk(['a', 'b'])
            eq_(r1, 2)
            eq_(server.data[DEFAULT_DB], {'c': 'foobar'})
            eq_(
                list(server.command_logs),
                [dict(command='remove_bulk', num_keys=2, keys=['a', 'b'])]
            )

            r2 = client.remove_bulk(['xxx'])
            eq_(r2, 0)
            eq_(server.data[DEFAULT_DB], {'c': 'foobar'})
            eq_(
                list(server.command_logs),
                [
                    dict(command='remove_bulk', num_keys=2, keys=['a', 'b']),
                    dict(command='remove_bulk', num_keys=1, keys=['xxx']),
                ]
            )

            r3 = client.remove_bulk(['c'])
            eq_(r3, 1)
            eq_(server.data[DEFAULT_DB], {})
            eq_(
                list(server.command_logs),
                [
                    dict(command='remove_bulk', num_keys=2, keys=['a', 'b']),
                    dict(command='remove_bulk', num_keys=1, keys=['xxx']),
                    dict(command='remove_bulk', num_keys=1, keys=['c']),
                ]
            )

            server.data[DEFAULT_DB] = {'a': 'foo', 'b': 'bar', 'c': 'foobar'}

            r4 = client.remove_bulk(['a', 'x'])
            eq_(r4, 1)
            eq_(server.data[DEFAULT_DB], {'b': 'bar', 'c': 'foobar'})
            eq_(
                list(server.command_logs),
                [
                    dict(command='remove_bulk', num_keys=2, keys=['a', 'b']),
                    dict(command='remove_bulk', num_keys=1, keys=['xxx']),
                    dict(command='remove_bulk', num_keys=1, keys=['c']),
                    dict(command='remove_bulk', num_keys=2, keys=['a', 'x']),
                ]
            )

            client.close()
            server.stop()

        thread = gevent.spawn(_test_remove_bulk)
        thread.join()

    def test_multi_db(self):
        def _test_multi_db():
            server = KyotoTycoonMockServer()
            server.start()
            client = KyotoTycoonConnection(pack=False)

            client.set_bulk({'a': 'foo1', 'b': 'bar1'}, db=1)
            eq_(client.get_bulk(["a", "b"], db=1), {'a': 'foo1', 'b': 'bar1'})
            eq_(client.get_bulk(["a", "b"], db=2), {})

            client.set_bulk({'a': 'foo2', 'b': 'bar2'}, db=2)
            eq_(client.get_bulk(["a", "b"], db=1), {'a': 'foo1', 'b': 'bar1'})
            eq_(client.get_bulk(["a", "b"], db=2), {'a': 'foo2', 'b': 'bar2'})

            client.remove_bulk(["a", "b"], db=1)
            eq_(client.get_bulk(["a", "b"], db=1), {})
            eq_(client.get_bulk(["a", "b"], db=2), {'a': 'foo2', 'b': 'bar2'})

            client.remove_bulk(["a", "b"], db=2)
            eq_(client.get_bulk(["a", "b"], db=1), {})
            eq_(client.get_bulk(["a", "b"], db=2), {})

            client.close()
            server.stop()

        thread = gevent.spawn(_test_multi_db)
        thread.join()

    @raises(KTMockTimeOutError)
    def test_timeout(self):
        server = KyotoTycoonMockServer()
        server.start()
        try:
            server.wait(n=1, timeout_msec=100)
        except Exception:
            server.stop()
            etype, value, traceback = sys.exc_info()
            raise etype, value, traceback
