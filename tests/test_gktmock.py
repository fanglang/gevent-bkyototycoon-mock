#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
from unittest import TestCase
from nose.tools import eq_, raises
import gevent
import gevent.socket
import gevent.monkey
gevent.monkey.patch_all()

from gktmock import KyotoTycoonMockServer
from bkyototycoon import KyotoTycoonConnection


class KyotoTycoonMockServerTestCase(TestCase):

    def test_set_bulk(self):
        def _test_set_bulk():
            server = KyotoTycoonMockServer()
            server.start()
            eq_(server.data, {})
            client = KyotoTycoonConnection(pack=False)
            
            client.set_bulk({'a': 'foo', 'b': 'bar'})
            eq_(server.data, {'a': 'foo', 'b': 'bar'})
            
            client.set_bulk({'c': 'foobar'})
            eq_(server.data, {'a': 'foo', 'b': 'bar', 'c': 'foobar'})
            
            client.set_bulk({'a': 'FOO'})
            eq_(server.data, {'a': 'FOO', 'b': 'bar', 'c': 'foobar'})
            
            client.close()
            server.stop()

        thread = gevent.spawn(_test_set_bulk)
        thread.join()

    def test_get_bulk(self):
        def _test_get_bulk():
            server = KyotoTycoonMockServer()
            server.start()
            eq_(server.data, {})
            client = KyotoTycoonConnection(pack=False)
            server.data = {'a': 'foo', 'b': 'bar', 'c': 'foobar'}

            r1 = client.get_bulk(['a', 'b'])
            eq_(r1, {'a': 'foo', 'b': 'bar'})

            r2 = client.get_bulk(['c'])
            eq_(r2, {'c': 'foobar'})

            r3 = client.get_bulk(['d', 'e'])
            eq_(r3, {})

            client.close()
            server.stop()

        thread = gevent.spawn(_test_get_bulk)
        thread.join()

    def test_remove_bulk(self):
        def _test_remove_bulk():
            server = KyotoTycoonMockServer()
            server.start()
            client = KyotoTycoonConnection(pack=False)
            server.data = {'a': 'foo', 'b': 'bar', 'c': 'foobar'}

            r1 = client.remove_bulk(['a', 'b'])
            eq_(r1, 2)
            eq_(server.data, {'c': 'foobar'})

            r2 = client.remove_bulk(['xxx'])
            eq_(r2, 0)
            eq_(server.data, {'c': 'foobar'})

            r3 = client.remove_bulk(['c'])
            eq_(r3, 1)
            eq_(server.data, {})

            server.data = {'a': 'foo', 'b': 'bar', 'c': 'foobar'}

            r4 = client.remove_bulk(['a', 'x'])
            eq_(r4, 1)
            eq_(server.data, {'b': 'bar', 'c': 'foobar'})

            client.close()
            server.stop()

        thread = gevent.spawn(_test_remove_bulk)
        thread.join()

    @raises(StandardError)
    def test_timeout(self):
        def _test_timeout():
            server = KyotoTycoonMockServer()
            server.start()
            try:
                server.wait(n=1, timeout_msec=100)
            except Exception:
                server.stop()
                etype, value, traceback = sys.exc_info()
                raise etype, value, traceback

        thread = gevent.spanw(_test_timeout)
        thread.join()
