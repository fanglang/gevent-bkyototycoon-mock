gevent-bkyototycoon-mock
========================

Basic Usage
-----------

In this sample we use `python-kyototycoon-binary <https://github.com/studio-ousia/python-kyototycoon-binary>`_ for client library

.. code-block:: python

    >>> from gktmock import KyotoTycoonMockServer
    >>> import gevent.monkey
    >>> gevent.monkey.patch_all()
    >>> server = KyotoTycoonMockServer()
    >>> server.start()
    >>> from bkyototycoon import KyotoTycoonConnection
    >>> client = KyotoTycoonConnection()
    >>> client.set_bulk({'a': 'value1'})
    >>> assert server.data == {'a': 'value1'}

