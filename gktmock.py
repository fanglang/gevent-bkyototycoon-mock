#!/usr/bin/python
# -*- coding: utf-8 -*-
import gevent
import time
import struct
from collections import deque
from gevent.server import StreamServer

MB_SET_BULK = 0xb8
MB_GET_BULK = 0xba
MB_REMOVE_BULK = 0xb9
FLAG_NOREPLY = 0x01

class KTMockTimeOutError(Exception):
    pass

class KyotoTycoonMockServer(object):

    def __init__(self):
        self.data = {}
        self.command_logs = deque([])
        self.stream_server = None

    def __call__(self, sock, address):
        """StreamServer framework will call this instance as callable object"""
        self.sock = sock
        self.address = address
        self._run()

    def _run(self):
        """receive log data and put into each queue"""
        while True:
            # determine command type
            (command,) = struct.unpack('!B', self._read(1))

            if command == MB_SET_BULK:
                # determine number of key-value pairs
                flag, num_data = struct.unpack('!II', self._read(4+4))

                # receive pairs
                data_to_set = {}
                for i in range(num_data):
                    # !HIIq => 2 + 4 + 4 + 8
                    kv_header = self._read(2+4+4+8)
                    zero, key_len, val_len, lifetime = struct.unpack('!HIIq', kv_header)
                    key = self.sock.recv(key_len)
                    val = self.sock.recv(val_len)
                    data_to_set[key] = val

                # update cache memory
                self.data.update(data_to_set)

                # log command
                self.command_logs.append(dict(
                    command='set_bulk',
                    num_data=num_data,
                    values=data_to_set
                ))

                # reply
                if flag != FLAG_NOREPLY:
                    reply = struct.pack('!BI', MB_SET_BULK, len(data_to_set))
                    self.sock.send(reply)

            elif command == MB_GET_BULK:
                # command: get bulk
                zero, num_keys = struct.unpack('!II', self._read(4+4))

                keys = []
                for i in range(num_keys):
                    k_header = self._read(2+4)
                    zero, key_len = struct.unpack('!HI', k_header)
                    key = self.sock.recv(key_len)
                    keys.append(key)

                found_keys = [k for k in keys if k in self.data]

                # reply
                self.sock.send(struct.pack('!BI', MB_GET_BULK, len(found_keys)))
                for key in found_keys:
                    value = self.data[key]
                    kv_data = struct.pack('!HIIq', 0, len(key), len(value), 0)
                    self.sock.send(kv_data)
                    self.sock.send(key)
                    self.sock.send(value)

            elif command == MB_REMOVE_BULK:
                # command: remove bulk
                flag, num_keys = struct.unpack('!II', self._read(4+4))

                keys = []
                for i in range(num_keys):
                    k_header = self._read(2+4)
                    zero, key_len = struct.unpack('!HI', k_header)
                    key = self.sock.recv(key_len)
                    keys.append(key)

                found_keys = [k for k in keys if k in self.data]
                for k in found_keys:
                    del self.data[k]

                # reply
                if flag != FLAG_NOREPLY:
                    self.sock.send(struct.pack('!BI', MB_REMOVE_BULK, len(found_keys)))
            else:
                pass  # unsupported operation

    def _read(self, length):
        buf = ''
        readlen = 0
        while len(buf) < length:
            chunk = self.sock.recv(length - readlen)
            if chunk == '':
                gevent.sleep()
            buf += chunk
            readlen += len(chunk)
        return buf

    def clear_data(self):
        self.data = {}
        self.command_logs = deque([])

    def wait(self, n, timeout_msec=0):
        """wait until log data queue named with 'tag' is filled with 'n' items"""
        time_msec_start = int(time.time() * 1000)

        while len(self.data) < n:
            gevent.sleep()
            if 0 < timeout_msec and time_msec_start + timeout_msec < int(time.time() * 1000):
                raise KTMockTimeOutError('wait(n=%d) timed out' % n)

    def start(self, port=1978, bind_address='127.0.0.1'):
        if hasattr(self, 'stream_server') and self.stream_server is not None:
            raise StandardError('server is already started')
        self.stream_server = StreamServer((bind_address, port), self)
        self.stream_server.start()

    def stop(self):
        if self.stream_server is None:
            raise StandardError('server has not started yet')
        self.stream_server.stop()
        self.stream_server.close()
        self.stream_server = None


def main():
    import gevent.monkey
    gevent.monkey.patch_all()
    import pprint

    port = 1978
    mock_server = KyotoTycoonMockServer()
    print 'mock server created'
    mock_server.start(port)
    print 'mock server started'

    from bkyototycoon import KyotoTycoonConnection
    client = KyotoTycoonConnection(host='127.0.0.1', port=port, pack=False)
    print 'client created'
    client.set_bulk({"a": "value1", "b": "value2", "c": "value3"})
    print 'client sent set_bulk request'

    r1 = client.get_bulk(["a", "c"])
    print 'r1=%s' % pprint.pformat(r1)

    r2 = client.get_bulk(["a"])
    print 'r2=%s' % pprint.pformat(r2)

    r3 = client.get_bulk(["c", "b"])
    print 'r2=%s' % pprint.pformat(r3)

    print 'mock_server.data=%s' % pprint.pformat(mock_server.data)

    mock_server.wait(3)
    mock_server.stop()


if __name__ == '__main__':
    gth = gevent.spawn(main)
    gth.join()
    print 'END'
