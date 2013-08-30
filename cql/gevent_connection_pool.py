
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gevent
from gevent import Greenlet
from gevent.queue import Queue, Full
from gevent.coros import BoundedSemaphore
from cql.native import NativeConnection


__all__ = ['ConnectionPool']


sem = BoundedSemaphore(4)


class ConnectionPool(object):
    """DIY: the same API as in connection_pool.py

       Added BoundedSemaphore in borrow_connection() to solve issue
       with losing events during replay.
    """
    def __init__(self, hostname, port=9160, keyspace=None, user=None,
            password=None, decoder=None, max_conns=100, max_idle=5,
            eviction_delay=10000, native=False):
        self.hostname = hostname
        self.port = port
        self.keyspace = keyspace
        self.user = user
        self.password = password
        self.decoder = decoder
        self.max_conns = max_conns
        self.max_idle = max_idle
        self.eviction_delay = eviction_delay
        self.native = native

        self.size = 0
        self.pool = Queue(maxsize=max_conns)
        Eviction(self).start()

        self.pool.put(self.__create_connection())

    def __create_connection(self):
        return NativeConnection(self.hostname,
              port=self.port,
              keyspace=self.keyspace,
              user=self.user,
              password=self.password)

    def borrow_connection(self):
        u"""Method for creating new/reusing free connections
        """
        sem.acquire()
        pool = self.pool
        if pool.empty() and self.max_conns >= self.size:
            connection = self.__create_connection()
            self.size += 1
        else:
            connection = pool.get(block=True)
        sem.release()
        return connection

    def return_connection(self, connection):
        try:
            self.pool.put(connection)
        except Full:
            connection.close()


class Eviction(Greenlet):
    u"""Takes care of idle connections
    """
    def __init__(self, conn_pool):
        Greenlet.__init__(self)

        self.conn_pool = conn_pool
        self.connections = self.conn_pool.pool
        self.max_idle = self.conn_pool.max_idle
        self.eviction_delay = self.conn_pool.eviction_delay

    def _run(self):
        while(True):
            while(self.connections.qsize() > self.max_idle):
                connection = self.connections.get(block=False)
                if connection:
                    if connection.is_open():
                        connection.close()
                        self.conn_pool.size -= 1
            gevent.sleep(self.eviction_delay/1000)
