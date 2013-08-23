
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

from gevent.queue import Queue, Empty, Full
import gevent
from threading import Thread
from time import sleep
# from cql.connection import Connection
from cql.native import NativeConnection
from cql.thrifteries import ThriftConnection


__all__ = ['ConnectionPool']


class ConnectionPool(object):
    """DIY: the same API as in connection_pool.py
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

        self.connections = Queue(maxsize=self.max_conns)
        self.busy = Queue(maxsize=self.max_conns)

        self.connections.put(self.__create_connection())
        self.eviction = Eviction(self.connections,
                                 self.max_idle,
                                 self.eviction_delay)

    def __create_connection(self):
        if self.native:
            return NativeConnection(self.hostname,
                  port=self.port,
                  keyspace=self.keyspace,
                  user=self.user,
                  password=self.password)
        return ThriftConnection(self.hostname,
              port=self.port,
              keyspace=self.keyspace,
              user=self.user,
              password=self.password)

    def borrow_connection(self):
        u"""Method for creating new/reusing free connections
            2 queues:
                > busy - keeps information of number of connections in use
                > connections - keeps free connections to reuse
        """
        try:
            connection = self.__create_connection()
            try:
                connection = self.connections.get(block=False)
            except Empty:
                pass
            finally:
                self.busy.put(connection)
                return connection
        except Full:
            # if busy is full, wait for free connections
            connection.close()
            gevent.sleep(.001)
            return self.borrow_connection()

    def return_connection(self, connection):
        try:
            self.busy.get(block=False)
            # prevent overflowing connections Queue
            self.connections.put(connection)
        except Full:
            connection.close()


class Eviction(Thread):
    def __init__(self, connections, max_idle, eviction_delay):
        Thread.__init__(self)

        self.connections = connections
        self.max_idle = max_idle
        self.eviction_delay = eviction_delay

        self.setDaemon(True)
        self.setName("EVICTION-THREAD")
        self.start()

    def run(self):
        while(True):
            while(self.connections.qsize() > self.max_idle):
                connection = self.connections.get(block=False)
                if connection:
                    if connection.is_open():
                        connection.close()
            sleep(self.eviction_delay/1000)
