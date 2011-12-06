#!/bin/env python
# 
# Copyright 2010 bit.ly
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Tools for creating `messages
<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>`_ to be sent to
MongoDB.

.. note:: This module is for internal use and is generally not needed by
   application developers.
"""

import tornado.iostream
import socket
import helpers
import struct
import logging
import functools
import message
import contextlib

from bson.son import SON
from tornado.stack_context import StackContext
from errors import ProgrammingError, IntegrityError, InterfaceError

class Connection(object):
    """
    :Parameters:
      - `host`: hostname or ip of mongo host
      - `port`: port to connect to
      - `create_callback`: callback to be called with the connected self
      - `autoreconnect` (optional): auto reconnect on interface errors
      
    """
    def __init__(self,
                 nodes,
                 slave_okay=True,
                 autoreconnect=True,
                 create_callback=None,
                 pool=None):
        assert isinstance(nodes, set)
        assert isinstance(slave_okay, bool)
        assert isinstance(autoreconnect, bool)
        assert callable(create_callback)
        assert pool
        self.__nodes = nodes
        self.__host = None
        self.__port = None
        self.__stream = None
        self.__callback = None
        self.__alive = False
        self.__slave_okay = slave_okay
        self.__autoreconnect = autoreconnect
        self.__pool = pool
        self.__repl = None
        self.usage_count = 0
        self.__connect(callback=create_callback)

    def __connect(self, callback):
        """Begin the connection process, sets up connection state
        and associated stack context.

        :Parameters:
         - `callback`: called when connected
        """
        connection_state = _ConnectionState(self.__nodes)
        connection_manager = functools.partial(self.__connection_manager,
                state=connection_state, callback=callback)
        with StackContext(connection_manager):
            self.__find_node(connection_state)
    
    def __socket_connect(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            s.connect((self.__host, self.__port))
            self.__stream = tornado.iostream.IOStream(s)
            self.__stream.set_close_callback(self._socket_close)
            self.__alive = True
        except socket.error, error:
            raise InterfaceError(error)

    def __try_node(self, node):
        """Try to connect to this node and see if it works
        for our connection type.

        :Parameters:
         - `node`: The (host, port) pair to try.

        Based on pymongo.Connection.__try_node
        """
        if self.__alive:
            self.close()
        self.__host, self.__port = node
        self.__socket_connect()

        command = message.query(
                options=0,
                collection_name='admin.$cmd',
                num_to_skip=0,
                num_to_return=-1,
                query=SON([('ismaster', 1)]))
        self.send_message(command,
            callback=functools.partial(self.__handle_ismaster, node=node), checking_master=True)

    def __handle_ismaster(self, result, error=None, node=None):
        if error:
            raise error

        if len(result['data']) == 1:
            response = result['data'][0]
        else:
            raise InterfaceError('Invalid response returned: %s' %
                    result['data'])

        # Replica Set?
        if len(self.__nodes) > 1 or self.__repl:
            # Check that this host is part of the given replica set.
            if self.__repl:
                set_name = response.get('setName')
                # The 'setName' field isn't returned by mongod before 1.6.2
                # so we can't assume that if it's missing this host isn't in
                # the specified set.
                if set_name and set_name != self.__repl:
                    raise InterfaceError("%s:%d is not a member of "
                            "replica set %s" % (node[0], node[1], self.__repl))
            if "hosts" in response:
                self.__nodes.update([_partition_node(h)
                                     for h in response["hosts"]])
            if response["ismaster"]:
                raise _NodeFound(node)
            elif "primary" in response:
                candidate = _partition_node(response["primary"])
                return self.__try_node(candidate)

            # Explain why we aren't using this connection.
            raise InterfaceError('%s:%d is not primary' % node)

        # Direct connection
        else:
            if response.get("arbiterOnly", False):
                raise ProgrammingError("%s:%d is an arbiter" % node)
            raise _NodeFound(node)

    def __find_node(self, state):
        """Find a host, port pair suitable for our connection type.

        If only one host was supplied to __init__ see if we can connect
        to it. Don't check if the host is a master/primary so we can make
        a direct connection to read from a slave.

        If more than one host was supplied treat them as a seed list for
        connecting to a replica set. Try to find the primary and fail if
        we can't. Possibly updates any replSet information on success.

        If the list of hosts is not a seed list for a replica set the
        behavior is still the same. We iterate through the list trying
        to find a host we can send write operations to.

        In either case a connection to an arbiter will never succeed.

        Based on pymongo.Connection.__find_node
        """
        try:
            node = state.remaining.pop()
        except KeyError:
            if state.tested_all_seeds:
                # We have failed to find a node...
                raise _NoNodeFound(', '.join(state.errors))
            else:
                # No luck with seeds; let's see if we discovered a new node
                state.tested_all_seeds = True
                state.remaining = self.__nodes.copy() - state.seeds
                self.__find_node(state)
        else:
            self.__try_node(node)

    @contextlib.contextmanager
    def __connection_manager(self, state, callback):
        try:
            yield
        except _NodeFound:
            callback(self)
        except _NoNodeFound, why:
            callback(None, error=why)
        except InterfaceError, why:
            state.errors.append(str(why))
            self.__find_node(state)

    def _socket_close(self):
        """cleanup after the socket is closed by the other end"""
        if self.__callback:
            self.__callback(None, InterfaceError('connection closed'))
        self.__callback = None
        self.__alive = False
        self.__host = None
        self.__port = None
        self.__pool.cache(self)
    
    def _close(self):
        """close the socket and cleanup"""
        if self.__callback:
            self.__callback(None, InterfaceError('connection closed'))
        self.__callback = None
        self.__alive = False
        self.__host = None
        self.__port = None
        self.__stream._close_callback = None
        self.__stream.close()
    
    def close(self):
        """close this connection; re-cache this connection object"""
        self._close()
        self.__pool.cache(self)
    
    def send_message(self, message, callback, checking_master=False):
        """ send a message over the wire; callback=None indicates a safe=False call where we write and forget about it"""
        
        self.usage_count +=1
        # TODO: handle reconnect
        if self.__callback is not None:
            raise ProgrammingError('connection already in use')
        
        if not self.__alive:
            if self.__autoreconnect:
                logging.warn('connection lost, reconnecting')
                self.__connect(functools.partial(Connection.send_message,
                    message=message, callback=callback))
                return
            else:
                raise InterfaceError('connection invalid. autoreconnect=False')
        
        self.__callback=callback
        # __request_id used by get_more()
        (self.__request_id, data) = message
        # logging.info('request id %d writing %r' % (self.__request_id, data))
        try:
            self.__stream.write(data)
            if callback:
                self.__stream.read_bytes(16, callback=functools.partial(self._parse_header, checking_master))
            else:
                self.__request_id = None
                self.__pool.cache(self)
                
        except IOError, e:
            self.__alive = False
            raise
        # return self.__request_id 
    
    def _parse_header(self, checking_master, header):
        # return self.__receive_data_on_socket(length - 16, sock)
        # logging.info('got data %r' % header)
        length = int(struct.unpack("<i", header[:4])[0])
        request_id = struct.unpack("<i", header[8:12])[0]
        assert request_id == self.__request_id, \
            "ids don't match %r %r" % (self.__request_id,
                                       request_id)
        operation = 1 # who knows why
        assert operation == struct.unpack("<i", header[12:])[0]
        # logging.info('%s' % length)
        # logging.info('waiting for another %d bytes' % length - 16)
        try:
            self.__stream.read_bytes(length - 16, callback=functools.partial(self._parse_response, checking_master))
        except IOError, e:
            self.__alive = False
            raise
    
    def _parse_response(self, checking_master, response):
        # logging.info('got data %r' % response)
        callback = self.__callback
        request_id = self.__request_id
        self.__request_id = None
        self.__callback = None

        if not checking_master:
            self.__pool.cache(self)
        
        try:
            response = helpers._unpack_response(response, request_id) # TODO: pass tz_awar
        except Exception, e:
            logging.error('error %s' % e)
            callback(None, e)
            return
        
        if response and response['data'] and response['data'][0].get('err') and response['data'][0].get('code'):
            # logging.error(response['data'][0]['err'])
            callback(None, IntegrityError(response['data'][0]['err'], code=response['data'][0]['code']))
            return
        # logging.info('response: %s' % response)
        callback(response)


class _ConnectionState(object):
    def __init__(self, nodes):
        self.errors = []
        self.node_found = False
        self.tested_all_seeds = False
        self.nodes = nodes
        self.seeds = nodes.copy()
        self.remaining = nodes.copy()


class _NodeFound(StandardError):
    def __init__(self, node):
        super(_NodeFound, self).__init__('Node %s:%d' % node)
        self.node = node


class _NoNodeFound(StandardError):
    pass


def _partition_node(node):
    """Split a host:port string returned from mongod/s into
    a (host, int(port)) pair needed for socket.connect().

    From pymongo.connection._partition_node
    """
    host = node
    port = 27017
    idx = node.rfind(':')
    if idx != -1:
        host, port = node[:idx], int(node[idx + 1:])
    if host.startswith('['):
        host = host[1:-1]
    return host, port
