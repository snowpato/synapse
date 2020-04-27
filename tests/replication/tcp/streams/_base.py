# -*- coding: utf-8 -*-
# Copyright 2019 New Vector Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import Any, Dict, List, Tuple

from synapse.replication.tcp.handler import ReplicationCommandHandler
from synapse.replication.tcp.protocol import ClientReplicationStreamProtocol
from synapse.replication.tcp.resource import ReplicationStreamProtocolFactory

from tests import unittest
from tests.server import FakeTransport


class BaseStreamTestCase(unittest.HomeserverTestCase):
    """Base class for tests of the replication streams"""

    def __init__(self, methodName, *args, **kwargs):
        super().__init__(methodName, *args, **kwargs)
        self.test_handler = TestReplicationDataHandler()

    def make_homeserver(self, reactor, clock):
        return self.setup_test_homeserver(replication_data_handler=self.test_handler)

    def prepare(self, reactor, clock, hs):
        # build a replication server
        server_factory = ReplicationStreamProtocolFactory(hs)
        self.streamer = hs.get_replication_streamer()
        self.server = server_factory.buildProtocol(None)

        repl_handler = ReplicationCommandHandler(hs)
        self.client = ClientReplicationStreamProtocol(
            hs, "client", "test", clock, repl_handler,
        )

        self._client_transport = None
        self._server_transport = None

    def reconnect(self):
        if self._client_transport:
            self.client.close()

        if self._server_transport:
            self.server.close()

        self._client_transport = FakeTransport(self.server, self.reactor)
        self.client.makeConnection(self._client_transport)

        self._server_transport = FakeTransport(self.client, self.reactor)
        self.server.makeConnection(self._server_transport)

    def disconnect(self):
        if self._client_transport:
            self._client_transport = None
            self.client.close()

        if self._server_transport:
            self._server_transport = None
            self.server.close()

    def replicate(self):
        """Tell the master side of replication that something has happened, and then
        wait for the replication to occur.
        """
        self.streamer.on_notifier_poke()
        self.pump(0.1)


class TestReplicationDataHandler:
    """Drop-in for ReplicationDataHandler which just collects RDATA rows"""

    def __init__(self):
        # streams to subscribe to: map from stream id to position
        self.stream_positions = {}  # type: Dict[str, int]

        # list of received (stream_name, token, row) tuples
        self.received_rdata_rows = []  # type: List[Tuple[str, int, Any]]

    def get_streams_to_replicate(self):
        return self.stream_positions

    async def on_rdata(self, stream_name, token, rows):
        for r in rows:
            self.received_rdata_rows.append((stream_name, token, r))

        if (
            stream_name in self.stream_positions
            and token > self.stream_positions[stream_name]
        ):
            self.stream_positions[stream_name] = token

    async def on_position(self, stream_name, token):
        pass
