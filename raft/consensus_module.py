import os
import asyncio

from ttraft.raft import states
from ttraft.log import Log
from ttraft.messages import Message
from ttraft.raft.storage import Storage
from ttraft.utils import execute_async


class ConsensusModule:
    def __init__(self, _id, cluster, transport, config, state_machine):
        self.id = _id
        self.leader_id = None
        self.cluster = cluster # Committed cluster
        self.new_cluster = [] # New cluster, used during cluster changes

        self.transport = transport
        self.recv_task = None
        self.config = config
        self.logger = config.logger.getChild(__name__)

        self.storage = Storage(self.config.persistent_dir)
        self.state_machine = state_machine
        self.log = Log(self.logger, config.serializer, config.max_log_size,
            config.persistent_dir, self.state_machine, self,
            self.config.instant_send)

        self.state_follower = states.Follower(self)
        self.state_candidate = states.Candidate(self)
        self.state_leader = states.Leader(self)
        self.state = None
        self.started = False

    @property
    def peers(self):
        peers = self.cluster.copy()
        if self.id in peers:
            peers.remove(self.id)
        return peers

    @property
    def new_peers(self):
        peers = self.new_cluster.copy()
        if self.id in peers:
            peers.remove(self.id)
        return peers

    async def start(self):
        self.logger.info("Raft start")
        if self.started:
            return
        self.started = True
        self.state = self.state_follower
        await asyncio.to_thread(os.makedirs, self.config.persistent_dir,
            exist_ok=True)
        await self.transport.start()
        self.recv_task = asyncio.create_task(self.recv_routine())
        await self.storage.load()
        await self.log.load()
        self.state.start()
        execute_async(self.config.on_follower_cb)

    async def stop(self):
        self.logger.info("Raft stop")
        if not self.started:
            return
        self.started = False
        await self.transport.stop()
        self.recv_task.cancel()
        self.state.stop()

    async def restart(self):
        await self.stop()
        await self.start()

    async def recv_routine(self):
        try:
            while True:
                data, sender = await self.transport.recv()
                msg = Message.decode(data)
                self.logger.debug(f"Recv msg {msg.type} from {sender}")
                await self.state.recv_cb(msg, sender)
        except asyncio.CancelledError:
            pass
        except:
            self.logger.exception("RECV exception")
            raise

    def become_follower(self):
        self.logger.info("Follower")
        self.state.stop()
        self.state = self.state_follower
        self.state.start()
        execute_async(self.config.on_follower_cb)

    def become_candidate(self):
        self.logger.info("Candidate")
        self.leader_id = None
        self.state.stop()
        self.state = self.state_candidate
        self.state.start()
        execute_async(self.config.on_candidate_cb)

    def become_leader(self):
        self.logger.info("Leader")
        self.leader_id = self.id
        self.state.stop()
        self.state = self.state_leader
        self.state.start()
        execute_async(self.config.on_leader_cb)

    def leader_change(self):
        self.logger.info("Leader changed")
        execute_async(self.config.on_leader_change_cb)

    def node_count(self):
        return len(self.peers) + 1

    def is_leader(self):
        return self.state == self.state_leader

    def current_leader(self):
        return self.leader_id

    async def client_request(self, command, blocking=False):
        if self.is_leader():
            await self.log.append(self.storage.current_term, command, blocking)
            self.logger.debug(f"Client cmd: {command}")
            return True
        self.logger.warning("Request failed: Not leader")
        return False

    async def client_new_cluster(self, cluster, blocking=False):
        if self.is_leader():
            self.new_cluster = cluster
            await self.log.append(self.storage.current_term,
                {"__c_old": self.cluster, "__c_new": self.new_cluster},
                blocking)
            self.logger.debug(f"Client new cluster: {len(cluster)}")
            return True
        self.logger.warning("New cluster failed: Not leader")
        return False
