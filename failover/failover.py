import os
import asyncio

from ttraft.failover import states
from ttraft.log import Log
from ttraft.messages import Message
from ttraft.utils import execute_async


class Failover:
    def __init__(self, node_id, peer_id, transport, config, state_machine):
        self.node_id = node_id
        self.peer_id = peer_id
        self.transport = transport
        self.recv_task = None
        self.config = config
        self.logger = config.logger.getChild(__name__)

        self.state_machine = state_machine
        self.log = Log(self.logger, config.serializer, config.max_log_size,
            config.persistent_dir, self.state_machine, self,
            self.config.instant_send)

        self.state_follower = states.Follower(self)
        self.state_leader = states.Leader(self)
        self.state = None
        self.term = 0
        self.started = False


    async def start(self):
        self.logger.info("Failover start")
        if self.started:
            return
        self.started = True
        self.state = self.state_follower
        await asyncio.to_thread(os.makedirs, self.config.persistent_dir,
            exist_ok=True)
        await self.transport.start()
        self.recv_task = asyncio.create_task(self.recv_routine())
        await self.log.load()
        self.state.start()
        execute_async(self.config.on_follower_cb)

    async def stop(self):
        self.logger.info("Failover stop")
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

    def become_leader(self):
        self.logger.info("Leader")
        self.state.stop()
        self.log.commit_index = self.log.last_log_index
        asyncio.create_task(self.log.apply_committed_entries())
        self.state = self.state_leader
        self.state.start()
        execute_async(self.config.on_leader_cb)

    def leader_change(self):
        self.logger.info("Leader changed")
        execute_async(self.config.on_leader_change_cb)

    def node_count(self):
        return 2

    def is_leader(self):
        return self.state == self.state_leader

    def current_leader(self):
        if self.is_leader():
            return self.node_id
        return self.peer_id

    async def client_request(self, command, blocking=False):
        if self.is_leader():
            await self.log.append(self.term, command, blocking)
            self.logger.debug(f"Client cmd: {command}")
            self.log.commit_index += 1
            await self.log.apply_committed_entries()
            return True
        self.logger.warning("Request failed: Not leader")
        return False
