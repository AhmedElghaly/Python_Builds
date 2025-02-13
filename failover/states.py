import asyncio

from ttraft.timer import Timer
from ttraft import messages
from ttraft.messages import MessageType


class State:
    def __init__(self, state_manager):
        self.state_manager = state_manager

    @property
    def logger(self):
        return self.state_manager.logger

    @property
    def term(self):
        return self.state_manager.term

    @term.setter
    def term(self, value):
        self.state_manager.term = value

    @property
    def log(self):
        return self.state_manager.log

    @property
    def transport(self):
        return self.state_manager.transport


class Follower(State):
    def __init__(self, state_manager):
        super().__init__(state_manager)
        _, t_max = self.state_manager.config.follower_timeout
        self.timer = Timer(self.logger, t_max, self.state_manager.become_leader)

    def start(self):
        self.timer.start()

    def stop(self):
        self.timer.stop()

    async def recv_cb(self, msg, sender):
        if msg.term > self.term:
            self.term = msg.term
        elif msg.term < self.term:
            self.state_manager.become_leader()
            self.state_manager.leader_change()
            return

        if msg.type == MessageType.FAILOVER_ENTRIES:
            await self.on_failover_entries_msg(msg, sender)

        if msg.type == MessageType.INSTALL_SNAPSHOT:
            await self.on_install_snapshot_msg(msg, sender)

    async def on_failover_entries_msg(self, msg, sender):
        self.timer.restart()
        prev_entry = self.log.get_entry(msg.prev_log_index)
        if (msg.prev_log_index != 0
                and msg.prev_log_index != self.log.last_included_index
                and (prev_entry is None
                    or prev_entry["term"] != msg.prev_log_term)):
            self.logger.debug(f"Missing entries [{msg.prev_log_index}"
                + f" {self.log.last_log_index}]")
            self.logger.log(9, f"{msg.prev_log_index=}, " +
                f"{self.log.last_included_index=}, {prev_entry=}, " +
                f"{msg.prev_log_term=}")
            reply = messages.FailoverEntriesReply(self.term, False,
                    self.log.last_log_index)
            await self.transport.send(reply.encode(), sender)
        else:
            self.logger.debug(f"Appending {len(msg.entries)}")
            for n, entry in enumerate(msg.entries):
                s_entry = self.log.get_entry(msg.prev_log_index+n+1)
                if s_entry is not None and s_entry["term"] != entry["term"]:
                    await self.log.remove_from(msg.prev_log_index+n)
                    await self.log.append(entry["term"], entry["command"])
                if s_entry is None:
                    await self.log.append(entry["term"], entry["command"])
            reply = messages.FailoverEntriesReply(self.term, True,
                self.log.last_log_index)
            await self.transport.send(reply.encode(), sender)
            if len(msg.entries):
                self.log.commit_index = self.log.last_log_index
                await self.log.apply_committed_entries()

    async def on_install_snapshot_msg(self, msg, sender):
        if msg.term < self.term:
            reply = messages.InstallSnapshotReply(self.term)
            await self.transport.send(reply.encode(), sender)
            return

        self.timer.restart()
        await self.log.install_snapshot(msg.data, msg.offset)
        if msg.done:
            await self.log.install_snapshot_finished(
                msg.last_included_index, msg.last_included_term)
            self.logger.info("Snapsot installed")
            reply = messages.InstallSnapshotReply(self.term)
            await self.transport.send(reply.encode(), sender)


class Leader(State):
    def __init__(self, state_manager):
        super().__init__(state_manager)
        period = self.state_manager.config.heartbeat_period
        self.timer = Timer(self.logger, period, self.send_heartbeats)
        self.last_sent_index = 0

    def start(self):
        self.term += 1
        self.last_sent_index = self.log.commit_index
        asyncio.create_task(self.send_heartbeats())

    def stop(self):
        self.timer.stop()

    async def send_heartbeats(self):
        self.logger.debug("Sending heartbeat")
        await self.append_entries(self.state_manager.peer_id,
            self.last_sent_index)
        self.last_sent_index = self.log.commit_index
        self.timer.restart()

    async def append_entries(self, peer, index):
        # If below snapshot, send snapshot and return
        # If above highest log entry, send snapshot and return (out of sync)
        if (index < self.log.last_included_index or
                index > self.log.last_log_index):
            self.logger.info("Sending snapshot")
            #TODO: send snapshot in pieces?
            snapshot_data = await self.log.get_raw_snapshot()
            msg = messages.InstallSnapshot(self.term,
                self.state_manager.node_id, self.log.last_included_index,
                self.log.last_included_term, 0, snapshot_data, True)
            await self.transport.send(msg.encode(), peer)
            return

        entries = []
        if self.log.last_log_index > index:
            entries = self.log.get_entry_range(index+1)
        prev_log_term = self.log.get_entry_term(index)

        msg = messages.FailoverEntries(self.term, index, prev_log_term, entries)
        await self.transport.send(msg.encode(), peer)

    async def recv_cb(self, msg, sender):
        if msg.term > self.term:
            self.term = msg.term
            self.state_manager.become_follower()
            self.state_manager.leader_change()
            await self.state_manager.state.recv_cb(msg, sender)

        elif msg.type == MessageType.FAILOVER_ENTRIES_REPLY:
            self.logger.log(9, f"Failover entries reply: {msg.term=}, " +
                f"{msg.success=}, {msg.last_log_index=}")
            if not msg.success:
                await self.append_entries(sender, msg.last_log_index)
