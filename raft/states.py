import asyncio

from ttraft.timer import Timer, RandomTimer
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
        return self.state_manager.storage.current_term

    @term.setter
    def term(self, value):
        self.state_manager.storage.current_term = value

    @property
    def voted_for(self):
        return self.state_manager.storage.voted_for

    @voted_for.setter
    def voted_for(self, value):
        self.state_manager.storage.voted_for = value

    @property
    def cluster(self):
        return self.state_manager.cluster

    @property
    def new_cluster(self):
        return self.state_manager.new_cluster

    @property
    def peers(self):
        return self.state_manager.peers

    @property
    def new_peers(self):
        return self.state_manager.new_peers

    @property
    def log(self):
        return self.state_manager.log

    @property
    def transport(self):
        return self.state_manager.transport


class Follower(State):
    def __init__(self, state_manager):
        super().__init__(state_manager)
        t_min, t_max = self.state_manager.config.follower_timeout
        self.timer = RandomTimer(self.logger, t_min, t_max,
                self.state_manager.become_candidate)
        self.vote_timer = Timer(self.logger, t_min, self.set_can_vote)
        self.can_vote = False

    def start(self):
        self.voted_for = None
        #self.vote_timer.start()
        self.can_vote = True
        self.timer.start()

    def stop(self):
        self.vote_timer.stop()
        self.timer.stop()

    def set_can_vote(self):
        self.can_vote = True

    async def recv_cb(self, msg, sender):
        if self.can_vote and msg.term > self.term:
            self.term = msg.term
            self.voted_for = None

        if msg.type == MessageType.REQUEST_VOTE:
            await self.on_request_vote_msg(msg, sender)

        elif msg.type == MessageType.APPEND_ENTRIES:
            await self.on_apped_entries_msg(msg, sender)

        elif msg.type == MessageType.INSTALL_SNAPSHOT:
            await self.on_install_snapshot_msg(msg, sender)

    async def on_request_vote_msg(self, msg, sender):
        if not self.can_vote:
            # Don't vote (same as voting false)
            return

        if msg.term < self.term:
            # Don't vote (same as voting false)
            return

        if self.voted_for is None or self.voted_for == sender:
            # Check log up to date
            if (msg.last_log_term > self.log.last_log_term
                    or (msg.last_log_term == self.log.last_log_term
                        and msg.last_log_index >= self.log.last_log_index)):
                if self.voted_for is None:
                    self.voted_for = sender
                self.timer.restart()
                self.logger.info(f"Vote to {sender}")
                reply = messages.RequestVoteReply(self.term, True)
                await self.transport.send(reply.encode(), sender)

    async def on_apped_entries_msg(self, msg, sender):
        if msg.term < self.term:
            reply = messages.AppendEntriesReply(self.term, False)
            await self.transport.send(reply.encode(), sender)
            return

        if self.state_manager.leader_id != msg.leader_id:
            self.state_manager.leader_id = msg.leader_id
            self.state_manager.leader_change()
        self.vote_timer.restart()
        self.timer.restart()
        self.can_vote = False
        prev_entry = self.log.get_entry(msg.prev_log_index)
        if (msg.prev_log_index != 0
                and msg.prev_log_index != self.log.last_included_index
                and (prev_entry is None
                    or prev_entry["term"] != msg.prev_log_term)):
            self.logger.debug(f"Missing entries [{msg.prev_log_index}"
                + f" {self.log.last_log_index}]")
            reply = messages.AppendEntriesReply(self.term, False)
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
            reply = messages.AppendEntriesReply(self.term, True,
                self.log.last_log_index)
            await self.transport.send(reply.encode(), sender)
            if msg.leader_commit > self.log.commit_index:
                self.log.commit_index = min(self.log.last_log_index,
                        msg.leader_commit)
                await self.log.apply_committed_entries()

    async def on_install_snapshot_msg(self, msg, sender):
        if msg.term < self.term:
            reply = messages.InstallSnapshotReply(self.term)
            await self.transport.send(reply.encode(), sender)
            return

        self.timer.restart()
        await self.log.install_snapshot(msg.data, msg.offset)
        if msg.done:
            await self.log.install_snapshot_finished(msg.last_included_index,
                msg.last_included_term)
            self.logger.info("Snapsot installed")
            reply = messages.InstallSnapshotReply(self.term)
            await self.transport.send(reply.encode(), sender)


class Candidate(State):
    def __init__(self, state_manager):
        super().__init__(state_manager)
        t_min, t_max = self.state_manager.config.follower_timeout
        self.timer = RandomTimer(self.logger, t_min, t_max,
                self.state_manager.become_candidate)
        self.voters = set()
        self.new_voters = set()

    def add_vote(self, node_id):
        if self.log.joint_consensus:
            if node_id in self.cluster and node_id not in self.voters:
                self.voters.add(node_id)
            if node_id in self.new_cluster and node_id not in self.new_voters:
                self.new_voters.add(node_id)
            if (len(self.voters)*2 > len(self.cluster)
                    and len(self.new_voters)*2 > len(self.new_cluster)):
                self.state_manager.become_leader()

        else:
            if node_id in self.cluster and node_id not in self.voters:
                self.voters.add(node_id)
                if len(self.voters)*2 > len(self.cluster):
                    self.state_manager.become_leader()

    def start(self):
        self.voters.clear()
        self.term += 1
        self.timer.start()
        self.requests_votes()
        self.voted_for = self.state_manager.id
        self.add_vote(self.state_manager.id) # Vote to himself

    def stop(self):
        self.timer.stop()

    def requests_votes(self):
        for peer in self.peers:
            msg = messages.RequestVote(self.term, self.state_manager.id,
                    self.log.last_log_index, self.log.last_log_term)
            asyncio.create_task(self.transport.send(msg.encode(), peer))
        if self.log.joint_consensus:
            for peer in self.new_peers:
                if peer in self.peers:
                    continue # Already sent
                msg = messages.RequestVote(self.term, self.state_manager.id,
                        self.log.last_log_index, self.log.last_log_term)
                asyncio.create_task(self.transport.send(msg.encode(), peer))

    async def recv_cb(self, msg, sender):
        if ((msg.term == self.term and msg.type == MessageType.APPEND_ENTRIES)
                or msg.term > self.term):
            self.term = msg.term
            self.state_manager.become_follower()
            await self.state_manager.state.recv_cb(msg, sender)

        elif msg.type == MessageType.REQUEST_VOTE:
            reply = messages.RequestVoteReply(self.term, False)
            await self.transport.send(reply.encode(), sender)

        elif msg.type == MessageType.REQUEST_VOTE_REPLY:
            if msg.vote_granted:
                self.add_vote(sender)

        elif msg.type == MessageType.APPEND_ENTRIES:
            reply = messages.AppendEntriesReply(self.term, False)
            await self.transport.send(reply.encode(), sender)


class Leader(State):
    def __init__(self, state_manager):
        super().__init__(state_manager)
        period = self.state_manager.config.heartbeat_period
        self.timer = Timer(self.logger, period, self.send_heartbeats)

    def start(self):
        self.log.next_index.clear()
        self.log.match_index.clear()
        for peer in self.peers:
            self.log.next_index[peer] = self.log.last_log_index+1
            self.log.match_index[peer] = 0
        if self.log.joint_consensus:
            for peer in self.new_peers:
                self.log.next_index_new[peer] = self.log.last_log_index+1
                self.log.match_index_new[peer] = 0
        asyncio.create_task(self.send_heartbeats())
        asyncio.create_task(self.log.check_majority())

    def stop(self):
        self.timer.stop()

    async def send_heartbeats(self):
        self.logger.debug("Sending heartbeats")
        for peer in self.peers:
            await self.append_entries(peer)
        if self.log.joint_consensus:
            for peer in self.new_peers:
                if peer in self.peers:
                    continue # Already sent
                await self.append_entries(peer)

        self.timer.restart()

    async def append_entries(self, peer):
        next_index = self.log.get_next_index(peer)
        # If below snapshot, send snapshot and return
        if next_index != 0 and next_index <= self.log.last_included_index:
            self.logger.info("Sending snapshot")
            #TODO: send snapshot in pieces?
            snapshot_data = await self.log.get_raw_snapshot()
            msg = messages.InstallSnapshot(self.term, self.state_manager.id,
                self.log.last_included_index, self.log.last_included_term,
                0, snapshot_data, True)
            await self.transport.send(msg.encode(), peer)
            self.log.update_next_index(peer, self.log.last_log_index+1)
            self.log.update_match_index(peer, 0)
            return

        entries = []
        if self.log.last_log_index >= next_index:
            entries = self.log.get_entry_range(next_index)
        prev_log_index = next_index-1
        prev_log_term = self.log.get_entry_term(prev_log_index)

        msg = messages.AppendEntries(self.term, self.state_manager.id,
                prev_log_index, prev_log_term, entries,
                self.log.commit_index)
        await self.transport.send(msg.encode(), peer)

    async def recv_cb(self, msg, sender):
        if msg.type != MessageType.REQUEST_VOTE and msg.term > self.term:
            self.term = msg.term
            self.state_manager.become_follower()
            await self.state_manager.state.recv_cb(msg, sender)

        elif msg.type == MessageType.APPEND_ENTRIES:
            reply = messages.AppendEntriesReply(self.term, False)
            await self.transport.send(reply.encode(), sender)

        elif msg.type == MessageType.APPEND_ENTRIES_REPLY:
            await self.on_append_entries_reply(msg, sender)

    async def on_append_entries_reply(self, msg, sender):
        self.logger.debug(f"AppenEntries reply from {sender}: {msg.success},"
                + f" {msg.last_log_index}")
        if not msg.success:
            self.log.update_next_index(sender,
                self.log.get_next_index(sender)-1)
            await self.append_entries(sender)
        else:
            self.log.update_next_index(sender, msg.last_log_index+1)
            self.log.update_match_index(sender, msg.last_log_index)
            await self.log.check_majority()
