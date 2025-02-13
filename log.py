import os
import asyncio


class Log:
    """ Raft log class. Replicated across all the cluster nodes, keeps
    track of the commands applied to the state machine.

    C_old,new: {"__c_old": [p1, p2, ..], "__c_new": [p1, p2, ..]}"
    C_new: {"__c_new2": [p1, p2, ..]}"

    :param logger: Logger object.
    :type logger: "logging.Logger"
    :param serializer: Serializer to write to disk.
    :param max_log_size: Max log size before compaction.
    :type max_log_size: int
    :param persistent_dir: Directory where to store persistent files.
    :type persistent_dir: str
    :param state_machine: State machine.
    :type state_machine: instance StateMachine
    :param instant_send: Sent append_entries if new entry is appended.
    :type instant_send: bool

    :ivar entries: Command entries. It is a list, containing the
        different entires. Entries are a dict with two keys, "term",
        the entry corresponding term, and "command", the command itself.
    :vartype entries: List[Dict[str, object]]

    :ivar commit_index: Index of the highest commited entry.
    :vartype commit_index: int
    :ivar last_applied: Index of the highest entry applied to the state
        machine.
    :vartype last_applied: int

    :ivar next_index: Only used if leader. Keep track of the next entry
        index for each peer in the cluster. Keys are the peer ids,
        values are the that peer index.
    :vartype next_index: Dict[object, int]
    :ivar match_index: Only used if leader. Highest log entry
        replicated on each peer. Keys are the peer ids, values are
        that peer index.
    :vartype match_index: Dict[object, int]

    :ivar last_log_index: Index of the last entry.
    :vartype last_log_index: int
    :ivar last_log_term: Term of the last entry.
    :vartype last_log_term: int

    :ivar last_included_index: Index of the last entry in the snapshot.
    :vartype last_included_index: int
    :ivar last_log_term: Index of the las entry in the snapshot.
    :vartype last_included_term: int
    """
    def __init__(self, logger, serializer, max_log_size, persistent_dir,
            state_machine, state_manager, instant_send):
        self.logger = logger.getChild(__name__)
        self.serializer = serializer
        self.max_log_size = max_log_size
        self.log_file = os.path.join(persistent_dir, "raft.log")
        self.snapshot_file = os.path.join(persistent_dir, "raft.snapshot")
        self.state_machine = state_machine
        self.state_manager = state_manager
        self.instant_send = instant_send

        self.entries = []

        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}

        self.last_included_index = 0
        self.last_included_term = 0

        self.joint_consensus = False
        self.next_index_new = {}
        self.match_index_new = {}

    @property
    def last_log_index(self):
        return self.last_included_index + len(self.entries)

    @property
    def last_log_term(self):
        if self.entries:
            return self.entries[-1]["term"]
        return self.last_included_term

    def get_next_index(self, peer_id):
        if peer_id in self.next_index:
            return self.next_index[peer_id]
        if self.joint_consensus and self.next_index_new[peer_id]:
            return self.next_index_new[peer_id]
        return 0

    def update_next_index(self, peer_id, index):
        if peer_id in self.next_index:
            self.next_index[peer_id] = index
        if self.joint_consensus and peer_id in self.next_index_new:
            self.next_index_new[peer_id] = index

    def get_match_index(self, peer_id):
        if peer_id in self.match_index:
            return self.match_index[peer_id]
        if self.joint_consensus and self.match_index_new[peer_id]:
            return self.match_index_new[peer_id]
        return 0

    def update_match_index(self, peer_id, index):
        if peer_id in self.next_index:
            self.match_index[peer_id] = index
        if self.joint_consensus and peer_id in self.match_index_new:
            self.match_index_new[peer_id] = index

    def _load(self):
        if os.path.isfile(self.snapshot_file):
            with open(self.snapshot_file, "rb") as f:
                snapshot = self.serializer.unpack(f.read())
            self.last_included_index = snapshot["last_included_index"]
            self.last_included_term = snapshot["last_included_term"]
            self.last_applied = self.last_included_index
            self.process_membership_command(snapshot["cluster"])
            self.state_machine.apply_snapshot(snapshot["data"])
            self.logger.info(f"Snapshot load ({self.last_included_index})")

        self.entries.clear()
        if os.path.isfile(self.log_file):
            with open(self.log_file, "rb") as f:
                for line in f.readlines():
                    entry = self.serializer.unpack(line[:-1])
                    if entry is None:
                        continue
                    self.process_membership_command(entry["command"])
                    self.entries.append(entry)
        else:
            open(self.log_file, 'x').close()

    async def load(self):
        await asyncio.to_thread(self._load)

    def get_entry(self, index):
        offset = self.last_included_index
        if self.entries and offset+1 <= index <= offset+len(self.entries):
            return self.entries[index-offset-1]
        return None

    def get_entry_range(self, init=None, end=None):
        offset = self.last_included_index
        if not init:
            init = offset + 1
        if not end:
            end = offset + len(self.entries)
        return self.entries[init-offset-1:end-offset]

    def get_entry_term(self, index):
        entry = self.get_entry(index)
        if entry:
            return entry["term"]
        if entry is None and index <= self.last_included_index:
            return self.last_included_term
        return None

    def _append(self, term, command):
        entry = {"term": term, "command": command}
        with open(self.log_file, "ab") as f:
            f.write(self.serializer.pack(entry) + "\n".encode())
        self.process_membership_command(command)
        self.entries.append(entry)

    def process_membership_command(self, command):
        if "__c_old" in command:
            self.state_manager.cluster = command["__c_old"].copy()
            self.state_manager.new_cluster = command["__c_new"].copy()
            self.joint_consensus = True
            if self.state_manager.is_leader():
                for peer in self.state_manager.new_peers:
                    self.next_index_new[peer] = self.last_log_index+1
                    self.match_index_new[peer] = 0
        elif "__c_new2" in command:
            self.state_manager.cluster = command["__c_new2"].copy()
            self.state_manager.new_cluster = []
            self.joint_consensus = False
            if self.state_manager.is_leader():
                self.next_index = self.next_index_new
                self.match_index = self.match_index_new
                self.next_index_new = {}
                self.match_index_new = {}

    async def append(self, term, command, blocking=False):
        await asyncio.to_thread(self._append, term, command)
        await asyncio.to_thread(self._check_log_size)
        if self.instant_send and self.state_manager.is_leader():
            await self.state_manager.state.send_heartbeats()
        if blocking:
            index = self.last_included_index + len(self.entries)
            while self.commit_index < index:
                await asyncio.sleep(0.2)

    async def apply_committed_entries(self):
        while self.commit_index > self.last_applied:
            command = self.get_entry(self.last_applied+1)["command"]

            if "__c_old" in command and self.state_manager.is_leader():
                if not hasattr(self.state_manager, 'id'):
                    self.last_applied += 1
                    continue # Algorithm does not implement joint consensus
                for entry in self.get_entry_range(self.last_applied+2):
                    c = entry["command"]
                    if "__c_new2" in c and c["__c_new2"] == command["__c_new"]:
                        break
                else:
                    await self.append(self.state_manager.storage.current_term,
                        {"__c_new2": command["__c_new"]})

            elif "__c_new2" in command:
                if not hasattr(self.state_manager, 'id'):
                    self.last_applied += 1
                    continue # Algorithm does not implement joint consensus
                if (self.state_manager.id not in self.state_manager.cluster
                        and self.state_manager.is_leader()):
                    self.state_manager.become_follower()

            else:
                self.state_machine.apply_command(command)
            self.last_applied += 1
        if self.instant_send and self.state_manager.is_leader():
            await self.state_manager.state.send_heartbeats()

    async def check_majority(self):
        index_values = list(self.match_index.values())
        if self.state_manager.id in self.state_manager.cluster:
            index_values.append(self.last_log_index)
        index_values.sort(reverse=True)
        med_value = index_values[len(index_values)//2]

        if self.joint_consensus:
            new_index_values = list(self.match_index_new.values())
            if self.state_manager.id in self.state_manager.new_cluster:
                new_index_values.append(self.last_log_index)
            new_index_values.sort(reverse=True)
            new_med_value = new_index_values[len(new_index_values)//2]

            if (med_value > self.commit_index
                    and new_med_value > self.commit_index):
                self.commit_index = min(med_value, new_med_value)
                await self.apply_committed_entries()

        else:
            if med_value > self.commit_index:
                self.commit_index = med_value
                await self.apply_committed_entries()

    def _rewrite_log(self, entries):
        self.entries = []
        open(self.log_file, 'w').close()
        for e in entries:
            self._append(e["term"], e["command"])

    async def remove_from(self, index):
        """ Remove all entries higher than the given index. The given
        index is not included.

        :param index: Index of the last entry to keep.
        :type index: int
        """
        if index >= self.last_log_index:
            return
        aux = self.get_entry_range(end=index)
        await asyncio.to_thread(self._rewrite_log, aux)

    def _check_log_size(self):
        if os.path.getsize(self.log_file) >= self.max_log_size:
            self.logger.info(f"Creating snapshot at {self.last_applied}")

            # Get last committed cluster membership change
            cluster = {}
            for entry in reversed(self.get_entry_range(end=self.last_applied)):
                c = entry["command"]
                if "__c_old" in c or "__c_new2" in c:
                    cluster = c
                    break

            snapshot_data = self.state_machine.get_snapshot()
            snapshot = {
                "last_included_index": self.last_applied,
                "last_included_term": self.get_entry_term(self.last_applied),
                "cluster": cluster,
                "data": snapshot_data
            }

            try:
                os.replace(self.snapshot_file, self.snapshot_file+".backup")
            except FileNotFoundError:
                pass

            with open(self.snapshot_file, "wb") as f:
                f.write(self.serializer.pack(snapshot))

            aux = self.get_entry_range(self.last_applied+1)
            self.last_included_term = self.get_entry_term(self.last_applied)
            self.last_included_index = self.last_applied

            self._rewrite_log(aux)

    def _install_snapshot(self, data, offset):
        if offset == 0:
            open(self.snapshot_file+".recv", 'w').close()
        with open(self.snapshot_file+".recv", "ab") as f:
            f.write(data)

    async def install_snapshot(self, data, offset):
        await asyncio.to_thread(self._install_snapshot, data, offset)


    def _get_raw_snapshot(self):
        with open(self.snapshot_file, "rb") as f:
            snapshot_data = f.read()
        return snapshot_data

    async def get_raw_snapshot(self):
        return await asyncio.to_thread(self._get_raw_snapshot)

    #TODO: Check the snapshot (all the packets?)
    def _install_snapshot_finished(self, snapshot_index, snapshot_term):
        entry = self.get_entry(snapshot_index)
        if entry and entry["term"] == snapshot_term:
            aux = self.get_entry_range(snapshot_index+1)
        else:
            aux = []
        if os.path.isfile(self.snapshot_file):
            os.replace(self.snapshot_file, self.snapshot_file+".backup")
        os.replace(self.snapshot_file+".recv", self.snapshot_file)
        self._rewrite_log(aux)
        self.commit_index = 0

    async def install_snapshot_finished(self, snapshot_index, snapshot_term):
        await asyncio.to_thread(self._install_snapshot_finished, snapshot_index,
            snapshot_term)
        await self.load()
