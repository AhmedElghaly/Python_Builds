from enum import IntEnum, auto

from ttraft.serializer import MsgPackSerializer


class MessageType(IntEnum):
    REQUEST_VOTE = auto()
    REQUEST_VOTE_REPLY = auto()
    APPEND_ENTRIES = auto()
    APPEND_ENTRIES_REPLY = auto()
    INSTALL_SNAPSHOT = auto()
    INSTALL_SNAPSHOT_REPLY = auto()
    FAILOVER_ENTRIES = auto()
    FAILOVER_ENTRIES_REPLY = auto()


class Message:
    def __init__(self, msg_type):
        self._type = msg_type

    @property
    def type(self):
        return self._type

    def encode(self):
        raise NotImplementedError

    @classmethod
    def decode(cls, data):
        l = MsgPackSerializer.unpack(data)
        if l[0] == MessageType.REQUEST_VOTE:
            msg = RequestVote(*l[1:])
        elif l[0] == MessageType.REQUEST_VOTE_REPLY:
            msg = RequestVoteReply(*l[1:])
        elif l[0] == MessageType.APPEND_ENTRIES:
            msg = AppendEntries(*l[1:])
        elif l[0] == MessageType.APPEND_ENTRIES_REPLY:
            msg = AppendEntriesReply(*l[1:])
        elif l[0] == MessageType.INSTALL_SNAPSHOT:
            msg = InstallSnapshot(*l[1:])
        elif l[0] == MessageType.INSTALL_SNAPSHOT_REPLY:
            msg = InstallSnapshotReply(*l[1:])
        elif l[0] == MessageType.FAILOVER_ENTRIES:
            msg = FailoverEntries(*l[1:])
        elif l[0] == MessageType.FAILOVER_ENTRIES_REPLY:
            msg = FailoverEntriesReply(*l[1:])
        else:
            msg = None
        return msg


class RequestVote(Message):
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        super().__init__(MessageType.REQUEST_VOTE)
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term, self.candidate_id,
                self.last_log_index, self.last_log_term])


class RequestVoteReply(Message):
    def __init__(self, term, vote_granted):
        super().__init__(MessageType.REQUEST_VOTE_REPLY)
        self.term = term
        self.vote_granted = vote_granted

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term, self.vote_granted])


class AppendEntries(Message):
    def __init__(self, term, leader_id, prev_log_index, prev_log_term, entries,
            leader_commit):
        super().__init__(MessageType.APPEND_ENTRIES)
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit = leader_commit

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term, self.leader_id,
            self.prev_log_index, self.prev_log_term, self.entries,
            self.leader_commit])


class AppendEntriesReply(Message):
    def __init__(self, term, success, last_log_index=0):
        super().__init__(MessageType.APPEND_ENTRIES_REPLY)
        self.term = term
        self.success = success
        self.last_log_index = last_log_index

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term, self.success,
            self.last_log_index])


class InstallSnapshot(Message):
    def __init__(self, term, leader_id, last_included_index,
            last_included_term, offset, data, done):
        super().__init__(MessageType.INSTALL_SNAPSHOT)
        self.term = term
        self.leader_id = leader_id
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.offset = offset
        self.data = data
        self.done = done

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term, self.leader_id,
            self.last_included_index, self.last_included_term, self.offset,
            self.data, self.done])


class InstallSnapshotReply(Message):
    def __init__(self, term):
        super().__init__(MessageType.INSTALL_SNAPSHOT_REPLY)
        self.term = term

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term])


class FailoverEntries:
    def __init__(self, term, prev_log_index, prev_log_term, entries):
        self.type = MessageType.FAILOVER_ENTRIES
        self.term = term
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term,
            self.prev_log_index, self.prev_log_term, self.entries])


class FailoverEntriesReply:
    def __init__(self, term, success, last_log_index):
        self.type = MessageType.FAILOVER_ENTRIES_REPLY
        self.term = term
        self.success = success
        self.last_log_index = last_log_index

    def encode(self):
        return MsgPackSerializer.pack([self.type, self.term, self.success,
            self.last_log_index])
