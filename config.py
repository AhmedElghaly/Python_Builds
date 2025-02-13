import os
import logging

from ttraft.serializer import JsonSerializer, MsgPackSerializer


class Config:
    """ Raft and failover configuration options.

    :param persistent_dir: Directory used to store persistent data. If
        it doesn't exist, it will be created.
    :type peristent_dir: str
    :param max_log_size: Maximun log size before log compaction.
    :type max_log_size: int
    :param follower_timeout: Time without receiving a heartbeat before
        becoming a candidate. It must be a tuple with the timeout range.
    :type follower_timeout: tuple(int, int)
    :param heartbeat_period: Heartbeat period.
    :type heartbeat_period: int
    :param instant_send: Send an append entry whenever a new entry is
        committed, instead of waiting until next heartbeat period.
    :type instant_send: bool
    :param serializer: Serializer for persistent data. Options are json
        or msgpack (default).
    :type serializer: str
    :param logger: Base logger to be used by the library.
    :type logger: "logging.Logger"
    :param on_follower_cb: Callback execute when state becomes follower.
    :type on_follower_cb: function or coroutine
    :param on_candidate_cb: Callback execute when state becomes candidate.
    :type on_candidate_cb: function or coroutine
    :param on_leader_cb: Callback execute when state becomes leader.
    :type on_leader_cb: function or coroutine
    :param on_leader_change_cb: Callback execute when leader_id changes.
    :type on_leader_change_cb: function or coroutine
    """
    def __init__(self, *,
            persistent_dir=os.path.expanduser("~/.ttraft"),
            max_log_size=16384,
            follower_timeout=(10, 15),
            heartbeat_period=4.5,
            instant_send=False,
            serializer="msgpack",
            logger=None,
            on_follower_cb=None,
            on_candidate_cb=None,
            on_leader_cb=None,
            on_leader_change_cb=None):

        self.persistent_dir = persistent_dir
        self.max_log_size = max_log_size
        self.follower_timeout = follower_timeout
        self.heartbeat_period = heartbeat_period
        self.instant_send = instant_send
        if serializer == "json":
            self.serializer = JsonSerializer
        else:
            self.serializer = MsgPackSerializer
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger("ttraft")
        self.on_follower_cb = on_follower_cb
        self.on_candidate_cb = on_candidate_cb
        self.on_leader_cb = on_leader_cb
        self.on_leader_change_cb = on_leader_change_cb
