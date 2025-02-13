try:
    import ujson as json
except ImportError:
    import json

import msgpack


class JsonSerializer:
    @staticmethod
    def pack(obj):
        return json.dumps(obj).encode()

    @staticmethod
    def unpack(obj):
        try:
            return json.loads(obj.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None


class MsgPackSerializer:
    @staticmethod
    def pack(obj):
        return msgpack.dumps(obj)

    @staticmethod
    def unpack(obj):
        try:
            return msgpack.loads(obj)
        except UnicodeDecodeError:
            return None
