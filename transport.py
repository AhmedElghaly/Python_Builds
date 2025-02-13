class TransportInterface:
    """ Transport layer interface. """

    async def recv(self):
        raise NotImplementedError

    async def send(self, data, dest):
        raise NotImplementedError

    async def start(self):
        raise NotImplementedError

    async def stop(self):
        raise NotImplementedError
