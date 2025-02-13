import asyncio
import random

from ttraft.utils import execute_async


class Timer:
    def __init__(self, logger, timeout, callback, *args, **kwargs):
        self.logger = logger.getChild(__name__)
        self.timeout = timeout
        self.callback = callback
        self.args = args
        self.kwargs = kwargs
        self.task = None

    def get_timeout(self):
        return self.timeout

    async def _timer(self):
        try:
            await asyncio.sleep(self.get_timeout())
            execute_async(self.callback, *self.args, **self.kwargs)
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass
        except:
            self.logger.exception("Timer exception")
            raise

    def start(self):
        self.task = asyncio.create_task(self._timer())

    def stop(self):
        if self.task is not None:
            self.task.cancel()

    def restart(self):
        self.stop()
        self.start()


class RandomTimer(Timer):
    def __init__(self, logger, t_low, t_high, callback, *args, **kwargs):
        self.t_low = t_low
        self.t_high = t_high
        super().__init__(logger, 0, callback, *args, **kwargs)

    def get_timeout(self):
        return random.randint(self.t_low, self.t_high)
