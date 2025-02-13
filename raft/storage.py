import os
import asyncio


class Storage:
    def __init__(self, persistent_dir):
        self._current_term_file = os.path.join(persistent_dir, "current_term")
        self._voted_for_file = os.path.join(persistent_dir, "voted_for")
        self._current_term = 0
        self._voted_for = None

    def _load(self):
        try:
            with open(self._current_term_file) as f:
                self._current_term = int(f.read())
            with open(self._voted_for_file) as f:
                value = f.read()
                if value != "":
                    self._voted_for = int(value)
                else:
                    self._voted_for = None
        except (FileNotFoundError, ValueError):
            self._current_term = 0
            self._voted_for = None

    async def load(self):
        await asyncio.to_thread(self._load)

    @property
    def current_term(self):
        return self._current_term

    @current_term.setter
    def current_term(self, value):
        asyncio.create_task(asyncio.to_thread(self._write_current_term, value))
        self._current_term = value

    def _write_current_term(self, value):
        with open(self._current_term_file, "w") as f:
            f.write(str(value))

    @property
    def voted_for(self):
        return self._voted_for

    @voted_for.setter
    def voted_for(self, value):
        asyncio.create_task(asyncio.to_thread(self._write_voted_for, value))
        self._voted_for = value

    def _write_voted_for(self, value):
        with open(self._voted_for_file, "w") as f:
            f.write(str(value) if value is not None else "")
