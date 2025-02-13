class StateMachine:
    def apply_command(self, command):
        """ Apply command to the state machine.
        Commands are generic (no type restrictions), their meaning
        implemented by the state machine.
        """
        raise NotImplementedError

    def apply_snapshot(self, snapshot):
        """ Applies a snapshot. This overwrites the current state
        machine.
        """
        raise NotImplementedError

    def get_snapshot(self):
        """ Returns a snapshot of the state machine. """
        raise NotImplementedError
