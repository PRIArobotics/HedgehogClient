from hedgehog.protocol.messages import motor, process


class AsyncRegistry:
    def __init__(self):
        self._register_cbs = None
        self.motor_cbs = {}
        self.process_cbs = {}

    @property
    def register_cbs(self):
        value = self._register_cbs
        assert value is not None, "register_cbs is not set"
        self._register_cbs = None
        return value

    @register_cbs.setter
    def register_cbs(self, value):
        assert self._register_cbs is None, "register_cbs is already set"
        self._register_cbs = value

    def handle_register(self, reps):
        register_cbs = self.register_cbs
        assert len(register_cbs) == len(reps)
        for register, rep in zip(register_cbs, reps):
            if register is not None:
                register(rep)

    def handle_async(self, backend, msg):
        if type(msg) is motor.StateUpdate:
            reached_cb, = self.process_cbs[msg.pid]
            if reached_cb is not None:
                backend.spawn(reached_cb, msg.port, msg.state)
        if type(msg) is process.StreamUpdate:
            stream_cb, _ = self.process_cbs[msg.pid]
            if stream_cb is not None:
                backend.spawn(stream_cb, msg.pid, msg.fileno, msg.chunk)
        if type(msg) is process.ExitUpdate:
            _, exit_cb = self.process_cbs[msg.pid]
            if exit_cb is not None:
                backend.spawn(exit_cb, msg.pid, msg.exit_code)
