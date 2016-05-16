from hedgehog.protocol import messages
from hedgehog.protocol.messages import motor, process


class AsyncRegistry:
    def __init__(self):
        self.register_cbs = None
        self.motor_cbs = {}
        self.process_cbs = {}

    def handle_register(self, reps):
        assert len(self.register_cbs) == len(reps)
        for register, rep in zip(self.register_cbs, reps):
            if register is not None:
                register(rep)
        self.register_cbs = None

    def handle_async(self, backend, msg):
        if type(msg) is messages.motor.StateUpdate:
            reached_cb, = self.process_cbs[msg.pid]
            if reached_cb is not None:
                backend.spawn(reached_cb, msg.port, msg.state)
        if type(msg) is messages.process.StreamUpdate:
            stream_cb, _ = self.process_cbs[msg.pid]
            if stream_cb is not None:
                backend.spawn(stream_cb, msg.pid, msg.fileno, msg.chunk)
        if type(msg) is messages.process.ExitUpdate:
            _, exit_cb = self.process_cbs[msg.pid]
            if exit_cb is not None:
                backend.spawn(exit_cb, msg.pid, msg.exit_code)
