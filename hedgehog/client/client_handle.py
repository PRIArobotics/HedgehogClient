import threading
import zmq
from hedgehog.utils.zmq.pipe import pipe
from hedgehog.protocol import messages
from hedgehog.protocol.messages import ack, motor, process


class AsyncUpdateHandler:
    @property
    def updates(self):
        raise NotImplementedError

    def __init__(self):
        self.rep = None

    @classmethod
    def update_key(cls, update):
        raise NotImplementedError

    @property
    def key(self):
        raise NotImplementedError

    def register(self, backend):
        pass

    def handle_update(self, backend, update):
        raise NotImplementedError


class MotorUpdateHandler(AsyncUpdateHandler):
    updates = (motor.StateUpdate,)

    def __init__(self, port, on_reached):
        super().__init__()
        self.port = port
        self.on_reached = on_reached

    @classmethod
    def update_key(cls, update):
        return update.port

    @property
    def key(self):
        port, _ = self.args
        return self.port

    def handle_update(self, backend, update):
        backend.spawn(self.on_reached, self.port, update.state)


class ProcessUpdateHandler(AsyncUpdateHandler):
    updates = (process.StreamUpdate, process.ExitUpdate)

    def __init__(self, on_stdout, on_stderr, on_exit):
        super().__init__()
        self.on_stdout = on_stdout
        self.on_stderr = on_stderr
        self.on_exit = on_exit
        self.update_handler = None

    @classmethod
    def update_key(cls, update):
        return update.pid

    @property
    def key(self):
        return self.rep.pid

    def register(self, backend):
        if self.on_stdout is None and self.on_stderr is None:
            # no streams, just call exit when it comes up
            def update_handler(backend, update):
                if type(update) is process.ExitUpdate and self.on_exit is not None:
                    backend.spawn(self.on_exit, update.pid, update.exit_code)
            self.update_handler = update_handler
        elif self.on_stdout is not None and self.on_stderr is not None:
            # both streams; the complicated case
            ctx = zmq.Context()
            stdout_a, stdout_b = pipe(ctx)
            stderr_a, stderr_b = pipe(ctx)
            stderr_eof_a, stderr_eof_b = pipe(ctx)

            def stdout_handler(client):
                while True:
                    update = messages.parse(stdout_b.recv())
                    self.on_stdout(client, update.pid, update.fileno, update.chunk)
                    if update.chunk == b'':
                        break

                stderr_eof_b.recv()
                stderr_eof_b.close()

                update = messages.parse(stdout_b.recv())
                stdout_b.close()

                if self.on_exit is not None:
                    self.on_exit(client, update.pid, update.exit_code)

            def stderr_handler(client):
                while True:
                    update = messages.parse(stderr_b.recv())
                    self.on_stderr(client, update.pid, update.fileno, update.chunk)
                    if update.chunk == b'':
                        break
                stderr_b.close()

                stderr_eof_a.send(b'')
                stderr_eof_a.close()

            backend.spawn(stdout_handler)
            backend.spawn(stderr_handler)

            def update_handler(backend, update):
                if type(update) is process.StreamUpdate:
                    if update.fileno == process.STDOUT:
                        stdout_a.send(messages.serialize(update))
                    elif update.fileno == process.STDERR:
                        stderr_a.send(messages.serialize(update))
                        if update.chunk == b'':
                            stderr_a.close()
                elif type(update) is process.ExitUpdate:
                    stdout_a.send(messages.serialize(update))
                    stdout_a.close()

            self.update_handler = update_handler
        else:
            # one stream
            ctx = zmq.Context()
            stream_a, stream_b = pipe(ctx)

            if self.on_stdout is not None:
                fileno, handler = process.STDOUT, self.on_stdout
            else:
                fileno, handler = process.STDERR, self.on_stderr

            def stream_handler(client):
                while True:
                    update = messages.parse(stream_b.recv())
                    handler(client, update.pid, update.fileno, update.chunk)
                    if update.chunk == b'':
                        break

                update = messages.parse(stream_b.recv())
                stream_b.close()

                if self.on_exit is not None:
                    self.on_exit(client, update.pid, update.exit_code)

            backend.spawn(stream_handler)

            def update_handler(backend, update):
                if type(update) is process.StreamUpdate and update.fileno == fileno:
                    stream_a.send(messages.serialize(update))
                elif type(update) is process.ExitUpdate:
                    stream_a.send(messages.serialize(update))
                    stream_a.close()

            self.update_handler = update_handler

    def handle_update(self, backend, update):
        self.update_handler(backend, update)


handler_types = (MotorUpdateHandler, ProcessUpdateHandler)
handler_map = {
    update: handler
    for handler in handler_types
    for update in handler.updates
}


class ClientHandle:
    def __init__(self):
        self._new_handlers = None
        self.handlers = {}
        for handler in handler_types:
            self.handlers[handler] = {}

    @property
    def new_handlers(self):
        value = self._new_handlers
        assert value is not None, "register_cbs is not set"
        self._new_handlers = None
        return value

    @new_handlers.setter
    def new_handlers(self, value):
        assert self._new_handlers is None, "register_cbs is already set"
        self._new_handlers = value

    def handle_register(self, backend, reps):
        new_handlers = self.new_handlers
        assert len(new_handlers) == len(reps)
        for handler, rep in zip(new_handlers, reps):
            if handler is None:
                continue
            if type(rep) == ack.Acknowledgement and rep.code != ack.OK:
                continue

            handler.rep = rep
            kind, key = type(handler), handler.key
            self.handlers[kind][key] = handler
            handler.register(backend)

    def handle_async(self, backend, msg):
        kind = handler_map[type(msg)]
        key = kind.update_key(msg)

        handler = self.handlers[kind][key]
        if handler is not None:
            handler.handle_update(backend, msg)
