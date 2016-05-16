from hedgehog.protocol.messages import ack, motor, process


class AsyncUpdateHandler:
    @property
    def updates(self):
        raise NotImplementedError

    def __init__(self, *args):
        self.rep = None
        self.args = args

    @classmethod
    def update_key(cls, update):
        raise NotImplementedError

    @property
    def key(self):
        raise NotImplementedError

    def register(self):
        pass

    def handle_update(self, backend, update):
        raise NotImplementedError


class MotorUpdateHandler(AsyncUpdateHandler):
    updates = (motor.StateUpdate,)

    @classmethod
    def update_key(cls, update):
        return update.port

    @property
    def key(self):
        port, _ = self.args
        return port

    def handle_update(self, backend, update):
        port, on_reached = self.args
        backend.spawn(on_reached, port, update.state)


class ProcessUpdateHandler(AsyncUpdateHandler):
    updates = (process.StreamUpdate, process.ExitUpdate)

    @classmethod
    def update_key(cls, update):
        return update.pid

    @property
    def key(self):
        return self.rep.pid

    def handle_update(self, backend, update):
        on_stream, on_exit = self.args
        if type(update) is process.StreamUpdate and on_stream is not None:
            backend.spawn(on_stream, update.pid, update.fileno, update.chunk)
        if type(update) is process.ExitUpdate and on_exit is not None:
            backend.spawn(on_exit, update.pid, update.exit_code)


handler_types = (MotorUpdateHandler, ProcessUpdateHandler)
handler_map = {
    update: handler
    for handler in handler_types
    for update in handler.updates
}

class AsyncRegistry:
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

    def handle_register(self, reps):
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
            handler.register()

    def handle_async(self, backend, msg):
        kind = handler_map[type(msg)]
        key = kind.update_key(msg)

        handler = self.handlers[kind][key]
        if handler is not None:
            handler.handle_update(backend, msg)
