from queue import Queue
from hedgehog.protocol.messages import Msg, ack, motor, process
from hedgehog.utils import coroutine
from hedgehog.utils.zmq.actor import CommandRegistry
from hedgehog.utils.zmq.pipe import pipe


_update_keys = {
    motor.StateUpdate: lambda update: update.port,
    process.StreamUpdate: lambda update: update.pid,
    process.ExitUpdate: lambda update: update.pid,
}


def _update_key(update):
    cls = type(update)
    return cls, _update_keys[cls](update)


class _EventHandler(object):
    def __init__(self, backend, handler):
        self.pipe, self._pipe = pipe(backend.ctx)
        self.handler = handler

    def run(self, client):
        registry = CommandRegistry()
        running = True

        @registry.command(b'UPDATE')
        def handle_update(update_raw):
            update = Msg.parse(update_raw)
            self.handler(client, update)

        @registry.command(b'$TERM')
        def handle_term():
            nonlocal running
            running = False

        while running:
            registry.handle(self._pipe.recv_multipart())

    def update(self, update):
        self.pipe.send_multipart([b'UPDATE', Msg.serialize(update)])

    def shutdown(self):
        self.pipe.send(b'$TERM')


class EventHandler(object):
    events = None
    _is_shutdown = False

    def initialize(self, backend, reply):
        raise NotImplementedError()

    def update(self, update):
        raise NotImplementedError()

    def shutdown(self):
        if not self._is_shutdown:
            self._is_shutdown = True
            self._shutdown()

    def _shutdown(self):
        raise NotImplementedError()


class MotorUpdateHandler(EventHandler):
    port = None
    handler = None

    def __init__(self, on_reached):
        self.on_reached = on_reached

    def initialize(self, backend, reply):
        self.port = reply.port
        self.events = {(motor.StateUpdate, self.port)}

        @coroutine
        def handle_motor_state_update():
            client, update = yield
            self.on_reached(client, self.port, update.state)
            self.handler.shutdown()
            yield

        self.handler = _EventHandler(backend, handle_motor_state_update())
        backend.spawn(self.handler.run)

    def update(self, update):
        self.handler.update(update)

    def _shutdown(self):
        self.handler.shutdown()


class ProcessUpdateHandler(EventHandler):
    pid = None
    stdout_handler = None
    stderr_handler = None

    def __init__(self, on_stdout, on_stderr, on_exit):
        self.on_stdout = on_stdout
        self.on_stderr = on_stderr
        self.on_exit = on_exit

    def initialize(self, backend, reply):
        self.pid = reply.pid
        self.events = {(process.StreamUpdate, self.pid),
                       (process.ExitUpdate, self.pid)}

        exit_a, exit_b = pipe(backend.ctx)

        @coroutine
        def handle_stdout_exit():
            while True:
                client, update = yield
                if self.on_stdout is not None:
                    self.on_stdout(client, self.pid, update.fileno, update.chunk)
                if update.chunk == b'':
                    break

            client, update = yield

            exit_a.wait()
            if self.on_exit is not None:
                self.on_exit(client, self.pid, update.exit_code)

            self.stdout_handler.shutdown()
            yield

        @coroutine
        def handle_stderr():
            while True:
                client, update = yield
                if self.on_stderr is not None:
                    self.on_stderr(client, self.pid, update.fileno, update.chunk)
                if update.chunk == b'':
                    break

            exit_b.signal()
            self.stderr_handler.shutdown()
            yield

        self.stdout_handler = _EventHandler(backend, handle_stdout_exit())
        self.stderr_handler = _EventHandler(backend, handle_stderr())
        backend.spawn(self.stdout_handler.run)
        backend.spawn(self.stderr_handler.run)

    def update(self, update):
        if isinstance(update, process.StreamUpdate):
            if update.fileno == process.STDOUT:
                self.stdout_handler.update(update)
            else:
                self.stderr_handler.update(update)
        elif isinstance(update, process.ExitUpdate):
            self.stdout_handler.update(update)
        else:
            assert False, update

    def _shutdown(self):
        self.stdout_handler.shutdown()
        self.stderr_handler.shutdown()


class ClientHandle(object):
    def __init__(self):
        self.queue = Queue()
        self.daemon = False

    def push(self, obj):
        self.queue.put(obj)

    def pop(self):
        # don't block, as we expect access synchronized via zmq sockets
        return self.queue.get(block=False)


class ClientRegistry(object):
    def __init__(self):
        self.clients = {}
        self._handler_queues = {}
        self._handlers = {}

    def connect(self, key):
        client_handle = ClientHandle()
        self.clients[key] = client_handle
        self._handler_queues[key] = []
        return client_handle

    def disconnect(self, key):
        del self.clients[key]

    def prepare_register(self, key):
        client_handle = self.clients[key]
        self._handler_queues[key].append(client_handle.pop())
        pass

    def handle_register(self, key, backend, replies):
        handlers = self._handler_queues[key].pop(0)
        assert len(handlers) == len(replies)
        for handler, reply in zip(handlers, replies):
            if handler is None:
                continue
            if type(reply) == ack.Acknowledgement and reply.code != ack.OK:
                continue

            handler.initialize(backend, reply)
            for event in handler.events:
                if event in self._handlers:
                    self._handlers[event].shutdown()
                self._handlers[event] = handler

    def handle_async(self, update):
        event = _update_key(update)
        if event in self._handlers:
            self._handlers[event].update(update)

    def shutdown(self):
        for handler in self._handlers.values():
            handler.shutdown()