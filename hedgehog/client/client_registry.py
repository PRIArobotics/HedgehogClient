from typing import cast, Any, Callable, Dict, Generator, List, Sequence, Set, Tuple, Type, Union

import zmq
from contextlib import contextmanager
from queue import Queue

from hedgehog.protocol import errors
from hedgehog.protocol.messages import ReplyMsg, Message, ack, motor, process
from hedgehog.protocol.sockets import ReqSocket
from hedgehog.utils import coroutine
from hedgehog.utils.zmq.actor import CommandRegistry
from hedgehog.utils.zmq.pipe import pipe

from typing import TYPE_CHECKING
if TYPE_CHECKING:  # pragma: nocover
    from . import client_backend

_update_keys = {
    # motor.StateUpdate: lambda update: cast(motor.StateUpdate, update).port,
    process.StreamUpdate: lambda update: cast(process.StreamUpdate, update).pid,
    process.ExitUpdate: lambda update: cast(process.ExitUpdate, update).pid,
}  # type: Dict[Type[Message], Callable[[Message], Any]]


def _update_key(update: Message) -> Tuple[Type[Message], Any]:
    cls = type(update)
    return cls, _update_keys[cls](update)


class _EventHandler(object):
    def __init__(self, backend, handler: Callable[[Message], None]) -> None:
        self.pipe, self._pipe = pipe(backend.ctx)
        self.handler = handler

    def run(self) -> None:
        registry = CommandRegistry()
        running = True

        @registry.command(b'UPDATE')
        def handle_update(update_raw) -> None:
            update = ReplyMsg.parse(update_raw)  # type: Message
            self.handler(update)

        @registry.command(b'$TERM')
        def handle_term() -> None:
            nonlocal running
            running = False

        while running:
            registry.handle(self._pipe.recv_multipart())
        self._pipe.close()

    def update(self, update: Message) -> None:
        self.pipe.send_multipart([b'UPDATE', ReplyMsg.serialize(update)])

    def shutdown(self) -> None:
        if not self.pipe.closed:
            self.pipe.send(b'$TERM')
            self.pipe.close()


EventHandler = Generator[Set[Tuple[Type[Message], Any]],
                         Union[Tuple['client_backend.ClientBackend', Message], Message, None],
                         None]


# class MotorUpdateHandler(EventHandler):
#     port = None  # type: int
#     handler = None  # type: _EventHandler
#
#     def __init__(self, on_reached: Callable[[int, int], None]) -> None:
#         self.on_reached = on_reached
#
#     def initialize(self, backend, reply) -> None:
#         self.port = reply.port
#         self.events = {(motor.StateUpdate, self.port)}
#
#         @coroutine
#         def handle_motor_state_update():
#             update, = yield
#             self.on_reached(self.port, update.state)
#             self.handler.shutdown()
#             yield
#
#         self.handler = _EventHandler(backend, handle_motor_state_update())
#         backend.spawn(self.handler.run)
#
#     def update(self, update):
#         self.handler.update(update)
#
#     def _shutdown(self):
#         self.handler.shutdown()


def process_handler(on_stdout, on_stderr, on_exit):
    # initialize
    backend, reply = yield

    pid = reply.pid
    events = {(process.StreamUpdate, pid),
              (process.ExitUpdate, pid)}

    exit_a, exit_b = pipe(backend.ctx)

    @coroutine
    def handle_stdout_exit():
        while True:
            update, = yield
            if on_stdout is not None:
                on_stdout(pid, update.fileno, update.chunk)
            if update.chunk == b'':
                break

        update, = yield

        exit_a.wait()
        exit_a.close()
        if on_exit is not None:
            on_exit(pid, update.exit_code)

        stdout_handler.shutdown()
        yield

    @coroutine
    def handle_stderr():
        while True:
            update, = yield
            if on_stderr is not None:
                on_stderr(pid, update.fileno, update.chunk)
            if update.chunk == b'':
                break

        exit_b.signal()
        exit_b.close()
        stderr_handler.shutdown()
        yield

    stdout_handler = _EventHandler(backend, handle_stdout_exit())
    stderr_handler = _EventHandler(backend, handle_stderr())
    backend.spawn(stdout_handler.run, async=True)
    backend.spawn(stderr_handler.run, async=True)

    try:
        # update
        update = yield events
        while True:
            if isinstance(update, process.StreamUpdate):
                if update.fileno == process.STDOUT:
                    stdout_handler.update(update)
                else:
                    stderr_handler.update(update)
            elif isinstance(update, process.ExitUpdate):
                stdout_handler.update(update)
            else:  # pragma: nocover
                assert False, update

            update = yield
    finally:
        # shutdown
        stdout_handler.shutdown()
        stderr_handler.shutdown()


_IDLE = 0
_CRITICAL = 1
_SHUTDOWN_SCHEDULED = 2


class ClientHandle(object):
    def __init__(self) -> None:
        self.queue = Queue()  # type: Queue
        self.socket = None  # type: ReqSocket
        self.daemon = False
        self._state = _IDLE

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @contextmanager
    def _critical_section(self):
        assert self._state == _IDLE
        try:
            self._state = _CRITICAL
            yield
        finally:
            if self._state == _SHUTDOWN_SCHEDULED:
                self._state = _IDLE
                self._shutdown_now()
                raise errors.EmergencyShutdown("Emergency Shutdown activated")
            else:
                self._state = _IDLE

    def close(self) -> None:
        with self._critical_section():
            if not self.socket.closed:
                self.socket.send_msg_raw(b'DISCONNECT')
                self.socket.wait()
                self.socket.close()

    def push(self, obj: Any) -> None:
        self.queue.put(obj)

    def pop(self) -> Any:
        # don't block, as we expect access synchronized via zmq sockets
        return self.queue.get(block=False)

    def send_commands(self, *cmds: Tuple[Message, EventHandler]) -> Sequence[Message]:
        with self._critical_section():
            self.push([handler for _, handler in cmds])
            self.socket.send(b'COMMAND', zmq.SNDMORE)
            self.socket.send_msgs([msg for msg, _ in cmds])
            return self.socket.recv_msgs()

    def shutdown(self) -> bool:
        """
        Shuts down the backend this client handle is connected to.
        A shutdown may occur normally in any thread, or in an interrupt handler on the main thread. If this is invoked
        on an interrupt handler during socket communication, the shutdown is deferred until after the socket operation.
        In that case, `EmergencyShutdown` is raised on the main thread after deferred shutdown. This method returns True
        for an immediate shutdown, False for a deferred one.

        :return: Whether shutdown was performed immediately
        """
        if self._state == _IDLE:
            self._shutdown_now()
            return True
        else:
            self._state = _SHUTDOWN_SCHEDULED
            return False

    def _shutdown_now(self) -> None:
        self.socket.send_msg_raw(b'SHUTDOWN')
        self.socket.wait()


class ClientRegistry(object):
    def __init__(self) -> None:
        self.clients = {}  # type: Dict[bytes, ClientHandle]
        self._handler_queues = {}  # type: Dict[bytes, List[Sequence[EventHandler]]]
        self._handlers = {}  # type: Dict[Tuple[Type[Message], Any], EventHandler]

    def connect(self, key: bytes) -> ClientHandle:
        client_handle = ClientHandle()
        self.clients[key] = client_handle
        self._handler_queues[key] = []
        return client_handle

    def disconnect(self, key: bytes) -> None:
        del self.clients[key]

    def prepare_register(self, key: bytes) -> None:
        client_handle = self.clients[key]
        self._handler_queues[key].append(client_handle.pop())

    def handle_register(self, key: bytes, backend: 'client_backend.ClientBackend', replies: Sequence[Message]) -> None:
        handlers = self._handler_queues[key].pop(0)
        assert len(handlers) == len(replies)
        for handler, reply in zip(handlers, replies):  # type: Tuple[EventHandler, Message]
            if handler is None:
                continue
            if isinstance(reply, ack.Acknowledgement) and reply.code != ack.OK:
                handler.close()
                continue

            next(handler)
            events = handler.send((backend, reply))
            for event in events:
                if event in self._handlers:
                    self._handlers[event].close()
                self._handlers[event] = handler

    def handle_async(self, update: Message) -> None:
        event = _update_key(update)
        if event in self._handlers:
            self._handlers[event].send(update)

    def shutdown(self) -> None:
        for handler in self._handlers.values():
            handler.close()
