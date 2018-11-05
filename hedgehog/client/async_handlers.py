from typing import cast, Any, Callable, Deque, Dict, Generator, List, Sequence, Set, Tuple, Type, Union

import asyncio
from collections import deque

from hedgehog.protocol.messages import Message, ack, analog, digital, io, motor, process, servo


UpdateHandler = Generator[None, Message, None]
UpdateKey = Tuple[Type[Message], Any]
AsyncHandler = Callable[[Message], Tuple[Set[UpdateKey], UpdateHandler]]


class ProcessHandler:
    def __init__(self, on_stdout, on_stderr, on_exit, sequential=True):
        self._on_stdout = on_stdout
        self._on_stderr = on_stderr
        self._on_exit = on_exit
        self._sequential = sequential
        pass

    async def handle_update(self, update: Union[process.StreamUpdate, process.ExitUpdate]) -> None:
        if isinstance(update, process.StreamUpdate):
            if update.fileno == process.STDOUT:
                if self._on_stdout is not None:
                    await self._on_stdout(self.pid, update.fileno, update.chunk)
            else:
                if self._on_stderr is not None:
                    await self._on_stderr(self.pid, update.fileno, update.chunk)
        elif isinstance(update, process.ExitUpdate):
            if self._on_exit is not None:
                await self._on_exit(self.pid, update.exit_code)
        else:  # pragma: nocover
            assert False, update

    def _handle_updates_sequential(self, tasks: List[asyncio.Task]):
        queue = asyncio.Queue()

        async def run_updates() -> None:
            while True:
                update = await queue.get()
                await self.handle_update(update)
                if isinstance(update, process.ExitUpdate):
                    break

        tasks.append(asyncio.create_task(run_updates()))

        while True:
            update = yield
            queue.put_nowait(update)
            if isinstance(update, process.ExitUpdate):
                break

    def _handle_updates_concurrent(self, tasks: List[asyncio.Task]):
        while True:
            update = yield
            tasks.append(asyncio.create_task(self.handle_update(update)))
            if isinstance(update, process.ExitUpdate):
                break

    def _handle_updates(self):
        tasks: List[asyncio.Task] = []

        try:
            if self._sequential:
                yield from self._handle_updates_sequential(tasks)
            else:
                yield from self._handle_updates_concurrent(tasks)

            # here we expect shutdown
            try:
                yield
            except StopIteration:
                raise
            else:
                raise RuntimeError("generator didn't stop")
        finally:
            for task in tasks:
                task.cancel()

    def __call__(self, reply: process.ExecuteReply) -> Tuple[Set[UpdateKey], UpdateHandler]:
        self.pid = reply.pid
        update_keys = {(process.StreamUpdate, self.pid), (process.ExitUpdate, self.pid)}
        update_handler: UpdateHandler = self._handle_updates()
        next(update_handler)
        return update_keys, update_handler


class HandlerRegistry(object):
    _update_keys = {
        io.CommandUpdate: lambda update: cast(io.CommandUpdate, update).port,
        analog.Update: lambda update: cast(analog.Update, update).port,
        digital.Update: lambda update: cast(digital.Update, update).port,
        motor.CommandUpdate: lambda update: cast(motor.CommandUpdate, update).port,
        motor.StateUpdate: lambda update: cast(motor.StateUpdate, update).port,
        servo.CommandUpdate: lambda update: cast(servo.CommandUpdate, update).port,
        process.StreamUpdate: lambda update: cast(process.StreamUpdate, update).pid,
        process.ExitUpdate: lambda update: cast(process.ExitUpdate, update).pid,
    }  # type: Dict[Type[Message], Callable[[Message], Any]]

    @staticmethod
    def _update_key(update: Message) -> UpdateKey:
        cls = type(update)
        return cls, HandlerRegistry._update_keys[cls](update)

    def __init__(self) -> None:
        self._queue: Deque[Sequence[AsyncHandler]] = deque()
        self._handlers = {}  # type: Dict[UpdateKey, UpdateHandler]

    def prepare_register(self, handlers: Sequence[AsyncHandler]):
        self._queue.append(handlers)

    def complete_register(self, replies: Sequence[Message]):
        handlers = self._queue.popleft()
        assert len(handlers) == len(replies)
        for handler, reply in zip(handlers, replies):
            if handler is None:
                continue
            if isinstance(reply, ack.Acknowledgement) and reply.code != ack.OK:
                # we simply don't call the EventHandler and everything is fine
                continue

            update_keys, update_handler = handler(reply)
            for update_key in update_keys:
                if update_key in self._handlers:
                    self._handlers[update_key].close()
                self._handlers[update_key] = update_handler

    def handle_updates(self, updates: Sequence[Message]) -> None:
        for update in updates:
            update_key = HandlerRegistry._update_key(update)
            if update_key in self._handlers:
                self._handlers[update_key].send(update)

    def shutdown(self) -> None:
        for handler in self._handlers.values():
            handler.close()
        self._handlers.clear()
