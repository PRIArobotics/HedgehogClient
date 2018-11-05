from typing import cast, Any, Callable, Deque, Dict, Generator, List, Sequence, Set, Tuple, Type, Union

import asyncio
from collections import deque

from hedgehog.protocol.messages import Message, ack, motor, process


UpdateHandler = Generator[None, Message, None]
UpdateKey = Tuple[Type[Message], Any]
AsyncHandler = Callable[[Message], Tuple[Set[UpdateKey], UpdateHandler]]


def process_handler(on_stdout, on_stderr, on_exit, reply: process.ExecuteReply, *, sequential=True)\
        -> Tuple[Set[UpdateKey], UpdateHandler]:
    pid = reply.pid

    async def handle_update(update: Union[process.StreamUpdate, process.ExitUpdate]) -> None:
        if isinstance(update, process.StreamUpdate):
            if update.fileno == process.STDOUT:
                if on_stdout is not None:
                    await on_stdout(pid, update.fileno, update.chunk)
            else:
                if on_stderr is not None:
                    await on_stderr(pid, update.fileno, update.chunk)
        elif isinstance(update, process.ExitUpdate):
            if on_exit is not None:
                await on_exit(pid, update.exit_code)
        else:  # pragma: nocover
            assert False, update

    tasks: List[asyncio.Task] = []

    if sequential:
        def _handle_updates():
            queue = asyncio.Queue()

            async def run_updates() -> None:
                while True:
                    update = await queue.get()
                    await handle_update(update)
                    if isinstance(update, process.ExitUpdate):
                        break

            tasks.append(asyncio.create_task(run_updates()))

            while True:
                update = yield
                queue.put_nowait(update)
                if isinstance(update, process.ExitUpdate):
                    break
    else:
        def _handle_updates():
            while True:
                update = yield
                tasks.append(asyncio.create_task(handle_update(update)))
                if isinstance(update, process.ExitUpdate):
                    break

    def handle_updates():
        try:
            yield from _handle_updates()

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

    update_keys = {(process.StreamUpdate, pid), (process.ExitUpdate, pid)}
    update_handler: UpdateHandler = handle_updates()
    next(update_handler)
    return update_keys, update_handler


class HandlerRegistry(object):
    _update_keys = {
        # motor.StateUpdate: lambda update: cast(motor.StateUpdate, update).port,
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
