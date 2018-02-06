from typing import cast, Any, Callable, Dict, Generator, List, Sequence, Set, Tuple, Type

import asyncio

from hedgehog.protocol.messages import ReplyMsg, Message, ack, motor, process


EventHandler = Generator[Set[Tuple[Type[Message], Any]],
                         Message,
                         None]


def process_handler(on_stdout, on_stderr, on_exit, sequential=True):
    # initialize
    reply = yield

    pid = reply.pid
    events = {(process.StreamUpdate, pid),
              (process.ExitUpdate, pid)}

    async def run_update(update):
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

    tasks = []  # type: List[asyncio.Task]

    if sequential:
        queue = asyncio.Queue()

        async def run_updates():
            while True:
                update = await queue.get()
                await run_update(update)
                if isinstance(update, process.ExitUpdate):
                    break

        def on_update(update):
            queue.put_nowait(update)

        tasks.append(asyncio.ensure_future(run_updates()))
    else:
        def on_update(update):
            tasks.append(asyncio.ensure_future(run_update(update)))

    try:
        # update
        update = yield events
        while True:
            on_update(update)
            update = yield
    finally:
        for task in tasks:
            task.cancel()


class HandlerRegistry(object):
    _update_keys = {
        # motor.StateUpdate: lambda update: cast(motor.StateUpdate, update).port,
        process.StreamUpdate: lambda update: cast(process.StreamUpdate, update).pid,
        process.ExitUpdate: lambda update: cast(process.ExitUpdate, update).pid,
    }  # type: Dict[Type[Message], Callable[[Message], Any]]

    @staticmethod
    def _update_key(update: Message) -> Tuple[Type[Message], Any]:
        cls = type(update)
        return cls, HandlerRegistry._update_keys[cls](update)

    def __init__(self) -> None:
        self._handlers = {}  # type: Dict[Tuple[Type[Message], Any], EventHandler]

    def register(self, handlers: Sequence[EventHandler], replies: Sequence[Message]):
        assert len(handlers) == len(replies)
        for handler, reply in zip(handlers, replies):
            if handler is None:
                continue
            if isinstance(reply, ack.Acknowledgement) and reply.code != ack.OK:
                handler.close()
                continue

            next(handler)
            events = handler.send(reply)
            for event in events:
                if event in self._handlers:
                    self._handlers[event].close()
                self._handlers[event] = handler

    def handle_async(self, updates: Sequence[Message]) -> None:
        for update in updates:
            event = HandlerRegistry._update_key(update)
            if event in self._handlers:
                self._handlers[event].send(update)

    def shutdown(self) -> None:
        for handler in self._handlers.values():
            handler.close()
        self._handlers.clear()
