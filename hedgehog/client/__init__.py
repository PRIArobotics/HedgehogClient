from typing import cast, Callable, Sequence, Tuple

import logging
import os
import signal
import sys
import time
import zmq
from contextlib import contextmanager, suppress
from hedgehog.utils.zmq.actor import CommandRegistry
from hedgehog.utils.zmq.poller import Poller
from hedgehog.utils.discovery.service_node import ServiceNode
from hedgehog.protocol import errors
from hedgehog.protocol.messages import Message, ack, io, analog, digital, motor, servo, process
from pycreate2 import Create2
from .client_backend import ClientBackend
from .client_registry import EventHandler, ProcessUpdateHandler

logger = logging.getLogger(__name__)


class HedgehogClient(object):
    def __init__(self, ctx: zmq.Context, endpoint: str='tcp://127.0.0.1:10789') -> None:
        self.backend = ClientBackend(ctx, endpoint)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        self.backend.client_handle.close()

    def send(self, msg: Message, handler: EventHandler=None) -> Message:
        reply, = self.send_multipart((msg, handler))
        if isinstance(reply, ack.Acknowledgement):
            if reply.code != ack.OK:
                raise errors.error(reply.code, reply.message)
            return None
        else:
            return reply

    def send_multipart(self, *cmds: Tuple[Message, EventHandler]) -> Sequence[Message]:
        return self.backend.client_handle.send_commands(*cmds)

    def spawn(self, callback, *args, name=None, daemon=False, **kwargs) -> None:
        self.backend.spawn(callback, *args, name=name, daemon=daemon, **kwargs)

    def shutdown(self) -> None:
        """
        Shuts down the client's backend.
        A shutdown may occur normally in any thread, or in an interrupt handler on the main thread. If this is invoked
        on an interrupt handler during socket communication, the shutdown is deferred until after the socket operation.
        In that case, `EmergencyShutdown` is raised on the main thread after deferred shutdown. This method returns True
        for an immediate shutdown, False for a deferred one.

        :return: Whether shutdown was performed immediately
        """
        return self.backend.client_handle.shutdown()

    def set_input_state(self, port: int, pullup: bool) -> None:
        self.send(io.Action(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING))

    def get_analog(self, port: int) -> int:
        response = cast(analog.Reply, self.send(analog.Request(port)))
        assert response.port == port
        return response.value

    def get_digital(self, port: int) -> bool:
        response = cast(digital.Reply, self.send(digital.Request(port)))
        assert response.port == port
        return response.value

    def set_digital_output(self, port: int, level: bool) -> None:
        self.send(io.Action(port, io.OUTPUT_ON if level else io.OUTPUT_OFF))

    def get_io_config(self, port: int) -> int:
        response = cast(io.CommandReply, self.send(io.CommandRequest(port)))
        assert response.port == port
        return response.flags

    def set_motor(self, port: int, state: int, amount: int=0,
                  reached_state: int=motor.POWER, relative: int=None, absolute: int=None,
                  on_reached: Callable[[int, int], None]=None) -> None:
        # if on_reached is not None:
        #     if relative is None and absolute is None:
        #         raise ValueError("callback given, but no end position")
        #     handler = MotorUpdateHandler(on_reached)
        # else:
        #     handler = None
        self.send(motor.Action(port, state, amount, reached_state, relative, absolute))

    def move(self, port: int, amount: int, state: int=motor.POWER) -> None:
        self.set_motor(port, state, amount)

    def move_relative_position(self, port: int, amount: int, relative: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        self.set_motor(port, state, amount, relative=relative, on_reached=on_reached)

    def move_absolute_position(self, port: int, amount: int, absolute: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        self.set_motor(port, state, amount, absolute=absolute, on_reached=on_reached)

    def get_motor_command(self, port: int) -> Tuple[int, int]:
        response = cast(motor.CommandReply, self.send(motor.CommandRequest(port)))
        assert response.port == port
        return response.state, response.amount

    def get_motor_state(self, port: int) -> Tuple[int, int]:
        response = cast(motor.StateReply, self.send(motor.StateRequest(port)))
        assert response.port == port
        return response.velocity, response.position

    def get_motor_velocity(self, port: int) -> int:
        velocity, _ = self.get_motor_state(port)
        return velocity

    def get_motor_position(self, port: int) -> int:
        _, position = self.get_motor_state(port)
        return position

    def set_motor_position(self, port: int, position: int) -> None:
        self.send(motor.SetPositionAction(port, position))

    def set_servo(self, port: int, active: bool, position: int) -> None:
        self.send(servo.Action(port, active, position))

    def get_servo_command(self, port: int) -> Tuple[bool, int]:
        response = cast(servo.CommandReply, self.send(servo.CommandRequest(port)))
        assert response.port == port
        return response.active, response.position

    def execute_process(self, *args: str, working_dir: str=None, on_stdout=None, on_stderr=None, on_exit=None) -> int:
        if on_stdout is not None or on_stderr is not None or on_exit is not None:
            handler = ProcessUpdateHandler(on_stdout, on_stderr, on_exit)
        else:
            handler = None
        response = cast(process.ExecuteReply, self.send(process.ExecuteAction(*args, working_dir=working_dir), handler))
        return response.pid

    def signal_process(self, pid: int, signal: int=2) -> None:
        self.send(process.SignalAction(pid, signal))

    def send_process_data(self, pid: int, chunk: bytes=b'') -> None:
        self.send(process.StreamAction(pid, process.STDIN, chunk))


def find_server(ctx, service='hedgehog_server', accept=None):
    if accept is None:
        accept = lambda peer: service in peer.services

    node = ServiceNode(ctx, "Hedgehog Client")
    with node:
        logger.info("Looking for servers...")

        poller = Poller()
        registry = CommandRegistry()
        poller.register(node.evt_pipe, zmq.POLLIN,
                        lambda: registry.handle(node.evt_pipe.recv_multipart()))

        def terminate():
            for socket in list(poller.sockets):
                poller.unregister(socket)

        @registry.command(b'BEACON TERM')
        def handle_beacon_term():
            logger.info("Beacon terminated (network gone?). Retry in 3 seconds...")
            time.sleep(3)
            node.restart_beacon()

        @registry.command(b'ENTER')
        def handle_enter(*args):
            pass

        @registry.command(b'EXIT')
        def handle_exit(*args):
            pass

        @registry.command(b'JOIN')
        def handle_join(*args):
            pass

        @registry.command(b'LEAVE')
        def handle_leave(*args):
            pass

        @registry.command(b'$TERM')
        def handle_term():
            logger.warn("Node terminated")
            terminate()

        @registry.command(b'UPDATE')
        def handle_update():
            peer = node.evt_pipe.pop()
            if accept(peer):
                terminate()
            return peer

        node.join(service)
        node.request_service(service)
        server = None

        with suppress(KeyboardInterrupt):
            while len(poller.sockets) > 0:
                items = poller.poll(1000)
                if len(items) > 0:
                    for _, _, handler in items:
                        server = handler()
                else:
                    node.request_service(service)
        return server


def get_client(endpoint='tcp://127.0.0.1:10789', service='hedgehog_server',
               ctx=None, client_class=HedgehogClient):
    # TODO when the context is created here, the caller has the responsibility to clean it up!
    ctx = ctx or zmq.Context()

    if endpoint is None:
        server = None
        while server is None:
            server = find_server(ctx, service)
        endpoint = list(server.services[service])[0]
        logger.debug("Chose this endpoint via discovery: {}".format(endpoint))

    return client_class(ctx, endpoint)


class __ProcessConfig(object):
    INSTANCE = None

    def __init__(self) -> None:
        self.clients = []

        def sigint_handler(signal, frame):
            self.shutdown()

        signal.signal(signal.SIGINT, sigint_handler)

    @classmethod
    def instance(cls) -> '__ProcessConfig':
        if cls.INSTANCE is None:
            cls.INSTANCE = cls()
        return cls.INSTANCE

    @contextmanager
    def register_client(self, client: HedgehogClient) -> None:
        self.clients.append(client)
        yield
        self.clients.remove(client)

    def shutdown(self) -> None:
        # note that this list comprehension has serious side effects!
        immediates = [client.shutdown() for client in self.clients]
        if all(immediates):
            # no client handle was in a critical section, so we immediately raise `EmergencyShutdown`
            raise errors.EmergencyShutdown("Emergency Shutdown activated")


def shutdown() -> None:
    __ProcessConfig.instance().shutdown()


@contextmanager
def connect(endpoint='tcp://127.0.0.1:10789', emergency=None, service='hedgehog_server',
            ctx=None, client_class=HedgehogClient, process_setup=True):
    # Force line buffering
    # TODO is there a cleaner way to do this than to reopen stdout, here?
    if process_setup:
        # ensure that reopening did happen
        __ProcessConfig.instance()

    with get_client(endpoint, service, ctx, client_class) as client:
        # getting the handle for the first time in the interrupt handler is problematic,
        # so make sure it is already fetched as the first thing
        client.backend.client_handle

        # if process_setup is set, register the client object with the __ProcessConfig.
        # if not, suppress() acts as a dummy
        with __ProcessConfig.instance().register_client(client) if process_setup else suppress():

            # TODO a remote application's emergency_stop is remote, so it won't work in case of a disconnection!
            def emergency_stop():
                try:
                    client.set_input_state(emergency, True)
                    # while not client.get_digital(emergency):
                    while client.get_digital(emergency):
                        time.sleep(0.1)

                    os.kill(os.getpid(), signal.SIGINT)
                except errors.EmergencyShutdown:
                    # the backend was shutdown; that means we don't need to do it, and that the program should terminate
                    # we do our part and let this thread terminate
                    pass

            if emergency is not None:
                client.spawn(emergency_stop, name="emergency_stop", daemon=True)

            try:
                yield client
            except errors.EmergencyShutdown as ex:
                print(ex)


@contextmanager
def connect_create(port='/dev/ttyUSB0', baud=57600):
    create = Create2(port, baud=baud)
    create.start()
    yield create
    # TODO do proper cleanup instead of waiting for the GC
    # del create does not seem to work, and create.__del__() raises an exception upon actual GC


@contextmanager
def connect_create2(port='/dev/ttyUSB0', baud=115200):
    create = Create2(port, baud=baud)
    create.start()
    yield create
    # TODO do proper cleanup instead of waiting for the GC
