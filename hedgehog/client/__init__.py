import logging
import os
import sys
import time
import zmq
from contextlib import contextmanager
from hedgehog.utils.zmq.actor import CommandRegistry
from hedgehog.utils.zmq.poller import Poller
from hedgehog.utils.discovery.service_node import ServiceNode
from hedgehog.protocol import errors
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo, process
from .components import AnalogSensor, DigitalSensor, DigitalOutput, Motor, Servo, Process
from .client_backend import ClientBackend
from .client_registry import MotorUpdateHandler, ProcessUpdateHandler

logger = logging.getLogger(__name__)


class HedgehogClient(object):
    def __init__(self, ctx, endpoint='tcp://127.0.0.1:10789'):
        self.backend = ClientBackend(ctx, endpoint)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.backend.client_handle.close()

    def send(self, msg, handler=None):
        reply, = self.send_multipart((msg, handler))
        if isinstance(reply, ack.Acknowledgement):
            if reply.code != ack.OK:
                raise errors.error(reply.code, reply.message)
            return None
        else:
            return reply

    def send_multipart(self, *cmds):
        return self.backend.client_handle.send_commands(*cmds)

    def spawn(self, callback, *args, daemon=False, **kwargs):
        self.backend.spawn(callback, *args, daemon=daemon, **kwargs)

    def shutdown(self):
        self.backend.client_handle.shutdown()

    def set_input_state(self, port, pullup):
        self.send(io.StateAction(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING))

    def get_analog(self, port):
        response = self.send(analog.Request(port))
        assert response.port == port
        return response.value

    def get_digital(self, port):
        response = self.send(digital.Request(port))
        assert response.port == port
        return response.value

    def set_digital_output(self, port, level):
        self.send(io.StateAction(port, io.OUTPUT_ON if level else io.OUTPUT_OFF))

    def set_motor(self, port, state, amount=0, reached_state=motor.POWER, relative=None, absolute=None, on_reached=None):
        if on_reached is not None:
            if relative is None and absolute is None:
                raise ValueError("callback given, but no end position")
            handler = MotorUpdateHandler(on_reached)
        else:
            handler = None
        self.send(motor.Action(port, state, amount, reached_state, relative, absolute), handler)

    def move(self, port, amount, state=motor.POWER):
        self.set_motor(port, state, amount)

    def move_relative_position(self, port, amount, relative, state=motor.POWER, on_reached=None):
        self.set_motor(port, state, amount, relative=relative, on_reached=on_reached)

    def move_absolute_position(self, port, amount, absolute, state=motor.POWER, on_reached=None):
        self.set_motor(port, state, amount, absolute=absolute, on_reached=on_reached)

    def get_motor(self, port):
        response = self.send(motor.Request(port))
        assert response.port == port
        return response.velocity, response.position

    def get_motor_velocity(self, port):
        velocity, _ = self.get_motor(port)
        return velocity

    def get_motor_position(self, port):
        _, position = self.get_motor(port)
        return position

    def set_motor_position(self, port, position):
        self.send(motor.SetPositionAction(port, position))

    def set_servo(self, port, active, position):
        self.send(servo.Action(port, active, position))

    def execute_process(self, *args, working_dir=None, on_stdout=None, on_stderr=None, on_exit=None):
        if on_stdout is not None or on_stderr is not None or on_exit is not None:
            handler = ProcessUpdateHandler(on_stdout, on_stderr, on_exit)
        else:
            handler = None
        response = self.send(process.ExecuteRequest(*args, working_dir=working_dir), handler)
        return response.pid

    def signal_process(self, pid, signal=2):
        self.send(process.SignalAction(pid, signal))

    def send_process_data(self, pid, chunk=b''):
        self.send(process.StreamAction(pid, process.STDIN, chunk))

    def analog(self, port):
        return AnalogSensor(self, port)

    def digital(self, port):
        return DigitalSensor(self, port)

    def output(self, port):
        return DigitalOutput(self, port)

    def motor(self, port):
        return Motor(self, port)

    def servo(self, port):
        return Servo(self, port)

    def process(self, *args, **kwargs):
        return Process(self, self.execute_process(*args, **kwargs))


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
        def handle_enter(*args):
            pass

        @registry.command(b'JOIN')
        def handle_enter(*args):
            pass

        @registry.command(b'LEAVE')
        def handle_enter(*args):
            pass

        @registry.command(b'$TERM')
        def handle_term():
            logger.warn("Node terminated")
            terminate()

        @registry.command(b'UPDATE')
        def handle_term():
            peer = node.evt_pipe.pop()
            if accept(peer):
                terminate()
            return peer

        node.join(service)
        node.request_service(service)
        server = None

        while len(poller.sockets) > 0:
            items = poller.poll(1000)
            if len(items) > 0:
                for _, _, handler in items:
                    server = handler()
            else:
                node.request_service(service)
        return server


def get_client(endpoint='tcp://127.0.0.1:10789', service='hedgehog_server', ctx=None):
    ctx = ctx or zmq.Context()

    if endpoint is None:
        server = None
        while server is None:
            server = find_server(ctx, service)
        endpoint = list(server.services[service])[0]
        logger.debug("Chose this endpoint via discovery: {}".format(endpoint))

    return HedgehogClient(ctx, endpoint)

@contextmanager
def connect(endpoint='tcp://127.0.0.1:10789', emergency=None, service='hedgehog_server', ctx=None):
    # Force line buffering
    # TODO is there a cleaner way to do this than to reopen stdout, here?
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 1)
    with get_client(endpoint, service, ctx) as client:
        # TODO a remote application's emergency_stop is remote, so it won't work in case of a disconnection!
        def emergency_stop():
            try:
                client.set_input_state(emergency, True)
                # while not client.get_digital(emergency):
                while client.get_digital(emergency):
                    time.sleep(0.1)
                client.shutdown()
            except errors.FailedCommandError:
                # the backend was shutdown; that means we don't need to do it, and that the program should terminate
                # we do our part and let this thread terminate
                pass

        if emergency is not None:
            client.spawn(emergency_stop, daemon=True)
        yield client
