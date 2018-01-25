from typing import Awaitable, Callable

import pytest
from hedgehog.utils.test_utils import zmq_aio_ctx
from hedgehog.client.test_utils import start_dummy, start_dummy_sync, start_server, start_server_sync, Commands

import time
import zmq.asyncio
from contextlib import contextmanager

from hedgehog.client.sync_client import HedgehogClient
from hedgehog.protocol import errors
from hedgehog.protocol.messages import io, motor, process
from hedgehog.protocol.sockets import DealerRouterSocket
from hedgehog.server.hardware import HardwareAdapter


# Pytest fixtures
zmq_aio_ctx, start_dummy, start_dummy_sync, start_server, start_server_sync


# additional fixtures

@pytest.fixture
def connect_client(zmq_aio_ctx: zmq.asyncio.Context):
    @contextmanager
    def do_connect(endpoint, client_class=HedgehogClient):
        with client_class(zmq_aio_ctx, endpoint) as client:
            yield client

    return do_connect


@pytest.fixture
def connect_dummy(start_dummy_sync, connect_client):
    @contextmanager
    def do_connect(server_coro: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                   endpoint: str='inproc://controller', client_class=HedgehogClient, **kwargs):
        with start_dummy_sync(server_coro, *args, endpoint=endpoint, **kwargs) as dummy, \
                connect_client(dummy, client_class=client_class) as client:
            yield client

    return do_connect


@pytest.fixture
def connect_server(start_server_sync, connect_client):
    @contextmanager
    def do_connect(hardware_adapter: HardwareAdapter=None, endpoint: str='inproc://controller',
                   client_class=HedgehogClient):
        with start_server_sync(hardware_adapter=hardware_adapter, endpoint=endpoint) as server, \
                connect_client(server, client_class=client_class) as client:
            yield client

    return do_connect


# tests

def test_connect(connect_server):
    with connect_server() as client:
        client.set_input_state(0, False)


def test_overlapping_contexts(start_server_sync, connect_client):
    with start_server_sync() as server:
        with connect_client(server) as client:
            def do_something():
                time.sleep(0.2)
                assert client.get_analog(0) == 0

            thread = client.spawn(do_something)
            assert client.get_analog(0) == 0
            time.sleep(0.1)
        thread.join()


def test_daemon_context(start_server_sync, connect_client):
    with start_server_sync() as server:
        with connect_client(server) as client:
            def do_something():
                assert client.get_analog(0) == 0
                time.sleep(0.2)
                with pytest.raises(errors.HedgehogCommandError):
                    assert client.get_analog(0) == 0

            thread = client.spawn(do_something, daemon=True)
            time.sleep(0.1)
        thread.join()


# tests for failures

def test_inactive_context(zmq_aio_ctx: zmq.asyncio.Context, start_server_sync):
    with start_server_sync() as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with pytest.raises(RuntimeError):
            client.get_analog(0)

        with client:
            assert client.get_analog(0) == 0

        with pytest.raises(RuntimeError):
            client.get_analog(0)


def test_daemon_context_first(zmq_aio_ctx: zmq.asyncio.Context, start_server_sync):
    with start_server_sync() as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with pytest.raises(RuntimeError):
            with client.daemon:
                pass

        # confirm the client works after a failure
        with client:
            assert client.get_analog(0) == 0


def test_shutdown_context(connect_server):
    with connect_server() as client:
        def do_something():
            assert client.get_analog(0) == 0
            time.sleep(0.2)
            with pytest.raises(errors.EmergencyShutdown):
                assert client.get_analog(0) == 0

        thread = client.spawn(do_something, daemon=True)
        time.sleep(0.1)
        client.shutdown()
        with pytest.raises(errors.EmergencyShutdown):
            assert client.get_analog(0) == 0

        thread.join()

        # this should not raise an exception out of the `with` block
        raise errors.EmergencyShutdown()


def test_reuse_after_shutdown(zmq_aio_ctx: zmq.asyncio.Context, start_server_sync):
    with start_server_sync() as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with client:
            assert client.get_analog(0) == 0

        with pytest.raises(RuntimeError):
            with client:
                pass


def test_unsupported(connect_server):
    with connect_server(hardware_adapter=HardwareAdapter()) as client:
        with pytest.raises(errors.UnsupportedCommandError):
            client.get_analog(0)


# API tests

class TestHedgehogClientAPI(object):
    def test_set_input_state(self, connect_dummy):
        port, pullup = 0, False
        with connect_dummy(Commands.io_action_input, port, pullup) as client:
            assert client.set_input_state(port, pullup) is None

    def test_get_io_config(self, connect_dummy):
        port, flags = 0, io.INPUT_FLOATING
        with connect_dummy(Commands.io_command_request, port, flags) as client:
            assert client.get_io_config(port) == flags

    def test_get_analog(self, connect_dummy):
        port, value = 0, 0
        with connect_dummy(Commands.analog_request, port, value) as client:
            assert client.get_analog(port) == value

    def test_get_digital(self, connect_dummy):
        port, value = 0, False
        with connect_dummy(Commands.digital_request, port, value) as client:
            assert client.get_digital(port) == value

    def test_set_digital_output(self, connect_dummy):
        port, level = 0, False
        with connect_dummy(Commands.io_action_output, port, level) as client:
            assert client.set_digital_output(port, level) is None

    def test_set_motor(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 100
        with connect_dummy(Commands.motor_action, port, state, amount) as client:
            assert client.set_motor(port, state, amount) is None

    def test_get_motor_command(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 0
        with connect_dummy(Commands.motor_command_request, port, state, amount) as client:
            assert client.get_motor_command(port) == (state, amount)

    def test_get_motor_state(self, connect_dummy):
        port, velocity, position = 0, 0, 0
        with connect_dummy(Commands.motor_state_request, port, velocity, position) as client:
            assert client.get_motor_state(port) == (velocity, position)

    def test_set_motor_position(self, connect_dummy):
        port, position = 0, 0
        with connect_dummy(Commands.motor_set_position_action, port, position) as client:
            assert client.set_motor_position(port, position) is None

    def test_set_servo(self, connect_dummy):
        port, active, position = 0, False, 0
        with connect_dummy(Commands.servo_action, port, active, position) as client:
            assert client.set_servo(port, active, position) is None

    def test_get_servo_command(self, connect_dummy):
        port, active, position = 0, False, None
        with connect_dummy(Commands.servo_command_request, port, active, position) as client:
            assert client.get_servo_command(port) == (active, position)

        port, active, position = 0, True, 0
        with connect_dummy(Commands.servo_command_request, port, active, position) as client:
            assert client.get_servo_command(port) == (active, position)


class TestHedgehogClientProcessAPI(object):
    def test_execute_process_handle_nothing(self, connect_dummy):
        pid = 2345
        with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
            assert client.execute_process('echo', 'asdf') == pid

    # def test_execute_process_handle_exit(self, connect_dummy):
    #     pid = 2346
    #     async with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
    #         exit_a, exit_b = pipe()
    #
    #         async def on_exit(_pid, exit_code):
    #             assert _pid == pid
    #             assert exit_code == 0
    #             exit_b.send(None)
    #
    #         assert client.execute_process('echo', 'asdf', on_exit=on_exit) == pid
    #
    #         await exit_a.recv()
    #
    # def test_execute_process_handle_stream(self, connect_dummy):
    #     pid = 2347
    #     with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
    #         exit_a, exit_b = pipe()
    #
    #         counter = 0
    #
    #         async def on_stdout(_pid, fileno, chunk):
    #             nonlocal counter
    #
    #             expect = [
    #                 (pid, process.STDOUT, b'asdf\n'),
    #                 (pid, process.STDOUT, b''),
    #             ]
    #
    #             assert (_pid, fileno, chunk) == expect[counter]
    #             counter += 1
    #
    #             if counter == len(expect):
    #                 await exit_b.send(None)
    #
    #         assert client.execute_process('echo', 'asdf', on_stdout=on_stdout) == pid
    #
    #         await exit_a.recv()
    #
    # def test_execute_process_handle_input(self, connect_dummy):
    #     pid = 2348
    #     with connect_dummy(Commands.execute_process_cat, pid) as client:
    #         assert client.execute_process('cat') == pid
    #         assert client.send_process_data(pid, b'asdf\n') is None
    #         assert client.send_process_data(pid) is None
