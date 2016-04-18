import zmq
import threading
import hedgehog.proto


class HedgehogController:
    def __init__(self, endpoint, identity, context=None):
        context = context or zmq.Context.instance()
        self.socket = context.socket(zmq.DEALER)
        self.socket.identity = identity
        self.socket.connect(endpoint)

    def get_analogs(self, *ports):
        msg = hedgehog.proto.AnalogRequest(ports)
        self.socket.send(msg.SerializeToString())

        msg = hedgehog.proto.parse(self.socket.recv())
        sensors = msg.analog_update.sensors
        return [sensors[port] for port in ports]

    def get_analog(self, port):
        return self.get_analogs(port)[0]

    def close(self):
        self.socket.close()
