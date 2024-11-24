from pika.adapters.blocking_connection import BlockingChannel
from rabbit.exc import RabbitException
import config
import pika


class RabbitBase:
    def __init__(
        self,
        connection_params: pika.ConnectionParameters = config.conn_params,
    ):
        self.connection_params = connection_params
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    @property
    def channel(self) -> BlockingChannel:
        if self._channel is None:
            raise RabbitException("Pls use context manager for rabbit helper.")
        return self._channel

    def __enter__(self):
        self._connection = pika.BlockingConnection(self.connection_params)
        self._channel = self._connection.channel()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._channel.is_open:
            self._channel.close()
        if self._connection.is_open:
            self._connection.close()