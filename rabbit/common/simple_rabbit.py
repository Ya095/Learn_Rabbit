"""
    creating dead letter queue and exchange for news
"""
from typing import TYPE_CHECKING, Callable
import logging

from pika.spec import BasicProperties

import config
from rabbit import RabbitBase


if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties


log = logging.getLogger(__name__)


class SimpleRabbitMixin:
    channel: "BlockingChannel"

    def declare_queue(self) -> None:
        self.channel.exchange_declare(
            exchange=config.MQ_SIMPLE_DEAD_LETTER_EXCHANGE
        )
        dlq = self.channel.queue_declare(
            queue=config.MQ_SIMPLE_DEAD_LETTER_KEY
        )
        log.info("Declared dlq: %r", dlq.method.queue)

        self.channel.queue_bind(
            queue=dlq.method.queue,
            exchange=config.MQ_SIMPLE_DEAD_LETTER_EXCHANGE,
        )

        queue = self.channel.queue_declare(
            queue=config.MQ_routing_key,
            arguments={
                "x-dead-letter-exchange": config.MQ_SIMPLE_DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key": config.MQ_SIMPLE_DEAD_LETTER_KEY,
            },
        )
        log.info("Declared queue: %r", queue.method.queue)

    def consume_messages(
            self,
            message_callback: Callable[
                [
                    "BlockingChannel",
                    "Basic.Deliver",
                    "BasicProperties",
                    bytes,
                ],
                None
            ],
            prefetch_count: int = 1,
    ):
        self.channel.basic_qos(prefetch_count=prefetch_count)
        self.declare_queue()
        self.channel.basic_consume(
            queue=config.MQ_routing_key,
            on_message_callback=message_callback,
            # auto_ack=True,
        )
        log.warning("Waiting for messages...")
        self.channel.start_consuming()


class SimpleRabbit(SimpleRabbitMixin, RabbitBase):
    pass
