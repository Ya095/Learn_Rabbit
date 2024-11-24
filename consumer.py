from typing import TYPE_CHECKING
import logging

from config import (
    configure_logging,
    MQ_routing_key,
)

from rabbit import RabbitBase

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties

log = logging.getLogger(__name__)


def process_new_message(
    ch: "BlockingChannel",
    method: "Basic.Deliver",
    properties: "BasicProperties",
    body: bytes,
):

    log.warning("[ ] Start processing message (expensive task!) %r", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.warning(
        "[X] Finished processing message %r",
        body,
    )


def consume_messages(channel: "BlockingChannel") -> None:
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(MQ_routing_key)
    channel.basic_consume(
        queue=MQ_routing_key,
        on_message_callback=process_new_message,
        # auto_ack=True,
    )
    log.warning("Waiting for messages...")
    channel.start_consuming()


def main():
    configure_logging(level=logging.WARNING)
    with RabbitBase() as rabbit:
        consume_messages(channel=rabbit.channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")