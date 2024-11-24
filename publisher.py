import time
from typing import TYPE_CHECKING
import logging

from config import (
    get_connection,
    configure_logging,
    MQ_exchange,
    MQ_routing_key,
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

log = logging.getLogger(__name__)


def declare_queue(channel: "BlockingChannel") -> None:
    queue = channel.queue_declare(queue=MQ_exchange)
    log.info("Declared queue %r %s", MQ_routing_key, queue)


def produce_message(channel: "BlockingChannel", idx: int) -> None:
    message_body = f"New message #{idx:02d}"
    log.info("Publish message %s", message_body)
    channel.basic_publish(
        exchange=MQ_exchange,
        routing_key=MQ_routing_key,
        body=message_body,
    )
    log.warning("Published message %s", message_body)


def main():
    configure_logging(level=logging.WARNING)
    with get_connection() as connection:
        log.info("Created connection: %s", connection)
        with connection.channel() as channel:
            log.info("Created channel: %s", channel)
            declare_queue(channel=channel)
            for idx in range(1, 6):
                produce_message(channel=channel, idx=idx)
                time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")