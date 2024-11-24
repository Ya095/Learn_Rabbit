import time

import config
from config import configure_logging
import logging
from rabbit.common import EmailUpdatesRabbit


log = logging.getLogger(__name__)


def process_new_msg(ch, method, properties, body):
    log.warning("body: %s", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    time.sleep(2)


def main():
    configure_logging(level=logging.WARNING)
    with EmailUpdatesRabbit() as rabbit:
        rabbit.consume_messages(
            message_callback=process_new_msg,
            # добавить имя, что бы не было создано автоматически
            queue_name=config.MQ_QUEUE_NAME_KYC_EMAIL_UPDATES,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
