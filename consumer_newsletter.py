import time

import config
from config import configure_logging
import logging
from rabbit.common import EmailUpdatesRabbit


log = logging.getLogger(__name__)


def process_new_msg(ch, method, properties, body):
    log.warning("[ ] Update user email for newsletter: %s", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    time.sleep(1)
    log.warning("[X] Updated user email: %s", body)



def main():
    configure_logging(level=logging.WARNING)
    with EmailUpdatesRabbit() as rabbit:
        rabbit.consume_messages(
            message_callback=process_new_msg,
            queue_name=config.MQ_QUEUE_NAME_NEWSLETTER_EMAIL_UPDATES,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
