import time

from config import configure_logging, MQ_EMAIL_UPDATES_EXCHANGE_NAME
from rabbit.common import EmailUpdatesRabbit
import logging


log = logging.getLogger(__name__)


class Producer(EmailUpdatesRabbit):
    def produce_message(self, idx: int) -> None:
        message_body = f"New message #{idx:02d}"
        self.channel.basic_publish(
            exchange=MQ_EMAIL_UPDATES_EXCHANGE_NAME,
            routing_key="",  # "", тк отправляем в обменник и он сам сделает нужный маршрут
            body=message_body
        )
        log.warning("Published message %s", message_body)


def main():
    configure_logging(level=logging.WARNING)
    with Producer() as producer:
        producer.declare_email_updates_exchange()  # вызываем на случай, если его нет
        for idx in range(1, 6):
            producer.produce_message(idx=idx)
            time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
