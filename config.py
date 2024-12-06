import pika
import logging

RMQ_HOST = "0.0.0.0"
RMQ_PORT = 5672

RMQ_USER = "guest"
RMQ_PASS = "guest"

MQ_exchange = ""
MQ_routing_key = "news"

MQ_EMAIL_UPDATES_EXCHANGE_NAME = "email_updates"
MQ_QUEUE_NAME_KYC_EMAIL_UPDATES = "kyc-email-updates"
MQ_QUEUE_NAME_NEWSLETTER_EMAIL_UPDATES = "newsletter-email-updates"

conn_params = pika.ConnectionParameters(
    host=RMQ_HOST,
    port=RMQ_PORT,
    credentials=pika.PlainCredentials(username=RMQ_USER, password=RMQ_PASS),
)


def get_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection()


def configure_logging(
    level: int = logging.INFO,
    pika_log_level: int = logging.WARNING,
):
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] %(module)s %(levelname)s: %(message)s",
    )

    # изменение конфига логера модуля pika
    logging.getLogger("pika").setLevel(pika_log_level)
