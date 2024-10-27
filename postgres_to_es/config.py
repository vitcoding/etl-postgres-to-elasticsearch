import logging
import os

from dotenv import load_dotenv

load_dotenv()

DSL = {
    "dbname": os.environ.get("POSTGRES_NAME"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.getenv("SQL_HOST"),
    "port": os.getenv("SQL_PORT"),
}

ES_HOST = os.environ.get("ELASTICSEARCH_HOST")
ES_PORT = os.environ.get("ELASTICSEARCH_PORT")

TABLES = (
    "film_work",
    "genre",
    "person",
    "genre_film_work",
    "person_film_work",
)

DB_SCHEMA = {
    "postgres": "content.",
}

BATCH_SIZE = 200

SLEEP_TIME = 10

format_log = (
    "#%(levelname)-8s [%(asctime)s] - %(filename)s:"
    "%(lineno)d - %(name)s - %(message)s"
)
logging.basicConfig(
    # level=logging.DEBUG,
    level=logging.INFO,
    # level=logging.ERROR,
    format=format_log,
)

# file_handler = logging.FileHandler("logs.log")
# formatter_file = logging.Formatter(fmt=format_log)
# file_handler.setFormatter(formatter_file)

logger = logging.getLogger(__name__)

# logger.addHandler(file_handler)
# logger.setLevel(logging.DEBUG)
