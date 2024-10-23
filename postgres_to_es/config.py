import logging
import os

from dotenv import load_dotenv

from dataclasses_ import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork

load_dotenv()
dsl = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
}

TABLES = (
    # "error_table",
    ###
    "film_work",
    "genre",
    "person",
    "genre_film_work",
    "person_film_work",
)

TABLE_DATA = {
    "film_work": Filmwork,
    "genre": Genre,
    "person": Person,
    "genre_film_work": GenreFilmwork,
    "person_film_work": PersonFilmwork,
}

DB_SCHEMA = {
    "postgres": "content.",
    "sqlite": "",
    ###
    # "sqlite": "user.",
}

BATCH_SIZE = 700

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
