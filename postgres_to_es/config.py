import logging
import os

from dotenv import load_dotenv

from dataclasses_ import FilmworkExtract

# , Genre, GenreFilmwork, Person, PersonFilmwork

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
    # "error_table",
    ###
    "film_work",
    "genre",
    "person",
    "genre_film_work",
    "person_film_work",
)

# TABLE_DATA = {
#     "film_work": Filmwork,
#     "genre": Genre,
#     "person": Person,
#     "genre_film_work": GenreFilmwork,
#     "person_film_work": PersonFilmwork,
# }

DB_SCHEMA = {
    "postgres": "content.",
    "sqlite": "",
    ###
    # "sqlite": "user.",
}

BATCH_SIZE = 200

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

logger.info("It's config")
