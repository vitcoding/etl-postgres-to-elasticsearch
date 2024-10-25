from contextlib import closing
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from config import BATCH_SIZE, DB_SCHEMA, logger
from dataclasses_ import FilmworkExtract
from validate import validate_data


class PostgresExtractor:
    def __init__(
        self,
        pg_connection: psycopg.Connection,
        schema: str = DB_SCHEMA["postgres"],
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self.pg_connection = pg_connection
        self.schema = schema
        self.batch_size = batch_size
        self.errors = 0

    def get_movies_ids(self):
        self.pg_connection.row_factory = dict_row

        with closing(
            self.pg_connection.cursor(row_factory=dict_row)
        ) as pg_cursor:
            logger.debug("Запущено получение ids")

            query = f"SELECT " f"id " f"FROM content.film_work;"
            try:
                pg_cursor.execute(query)
            # except (sqlite3.OperationalError,) as err:
            except Exception as err:
                logger.error(
                    "Ошибка %s при чтении данных: \n'%s'",
                    type(err),
                    err,
                )
                self.errors += 1

            logger.debug("Сформирован SQL запрос:\n'%s'", query)

            ids = pg_cursor.fetchmany(self.batch_size)
            return ids

    def extract_data(
        self,
    ) -> Generator[tuple[str, list[dict_row]], None, None] | bool:
        """Метод получения данных из SQLite"""

        ids = self.get_movies_ids()
        logger.debug("IDs: \n%s\n", ids)

        # query_ids_list = [repr(list(id.values())[0]) for id in ids[:30]]
        query_ids_list = [repr(list(id.values())[0]) for id in ids]
        logger.debug("IDs list for query: \n%s\n", query_ids_list)

        query_ids = ", ".join(query_ids_list)
        logger.info("IDs for query: \n%s\n", query_ids)

        self.pg_connection.row_factory = dict_row

        with closing(
            self.pg_connection.cursor(row_factory=dict_row)
        ) as pg_cursor:
            logger.debug("Запущено получение данных")

            query = (
                f"SELECT "
                f"fw.id, "
                f"fw.rating as imdb_rating, "
                f"g.name as g_genre ,"
                f"fw.title, "
                f"fw.description, "
                f"p.id as p_id, "
                f"p.full_name as p_name, "
                f"pfw.role as p_role "
                f"FROM content.film_work fw "
                f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
                f"LEFT JOIN content.person p ON p.id = pfw.person_id "
                f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
                f"LEFT JOIN content.genre g ON g.id = gfw.genre_id "
                f"WHERE fw.id IN ({query_ids});"
            )
            try:
                pg_cursor.execute(query)
            # except (sqlite3.OperationalError,) as err:
            except Exception as err:
                logger.error(
                    "Ошибка %s при чтении данных: \n'%s'",
                    type(err),
                    err,
                )
                self.errors += 1

            logger.debug("Сформирован SQL запрос:\n'%s'", query)

            while batch := pg_cursor.fetchmany(self.batch_size):
                try:
                    # yield [data_cls(**validate_data(row)) for row in batch]
                    # yield [data_cls(**row) for row in batch]
                    yield [row for row in batch]
                # except (TypeError,) as err:
                except Exception as err:
                    logger.error(
                        "Ошибка %s при получении данных: \n'%s'",
                        type(err),
                        err,
                    )
                    self.errors += 1
