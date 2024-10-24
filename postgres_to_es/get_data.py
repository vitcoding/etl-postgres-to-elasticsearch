# import sqlite3
from contextlib import closing
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from config import BATCH_SIZE, DB_SCHEMA, logger
from dataclasses_ import (
    Filmwork,
)  # , Genre, GenreFilmwork, Person, PersonFilmwork
from validate import validate_data


class PostgresExtractor:
    def __init__(
        self,
        pg_connection: psycopg.Connection,
        schema: str = DB_SCHEMA["postgres"],
        # table_data: dict[
        #     str, Filmwork | Genre | Person | GenreFilmwork | PersonFilmwork
        # ] = TABLE_DATA,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self.pg_connection = pg_connection
        self.schema = schema
        # self.table_data = table_data
        self.batch_size = batch_size
        self.errors = 0

    def extract_data(
        self,
        # table: str,
    ) -> Generator[tuple[str, list[dict_row]], None, None] | bool:
        """Метод получения данных из SQLite"""

        self.pg_connection.row_factory = dict_row
        # data_cls = self.table_data.get(table, None)
        # if data_cls is None:
        #     self.errors += 1
        #     return False

        with closing(
            self.pg_connection.cursor(row_factory=dict_row)
        ) as pg_cursor:
            # logger.debug("Запущено получение данных из таблицы '%s'", table)

            # query = f"SELECT * FROM {self.schema}{table}"

            query = (
                f"SELECT "
                f"fw.id as fw_id, "
                f"fw.title, "
                f"fw.description, "
                f"fw.rating, "
                f"fw.creation_date, "
                f"fw.type, "
                f"fw.created, "
                f"fw.modified, "
                f"p.id as p_id, "
                f"p.full_name as person_name, "
                f"pfw.role as person_role, "
                f"g.id as g_id, "
                f"g.name as genre "
                f"FROM content.film_work fw "
                f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
                f"LEFT JOIN content.person p ON p.id = pfw.person_id "
                f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
                f"LEFT JOIN content.genre g ON g.id = gfw.genre_id "
                # f"WHERE fwcontent.id IN (<id_всех_кинопроизводств>);"
            )
            print(query, "\n")
            try:
                pg_cursor.execute(query)
            # except (sqlite3.OperationalError,) as err:
            except Exception as err:
                logger.error(
                    "Ошибка %s при чтении данных (таблица %s): \n'%s'",
                    type(err),
                    # table,
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


# query = (
#     f"SELECT "
#     f"fw.id as fw_id, "
#     f"fw.title, "
#     f"fw.description, "
#     f"fw.rating, "
#     f"fw.type, "
#     f"fw.created, "
#     f"fw.modified, "
#     f"pfw.role, "
#     f"p.id, "
#     f"p.full_name, "
#     f"g.name "
#     f"FROM content.film_work fw "
#     f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
#     f"LEFT JOIN content.person p ON p.id = pfw.person_id "
#     f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
#     f"LEFT JOIN content.genre g ON g.id = gfw.genre_id "
#     # f"WHERE fwcontent.id IN (<id_всех_кинопроизводств>);"
# )
