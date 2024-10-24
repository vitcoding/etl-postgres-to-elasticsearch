# import sqlite3
from contextlib import closing
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from config import BATCH_SIZE, DB_SCHEMA, TABLE_DATA, logger
from dataclasses_ import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork
from validate import validate_data


class PostgresExtractor:
    def __init__(
        self,
        pg_connection: psycopg.Connection,
        schema: str = DB_SCHEMA["postgres"],
        table_data: dict[
            str, Filmwork | Genre | Person | GenreFilmwork | PersonFilmwork
        ] = TABLE_DATA,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self.pg_connection = pg_connection
        self.schema = schema
        self.table_data = table_data
        self.batch_size = batch_size
        self.errors = 0

    def extract_data(
        self,
        table: str,
    ) -> Generator[tuple[str, list[dict_row]], None, None] | bool:
        """Метод получения данных из SQLite"""

        self.pg_connection.row_factory = dict_row
        data_cls = self.table_data.get(table, None)
        if data_cls is None:
            self.errors += 1
            return False

        with closing(
            self.pg_connection.cursor(row_factory=dict_row)
        ) as pg_cursor:
            logger.debug("Запущено получение данных из таблицы '%s'", table)
            try:
                pg_cursor.execute(
                    query := f"SELECT * FROM {self.schema}{table}"
                )
            # except (sqlite3.OperationalError,) as err:
            except Exception as err:
                logger.error(
                    "Ошибка %s при чтении данных (таблица %s): \n'%s'",
                    type(err),
                    table,
                    err,
                )
                self.errors += 1

            logger.debug("Сформирован SQL запрос:\n'%s'", query)

            while batch := pg_cursor.fetchmany(self.batch_size):
                try:
                    # yield [data_cls(**validate_data(row)) for row in batch]
                    yield [data_cls(**row) for row in batch]
                # except (TypeError,) as err:
                except Exception as err:
                    logger.error(
                        "Ошибка %s при получении данных: \n'%s'",
                        type(err),
                        err,
                    )
                    self.errors += 1

    # def get_data(
    #     self,
    #     table: str,
    # ) -> Generator[tuple[str, list[dict_row]], None, None] | bool:
    #     """Метод получения данных из SQLite"""

    #     self.sqlite_connection.row_factory = sqlite3.Row
    #     data_cls = self.table_data.get(table, None)
    #     if data_cls is None:
    #         self.errors += 1
    #         return False

    #     with closing(self.sqlite_connection.cursor()) as sqlite_cursor:
    #         logger.debug("Запущено получение данных из таблицы '%s'", table)
    #         try:
    #             sqlite_cursor.execute(
    #                 query := f"SELECT * FROM {self.schema}{table}"
    #             )
    #         except (sqlite3.OperationalError,) as err:
    #             logger.error(
    #                 "Ошибка %s при чтении данных (таблица %s): \n'%s'",
    #                 type(err),
    #                 table,
    #                 err,
    #             )
    #             self.errors += 1

    #         logger.debug("Сформирован SQL запрос:\n'%s'", query)

    #         while batch := sqlite_cursor.fetchmany(self.batch_size):
    #             try:
    #                 yield [data_cls(**validate_data(row)) for row in batch]
    #             except (TypeError,) as err:
    #                 logger.error(
    #                     "Ошибка %s при получении данных: \n'%s'",
    #                     type(err),
    #                     err,
    #                 )
    #                 self.errors += 1
