import sqlite3
from contextlib import closing
from dataclasses import astuple
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from config import DB_SCHEMA, TABLE_DATA, logger
from dataclasses_ import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


class PostgresSaver:
    def __init__(
        self,
        pg_connection: psycopg.Connection,
        schema: str = DB_SCHEMA["postgres"],
        table_data: dict[
            str, Filmwork | Genre | Person | GenreFilmwork | PersonFilmwork
        ] = TABLE_DATA,
    ):
        self.pg_connection = pg_connection
        self.schema = schema
        self.table_data = table_data
        self.errors = 0

    def save_all_data(
        self, table: str, data: Generator[list[sqlite3.Row], None, None]
    ):
        """Метод загрузки данных в Postgres"""

        data_cls = self.table_data.get(table, None)
        if data_cls is None:
            self.errors += 1
            return False
        args_tuple = data_cls.__dict__["__match_args__"]
        args_names = ", ".join(args_tuple)
        args_values = ", ".join(["%s"] * len(args_tuple))
        counter = 0

        with closing(
            self.pg_connection.cursor(row_factory=dict_row)
        ) as pg_cursor:
            if not isinstance(data, Generator):
                self.errors += 1
                return False

            logger.debug("Запущена загрузка данных для таблицы '%s'", table)
            query = (
                f"INSERT INTO {self.schema}{table} "
                f"({args_names}) "
                f"VALUES ({args_values}) "
                f"ON CONFLICT (id) DO NOTHING"
            )
            logger.debug("Сформирован SQL запрос:\n'%s'", query)

            for batch in data:
                counter += 1

                batch_as_tuples = [astuple(row) for row in batch]
                try:
                    pg_cursor.executemany(query, batch_as_tuples)
                    logger.info(
                        "Загружены данные: таблица '%s', партия %s",
                        table,
                        counter,
                    )
                except (
                    psycopg.errors.UndefinedTable,
                    psycopg.errors.UndefinedColumn,
                    psycopg.errors.ForeignKeyViolation,
                ) as err:
                    logger.error(
                        "Ошибка %s при загрузке данных "
                        "(таблица '%s', партия %s):"
                        "\n'%s'\n",
                        type(err),
                        table,
                        counter,
                        err,
                    )
                    self.errors += 1

                self.pg_connection.commit()

        return True
