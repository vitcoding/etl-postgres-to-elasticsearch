import sqlite3
from contextlib import closing
from time import perf_counter

import psycopg
from psycopg import ClientCursor
from psycopg.rows import dict_row

from config import TABLES, dsl, logger
from get_data import SQLiteLoader
from load_data import PostgresSaver
from tests.check_consistency.test import test_transfer


def load_from_sqlite(
    sqllite_connection: sqlite3.Connection, pg_connection: psycopg.Connection
) -> bool:
    """Основной метод загрузки данных из SQLite в Postgres"""

    sqlite_loader = SQLiteLoader(sqllite_connection)
    postgres_saver = PostgresSaver(pg_connection)
    errors_total = 0

    for table in TABLES:
        data = sqlite_loader.load_data(table)
        postgres_saver.save_all_data(table, data)

    errors_total += sqlite_loader.errors + postgres_saver.errors
    logger.debug("Количество ошибок 'sqlite_loader': %s", sqlite_loader.errors)
    logger.debug(
        "Количество ошибок 'postgres_saver': %s\n", postgres_saver.errors
    )
    logger.info("Всего ошибок: %s\n", errors_total)

    if errors_total > 0:
        return False
    return True


if __name__ == "__main__":

    with closing(sqlite3.connect("db.sqlite")) as sqlite_connection, closing(
        psycopg.connect(
            **dsl, row_factory=dict_row, cursor_factory=ClientCursor
        )
    ) as pg_connection:
        logger.info("Программа запущена\n")
        start_time = perf_counter()

        transfer = load_from_sqlite(sqlite_connection, pg_connection)

        start_tests_time = perf_counter()

        test_transfer(sqlite_connection, pg_connection, TABLES)

        end_time = perf_counter()
        result = (
            "В ходе переноса данных возникли ошибки.",
            "🎉 Данные успешно перенесены !!!",
        )[transfer]
        logger.info(result)

    transfer_time = start_tests_time - start_time
    logger.debug("\nВремя выполнения переноса данных: %s", transfer_time)

    tests_time = end_time - start_tests_time
    logger.debug("\nВремя выполнения тестов: %s", tests_time)

    execute_time = end_time - start_time
    logger.info("\nВремя выполнения программы: %s", execute_time)
