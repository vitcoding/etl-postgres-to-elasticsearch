import sqlite3
from contextlib import closing
from time import perf_counter

import psycopg
from psycopg import ClientCursor
from psycopg.rows import dict_row

from config import dsl, logger
from get_data import PostgresExtractor
from load_data import ElasticsearchLoader


# typing to change
def load_from_postgres(pg_connection: psycopg.Connection) -> bool:
    """Основной метод загрузки данных из Postgres в ElasticSearch"""

    postgres_extractor = PostgresExtractor(pg_connection)
    elasticsearch_loader = ElasticsearchLoader()
    errors_total = 0

    counter = 0

    data = postgres_extractor.extract_data()
    elasticsearch_loader.load_data(data)

    # # for debug only
    # ###
    # for line in data:
    #     # logger.info("%s\n", line)
    #     logger.info("fw: \n%s\ntype(fw): \n%s", line[0], type(line[0]))
    #     # for key in line[0]:
    #     #     print(key)
    #     # print()
    #     counter += 1

    # logger.info("Counter: %s\n\n", counter)
    # ###

    # es_loader = ElasticsearchLoader()
    # es_loader.load_data()

    # errors_total += postgres_getter.errors + es_saver.errors
    # logger.debug("Количество ошибок 'sqlite_loader': %s", postgres_getter.errors)
    # logger.debug(
    #     "Количество ошибок 'postgres_saver': %s\n", es_saver.errors
    # )
    # logger.info("Всего ошибок: %s\n", errors_total)

    if errors_total > 0:
        return False
    return True


if __name__ == "__main__":

    with closing(
        psycopg.connect(
            **dsl, row_factory=dict_row, cursor_factory=ClientCursor
        )
    ) as pg_connection:
        logger.info("Программа запущена\n")
        start_time = perf_counter()

        transfer = load_from_postgres(pg_connection)

        start_tests_time = perf_counter()

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
