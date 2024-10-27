from contextlib import closing
from datetime import datetime, timezone
from time import perf_counter, sleep

import psycopg
from psycopg import ClientCursor
from psycopg.rows import dict_row

from config import DSL, logger
from data_state import JsonFileStorage, State
from get_data import PostgresExtractor
from load_data import ElasticsearchLoader


def load_from_postgres(pg_connection: psycopg.Connection) -> bool:
    """Основной метод загрузки данных из Postgres в ElasticSearch"""

    postgres_extractor = PostgresExtractor(pg_connection)
    elasticsearch_loader = ElasticsearchLoader()
    errors_total = 0

    storage = JsonFileStorage("./data/data_state.json")
    state = State(storage)

    update_time = state.get_state("last_update")
    new_update_time = datetime.now(timezone.utc)

    data = postgres_extractor.extract_data(update_time)
    result = elasticsearch_loader.load_data(data)

    if result is True:
        update_time = state.set_state("last_update", str(new_update_time))

    # errors_total += postgres_getter.errors + es_saver.errors
    # logger.debug("Количество ошибок 'sqlite_loader': %s", postgres_getter.errors)
    # logger.debug(
    #     "Количество ошибок 'postgres_saver': %s\n", es_saver.errors
    # )
    # logger.info("Всего ошибок: %s\n", errors_total)

    if errors_total > 0:
        return False
    return True


def main():
    with closing(
        psycopg.connect(
            **DSL, row_factory=dict_row, cursor_factory=ClientCursor
        )
    ) as pg_connection:
        logger.info("Программа запущена\n")
        start_time = perf_counter()

        transfer = load_from_postgres(pg_connection)

        end_time = perf_counter()
        # result = (
        #     "В ходе переноса данных возникли ошибки.",
        #     "🎉 Данные успешно перенесены !!!",
        # )[transfer]
        # logger.info(result)

    execute_time = end_time - start_time
    logger.info("\n🎉 Время выполнения цикла программы: \n%s\n", execute_time)
    sleep(10)


if __name__ == "__main__":
    counter = 0
    while True:
        main()
        # try:
        #     main()
        # except Exception as err:
        #     counter += 1
        #     logger.info("\nОшибка: \n%s", err)
        #     if counter > 10:
        #         break
        #     sleep(3)
