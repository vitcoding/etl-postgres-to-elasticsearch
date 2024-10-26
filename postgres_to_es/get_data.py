from contextlib import closing
from datetime import datetime
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

    def get_query(self):

        if self.update_time is None:
            all_films = "SELECT id FROM content.film_work "
            query_list = [all_films]

        else:
            all_films_updates = (
                f"SELECT id FROM content.film_work "
                f"WHERE modified > '{str(self.update_time)}';"
            )
            all_persons_updates = (
                f"SELECT fw.id FROM content.film_work fw "
                f"LEFT JOIN content.person_film_work pfw "
                f"ON pfw.film_work_id = fw.id "
                f"LEFT JOIN content.person p ON p.id = pfw.person_id "
                f"WHERE p.modified > '{str(self.update_time)}';"
            )

            all_genres_updates = (
                f"SELECT fw.id FROM content.film_work fw "
                f"LEFT JOIN content.genre_film_work gfw "
                f"ON gfw.film_work_id = fw.id "
                f"LEFT JOIN content.genre g ON g.id = gfw.genre_id "
                f"WHERE g.modified > '{str(self.update_time)}';"
            )
            query_list = [
                all_films_updates,
                all_persons_updates,
                all_genres_updates,
            ]
        return query_list

    def get_movies_ids(self):
        self.pg_connection.row_factory = dict_row

        with closing(
            self.pg_connection.cursor(row_factory=dict_row)
        ) as pg_cursor:
            logger.debug("Запущено получение ids")

            # query = "SELECT id FROM content.film_work;"
            # query = self.get_query()
            ids_set = set()
            for query in self.get_query():
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
                try:
                    ids = pg_cursor.fetchall()
                # except (TypeError,) as err:
                except Exception as err:
                    logger.error(
                        "Ошибка %s при получении ID фильмов: \n'%s'",
                        type(err),
                        err,
                    )
                    # self.errors += 1

                ids = set(list(id.values())[0] for id in ids)
                ids_set |= ids
                # ids = set(list(ids.values()))
                # ids_list.append(ids)
                # logger.info("\nids: \n%s\n", ids)
                # logger.info("\nlen(ids): \n%s\n", len(ids))
            logger.info("\nids_set: \n%s\n", ids_set)
            logger.info("\nlen(ids_set): \n%s\n", len(ids_set))

            logger.debug("Сформирован SQL запрос:\n'%s'", query)

            ids_list = list(ids_set)

            for i in range(0, len(ids_list), self.batch_size):
                batch = ids_list[i : i + self.batch_size]
                logger.info("\nbatch: \n%s\n", batch)
                ###
                yield batch

    def extract_data(
        self,
        update_time,
    ) -> Generator[tuple[str, list[dict_row]], None, None] | bool:
        """Метод получения данных из SQLite"""

        self.update_time = update_time

        for ids in self.get_movies_ids():
            logger.debug("\nIDs: \n%s\n", ids)
            logger.debug("\nlen(IDs): \n%s\n", len(ids))

            query_ids_list = [repr(id) for id in ids]
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
                    f"LEFT JOIN content.person_film_work pfw "
                    f"ON pfw.film_work_id = fw.id "
                    f"LEFT JOIN content.person p ON p.id = pfw.person_id "
                    f"LEFT JOIN content.genre_film_work gfw "
                    f"ON gfw.film_work_id = fw.id "
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

                batch = pg_cursor.fetchall()
                try:
                    yield batch
                # except (TypeError,) as err:
                except Exception as err:
                    logger.error(
                        "Ошибка %s при получении данных: \n'%s'",
                        type(err),
                        err,
                    )
                    self.errors += 1
