from dataclasses import astuple
from datetime import datetime, timezone
from typing import Generator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config import DB_SCHEMA, TABLE_DATA, logger
from dataclasses_ import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


class ElasticsearchLoader:
    def init(
        self,
        es_client=Elasticsearch("http://127.0.0.1:9200/"),
    ):
        # self.es = Elasticsearch(
        #     [{"host": "localhost", "port": 9200, "scheme": "https"}]
        # )
        self.es_client = es_client
        self.counter = 0

    def load_data(
        self,
        table,
        data,
    ):
        """Метод загрузки данных в ElasticSearch"""

        es_client = Elasticsearch("http://127.0.0.1:9200/")

        index_name = table

        actions = []
        for sources in data:
            for source in sources:
                source = dict(source)
                action = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": source["id"],
                    "_source": source,
                }
                actions.append(action)

        bulk(es_client, actions=actions)

        # sources = [dict(source) for source in sources]
        # print(sources, type(sources), sep="\n")
        # print(sources[0], type(sources[0]), sep="\n")

        # bulk(self.es_client, actions=actions)

        # logger.info("Таблица: %s\nДаные: \n%s\n", table, data)
