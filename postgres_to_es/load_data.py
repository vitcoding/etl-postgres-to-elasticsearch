from dataclasses import astuple
from datetime import datetime, timezone
from typing import Generator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config import DB_SCHEMA, logger
from dataclasses_ import Filmwork
from transform_data import TransformData

from time import sleep
import json


class ElasticsearchLoader:
    def init(
        self,
        es_client=Elasticsearch("http://127.0.0.1:9200/"),
    ):
        self.es_client = es_client
        self.counter = 0

    def load_data(
        self,
        data,
    ):
        """Метод загрузки данных в ElasticSearch"""

        es_client = Elasticsearch("http://127.0.0.1:9200/")

        index_name = "movies"
        transform = TransformData()
        transformed_data = transform.transform_batch(data)

        for sources in transformed_data:
            actions = []
            for source in sources:
                # print(f"{source}\n{type(source)}\n\n")
                source = dict(source)
                action = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": source["id"],
                    "_source": source,
                }
                actions.append(action)

            try:
                logger.debug("actions: \n\n%s", actions)
                bulk(es_client, actions=actions)
            except Exception as err:
                logger.error(
                    "Ошибка %s при загрузке данных: \n'%s'\n",
                    type(err),
                    err,
                )
            # sleep(2)
