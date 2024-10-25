import json
from dataclasses import astuple
from datetime import datetime, timezone
from time import sleep
from typing import Generator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config import DB_SCHEMA, logger
from dataclasses_ import FilmworkExtract
from es_schema import es_settings
from transform_data import TransformData


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

        counter = 0
        es_client = Elasticsearch("http://127.0.0.1:9200/")

        index_name = "movies"
        transform = TransformData()
        transformed_data = transform.transform_batch(data)

        # Настройка схемы и создание индекса
        settings = es_settings
        if not es_client.indices.exists(index=index_name):
            es_client.indices.create(index=index_name, body=settings)
            logger.info("Индекс '%s' успешно создан.", index_name)
        else:
            logger.info("Индекс с именем '%s' уже существует.", index_name)

        for sources in transformed_data:
            inner_counter = 0
            actions = []
            for source in sources:
                # print(f"{source}\n{type(source)}\n\n")
                source = dict(source)
                # print(str(source), end="\n\n")
                # source = json.loads(dict(source))
                action = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": source["id"],
                    "_source": source,
                }
                actions.append(action)
                inner_counter += 1
            action_example = actions[0] if len(actions) > 0 else None
            logger.info("\nПример данных для загрузки: \n%s\n", action_example)
            counter += inner_counter
            logger.info("\nLoad counter:  %s\n", counter)

            try:
                logger.debug("actions: \n\n%s", actions)
                bulk(es_client, actions=actions)
            except Exception as err:
                logger.error(
                    "Ошибка %s при загрузке данных: \n'%s'\n",
                    type(err),
                    err,
                )
            # sleep(5)
            # logger.info("Sleep...\n\n")
