from datetime import datetime, timezone
from uuid import UUID

import elasticsearch
from elasticsearch import helpers

es = elasticsearch.Elasticsearch(["http://localhost:9200"])

# Request: http://127.0.0.1:9200/my-index/_search
# or http://localhost:9200/my-index/_search


### ???
# self.es.indices.get_mapping()
# self.es.indices.validate_query()

documents = [
    {
        "title": "Document Title 1",
        "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    },
    {
        "title": "Document Title 2",
        "content": "Praesent posuere tempus pretium",
    },
]

index_name = "my-index"
doc_type = "docs"

#########################

for document in documents:
    document["@timestamp"] = datetime.now(timezone.utc)
    index_and_type = index_name + "/" + doc_type
    # es.index(
    #     index=index_name,
    #     # doc_type=doc_type,
    #     id=str(document["title"]),
    #     document=document,
    # )

#########################

operations = [
    {
        "_op_type": "index",
        "_index": index_name,
        "_id": "Document Title 1",
        # "_type": "_doc",
        "_source": {
            "title": "Document Title 1",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        },
    },
    {
        "_op_type": "update",
        "_index": index_name,
        "_id": "Document Title 1",
        "doc": {
            "title": "Document Title Update",
        },
    },
    # {
    #     "_op_type": "delete",
    #     "_index": index_name,
    #     "_id": "Document Title N",
    # },
    # {
    #     "_op_type": "delete",
    #     "_index": index_name,
    #     "_id": "uVMRvZIBXpbTCfYW_wH9",
    # },
]
# es.bulk(operations=operations)
# result = helpers.bulk(es, actions=operations)
###
### to create example
# result = helpers.bulk(es, actions=operations)
# print(result)

# print(dir(es))

# After delete:
# BulkIndexError(f"{len(errors)} document(s) failed to index.", errors)
# elasticsearch.helpers.BulkIndexError: 1 document(s) failed to index.

# get_result = es.get(index=index_name, id="Document Title N")
# # get_result = dict(es.get(index=index_name, id="Document Title N"))
# print(get_result, type(get_result), sep="\n")

##############

# # Пустой поисковый запрос, возвращающий все документы
# query = {"query": {"match_all": {}}}

# # Получение всех документов из индекса
# response = es.search(index=index_name, body=query)

# # Извлечение результатов
# hits = response["hits"]["hits"]

# # Вывод результатов
# for hit in hits:
#     print(hit["_source"])

#############
### Delete an index

# if es.indices.exists(index=index_name):
#     es.indices.delete(index=index_name)
#     print(f"Индекс {index_name} удален.")
# else:
#     print(f"Индекс {index_name} не существует.")

#############
### Delete docs of index

# # Запрос на удаление всех документов
# query = {"query": {"match_all": {}}}

# # Выполняем очистку индекса
# result = es.delete_by_query(index=index_name, body=query)
# print(result)

################
### get mapping
resp1 = es.indices.get_mapping(
    index="movies",
)
print(resp1)

#################
#################
#################
index_name = "movies"

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Индекс {index_name} удален.")
else:
    print(f"Индекс {index_name} не существует.")

####################
####################
####################
### get document

index_name = "movies"

# Идентификатор документа
document_id = UUID("0d79e7f3-842f-4006-aa1c-f18e4e76abbe")

# Получаем документ
try:
    result = es.get(index=index_name, id=document_id)
    print("Документ:", result["_source"])
except elasticsearch.exceptions.NotFoundError as e:
    print(f"Документ с ID {document_id} не найден.")
