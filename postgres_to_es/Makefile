install:
	pip install -r requirements.txt

run:
	python main.py

json-rm:
	rm data/data_state.json

es-movies-rm: json-rm
	curl -X DELETE http://localhost:9200/movies

query_all:
	curl -X GET http://localhost:9200/movies/_search 

