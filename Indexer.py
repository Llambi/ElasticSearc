import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

INDEX_NAME = "dataset_tweets"
TYPE = "Tweet"


def makeSource(fileName):
    with open(fileName, "r", encoding="utf8") as rawData:
        for rawLine in rawData:
            data = json.loads(rawLine)
            yield data["_source"]


def makeDoc(fileName):
    for var in makeSource(fileName):
        yield {
            '_index': INDEX_NAME,
            '_type': TYPE,
            '_id': var['id_str'],
            '_source': var
        }


def indexNow(es, filename):
    print("INICIADO")
    resetIndex(es)
    ls = []
    count=0
    for var in makeDoc(filename):
        ls.append(var)
        if len(ls) >= 10000:
            load(es, ls)
            count+=1
            print("Bulk", count)
            ls = []
    load(es,ls)
    print("FINALIZADO.")


def createIndex(es):
    print(" Creando Index.")
    body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {}
    }
    es.indices.create(index=INDEX_NAME, body=body)


def deleteIndex(es):
    if es.indices.exists(INDEX_NAME):
        es.indices.delete(INDEX_NAME)
        print(" Borrado Index.")


def resetIndex(es):
    deleteIndex(es)
    createIndex(es)


def load(es, ls):
    parallel_bulk(es, ls, INDEX_NAME)


elastic = Elasticsearch()
indexNow(elastic, "2008-Feb-02-04.json")
