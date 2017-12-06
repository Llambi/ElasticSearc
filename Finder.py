# -*- coding: utf-8 -*-
import operator
from collections import Counter

import math
from nltk import word_tokenize, re
from nltk.corpus import stopwords
from elasticsearch import Elasticsearch

INDEX_NAME = "dataset_tweets"
TYPE = "Tweet"


def search(es, term, operator, size=0):
    """Simple Elasticsearch Query"""
    query = {
        "size": size,
        "query": {
            "match": {
                "text": {
                    "query": term,
                    "operator": operator
                }
            }
        }
    }
    response = es.search(index=INDEX_NAME, doc_type=TYPE, body=query)
    return response


def format_results(results, flag=False, count=-1):
    """Print results nicely:
    doc_id) content
    """
    data = [doc for doc in results['hits']['hits']]
    ls = []
    for doc in data:
        ls.append(str.lower(doc['_source']['text']))
        if (flag):
            print("%s) %s" % (doc['_id'], doc['_source']['text']))
        if count==0:
            print("----And More----")
            break
        count-=1
    return ls


def results_to_dicc(results):
    """Conver result to a diccionary"""
    dicc = {}
    for data in results:
        diccAux = clean_tweet(data)
        dicc = reduce2diccs(dicc, diccAux)
    return dicc


def reduce2diccs(dicc1, dicc2):
    """Reduce two diccionaries"""
    result = dicc1
    for key in dicc2:
        if key in result:
            result[key] += dicc2[key]
        else:
            result[key] = dicc2[key]
    return result


def clean_tweet(data):
    """Clean stop-words"""
    stopWords = stopwords.words('english')
    words = word_tokenize(data)
    wordsFiltered = []
    for w in words:
        w = re.sub(
            r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))''',
            '', w, flags=re.MULTILINE)
        w = re.sub(r'[^\w]', '', w)
        if w not in stopWords and w != '':
            wordsFiltered.append(w)
    return Counter(wordsFiltered)


def ngd(es, result, termX, flag=False):
    "devuelve un diccionario ordenado de las palabra relaccionadas con la busqueda y su importancia"
    dicc = {}
    x = result['hits']['total']
    palabrasRealccionadas = results_to_dicc(format_results(result))
    for key in palabrasRealccionadas:
        y = search(es, key, "or")['hits']['total']
        newTerm = key + " " + termX
        xy = search(es, newTerm, "and")['hits']['total']
        if xy != 0 and y != 0:
            n = (x + y - xy) * 100
            ngd = (max(math.log(x), math.log(y)) - math.log(xy)) / (math.log(n) - min(math.log(x), math.log(y)))
            dicc[key] = ngd
            if flag:
                print(key, " = ngd =", ngd)
    sorted_dicc = sorted(dicc.items(), key=operator.itemgetter(1))
    return dict(sorted_dicc)


elastic = Elasticsearch()
end = False
print("\nELASTIC SEARCH")
while not end:
    option = input("\nEscoge una opción:\n1. Ayuda\n2. Hacer una consulta\n3. Salir\n\n");
    if option == "1":
        print(
            "\nEscoge la opción 2 para realizar una consulta en ElasticSearch."
            "\nDebes proporcionar un término para consultar, así como el número de términos significativos que quieres emplear para expandir la consulta."
            "\nRecibirás el resultado de la consulta inicial, así como la lista de términos más signinficativos ordenados por su NGD y el resultado de la consulta expandida.")
    elif option == "2":
        term = input("\nEscoge un término para realizar la consulta: ")
        count = int(input("\nEscoge el número de términos significativos para expandir la consulta: "))
        res = search(elastic, term, "or", 10000)
        hitsIniciales=res['hits']['total']
        print("\nConsulta: " + term + "\n")
        format_results(res, True, 10)
        dicc = ngd(elastic, res, term)
        print("\n" + str(count) + " términos más relacionados con " + term + ":\n")
        terms=[]
        terms.append(term)
        for key in dicc:
            print("Término: %s\t NGD: %s" % (key, dicc[key]))
            terms.append(key)
            term = term + " " + key
            count = count - 1
            if count == 0:
                break
        res = search(elastic, term, "or", 10000)
        print("\nConsulta expandida: " + term + "\n")
        format_results(res, True, 10)
        keys = term.split(" ")
        print("\n\n\nConsulta inicial sin aumentar con el termino", terms[0], " ha dando el No. de hits =", hitsIniciales)
        print("Consulta inicial sin aumentar con el termino", terms[1:], " ha dando el No. de hits =", res['hits']['total'])

    elif option == "3":
        end = True
    else:
        print("\nPor favor, escoja una opción correcta (1, 2 o 3)\n")
