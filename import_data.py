from elasticsearch import Elasticsearch
import requests, traceback, json
from datetime import datetime
import time

HOST_NAME = 'localhost'
PORT_NB = 9200
INDEX_NAME = 'annonces'
DOC_TYPE = 'annonce'
MAPPING_FILEPATH = 'mapping_offres.json'

es = Elasticsearch([{'host': HOST_NAME, 'port': PORT_NB}])
mots_cles = ['securite', 'informatique', 'audit']
imports_ids = set()

def init():
    with open(MAPPING_FILEPATH) as f:
        mapping = json.load(f)

    es.indices.create(index=INDEX_NAME, body=mapping)

def get_imports():
    for s in mots_cles:
        reponse = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=datefindiffusion%3E%3D20180527%20' + s)
        annonces = reponse.json()["item"]
        for a in annonces:
            imports_ids.add(a['value'])

def get_daily_imports():
    for s in mots_cles:
        reponse = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=dateparution%3E%3D'+currentDate()+'%20' + s)
        if reponse.status_code == 200:
            annonces = reponse.json()["item"]
            for a in annonces:
                imports_ids.add(a['value'])
    import_in_es()

def currentDate():
    return datetime.today().strftime("%Y%m%d")

def import_in_es():
    try:
        for id_annonce in imports_ids:
            annonce = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/v230/' + id_annonce)
            if annonce.status_code == 200:
                f = annonce.json()
                f["input"] = {"actif": True, "commentaire": " ", "nom": " "}
                es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=f)
            else:
                print("Non importé : " + str(id_annonce))
    except:
        traceback.print_exc()
        print('ERREUR : ' + str(id_annonce))

def update_annonce(id_annonce, bool_rejet, str_nom, str_commentaire):
    id_es = search(id_annonce)
    if id_es:
        es.update(index=INDEX_NAME, doc_type=DOC_TYPE, id=id_es, body={"doc": {"input": {"actif": bool_rejet, "commentaire": str_commentaire, "nom": str_nom}}})

#retourne l'id elasticsearch correspondant à un id d'annonce
def search(id_annonce):
    query = json.dumps({
        "query": {
            "term": {
                "gestion.reference.idweb": id_annonce
            }
        }
    })
    result = es.search(index=INDEX_NAME, body=query)
    if result["hits"]["total"] > 0:
        id_es = result["hits"]["hits"][0]["_id"]
    else:
        return None
    return id_es

def delete_outdated():    
    query = json.dumps({
        "query": {
            "match_all": {}
        }
    })
    result = es.search(index=INDEX_NAME, body=query)
    for annonce in result["hits"]["hits"]:
        
        datefindiffusion = annonce["_source"]["gestion"]["indexation"]["datefindiffusion"]
        milli_sec = int(round(time.time() * 1000))

        if datefindiffusion < milli_sec:
            id_es = annonce["_id"]
            es.delete(index=INDEX_NAME, doc_type=DOC_TYPE, id=id_es)

if __name__ == '__main__':
    init()
    get_imports()
    import_in_es()