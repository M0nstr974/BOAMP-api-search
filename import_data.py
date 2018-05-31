from datetime import datetime 
from elasticsearch import Elasticsearch
import requests
import json

INDEX_NAME = 'annonces'
MAPPING_FILEPATH = 'mapping_offres.json'
HOST_NAME = 'localhost'
HOST_PORT = 9200


es = Elasticsearch([{'host': HOST_NAME, 'port': HOST_PORT}])
mots_cles = ['securite', 'informatique', 'audit']
imports_ids = set()


def currentDate():
  return datetime.today().strftime("%Y%m%d")
   

def init():
    with open(MAPPING_FILEPATH) as f:
        mapping = json.load(f)

    es.indices.create(index=INDEX_NAME, body=mapping)

def get_imports():
    for s in mots_cles:
        reponse = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=datefindiffusion%3E%3D'+currentDate()+'%20' + s) # >1000 items response not handled
        annonces = reponse.json()["item"]
        for a in annonces:
            imports_ids.add(a['value'])
            
def get_daily_imports():
    for s in mots_cles:
        reponse = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=dateparution%3E%3D'+currentDate()+'%20' + s) # >1000 items response not handled
        annonces = reponse.json()["item"]
        for a in annonces:
            imports_ids.add(a['value'])

def import_in_es():
    i = 1
    try:
        for id_annonce in imports_ids:
            annonce = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/v230/' + id_annonce)
           if annonce.status_code == 200:
                f = annonce.json()
                f["input"] = {"actif": True, "commentaire": None, "nom": None}
                es.index(index=INDEX_NAME, doc_type='annonce', id=i, body=f)
            else:
                print(str(id_annonce))
    except:
        print('ERREUR : ' + str(id_annonce))
        



if __name__ == '__main__':
    init()
    get_imports()
    import_in_es()
