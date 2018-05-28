from elasticsearch import Elasticsearch
import requests
import json

INDEX_NAME = 'annonces'
MAPPING_FILEPATH = 'mapping_offres.json'

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
mots_cles = ['securite', 'informatique', 'audit']
imports_ids = set()

def init():
    with open(MAPPING_FILEPATH) as f:
        mapping = json.load(f)

    es.indices.create(index=INDEX_NAME, body=mapping)

def get_imports():
    for s in mots_cles:
        reponse = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=datefindiffusion%3E%3D20180527%20' + s) # >1000 items response not handled
        annonces = reponse.json()["item"]
        for a in annonces:
            imports_ids.add(a['value'])

def import_in_es():
    i = 1
    try:
        for id_annonce in imports_ids:
            annonce = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/v230/' + id_annonce)
            if annonce.status_code == 200:
                es.index(index=INDEX_NAME, doc_type='annonce', id=i, body=annonce.json())
                i += 1
            else:
                print(str(id_annonce))
    except:
        print('ERREUR : ' + str(id_annonce))
        



if __name__ == '__main__':
    init()
    get_imports()
    import_in_es()