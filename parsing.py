import requests
from elasticsearch import Elasticsearch
import traceback
import json

with open('mapping_offres.json') as f:
    mapping = json.load(f)

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es.indices.create(index='test-offres3', ignore=400, body=mapping)

max_offres = 1000
id_annonce = 1

response = requests.get("http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=securite")
curseur_limit = response.json()["nbItemsExistants"] - max_offres -1 
curseur = response.json()["nbItemsExistants"] - 1000 #response contient max 1000 annonces.

try:
    while curseur > curseur_limit:
        #pour chaque annonce, l'ajouter à l'index
        for offre in response.json()["item"]:
            annonce = requests.get('http://api.dila.fr/opendata/api-boamp/annonces/v230/' + offre["value"])
            id_offre = offre["value"]
            if annonce.status_code == 200:
                es.index(index='test-offres3', doc_type='annonce', id=id_annonce, body=annonce.json())
                id_annonce += 1
        curseur -= 1000
        response = requests.get("http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=securite&curseur=" + str(curseur))
except:
    traceback.print_exc()
    print('Error at \'id_offre\' = ' + str(id_offre) + '\n')
