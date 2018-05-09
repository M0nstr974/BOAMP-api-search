import requests
import json
from elasticsearch import Elasticsearch

curseur = 1
id_offre = 1

response = requests.get("http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=securite&curseur=" + str(curseur)).json()

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
for offre in response["item"]:
    es.index(index='offres', doc_type='offre', id=id_offre, body=offre)
    id_offre += 1

""" while True:
    response = requests.get("http://api.dila.fr/opendata/api-boamp/annonces/search?criterion=securite&curseur=" + str(curseur))
    if response.status_code != 200:
        break
    else:
        items.update(response.json())
    curseur += 1000 """

