import requests
import datetime
import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def run(elk_url):
	secciones_url = 'https://www.boletinoficial.gob.ar/secciones/secciones.json'
	response = requests.post(secciones_url,data={'nombreSeccion':'primera','subCat':'86','offset':'1','itemsPerPage':'500','fecha':'{:%Y%m%d}'.format(datetime.date.today())})
	print ("Hecho request de {} status code: {}".format(secciones_url,response.status_code))
	response_parsed_object = json.loads(str(response.text))
	esclient = Elasticsearch([elk_url])
	bulk_bodies = []
	for x in response_parsed_object['dataList'][0]:
		detalle_url = 'https://www.boletinoficial.gob.ar/norma/detallePrimera'
		aviso_response = requests.post(detalle_url,data={'numeroTramite':x['idTamite']})
		parsed = json.loads(str(aviso_response.text))
		print ("Hecho request de {} status code {}".format(detalle_url,aviso_response.status_code))
		print ("Procesando idTramite:{}".format(parsed['dataList']['idTramite']))
		body = {
		    "_index": "avisos_oficiales",
		    "_type": "aviso",
		    "_source": {
		        "detalleNorma":parsed['dataList']['detalleNorma'],
		        "id":parsed['id'],
		        "idTramite":parsed['dataList']['idTramite'],
		        "fechaPublicacion":parsed['dataList']['fechaPublicacion']
		        }
		}
		bulk_bodies.append(body)
	print("Iniciando bulking de elk a {}".format(elk_url))
	helpers.bulk(esclient,bulk_bodies)
	print("Bulking terminado")

if __name__ == "__main__":
	if len(sys.argv) < 2 :
		elk_url = "172.17.0.1:9200"
	else :
		elk_url = sys.argv[1]
	run(elk_url)