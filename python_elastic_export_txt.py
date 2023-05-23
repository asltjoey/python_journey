__author__ = 'Prakasit.S'

from elasticsearch import Elasticsearch
import csv

joe = Elasticsearch('http://xxx:9200')

response = joe.search(
    index = ["logstash-2015.05.08"],
    scroll = '30s',
    search_type = 'scan',
    size = 5000,
    body = {
        "_source": [
            "_type",
            "event_type",
            "item_id",
            "timestamp",
            "log_type",
            "log_id",
            "host",
            "module",
            "system"
        ],
        "query": {
            "bool": {
                "must": [
                {
                    "term": {
                        "_type": "diag"
                    }
                },
                {
                    "term": {
                        "event_type": "insert"
                    }
                }
                ]
            }
        } #, "from": f, "size": s
    }
)

sample = []
scroll_size = response['hits']['total']
#resp = response['hits']['hits']

count = 0
while (scroll_size > 0):
    count += 1
    try:
        scroll_id = response['_scroll_id']
        response = joe.scroll(scroll_id=scroll_id, scroll='30s')
        sample += response['hits']['hits']
        scroll_size = len(response['hits']['hits'])
        print count
        print scroll_size
    except:
        break


with open('D:/pipe_check.txt', 'wb') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    filewriter.writerow(["event_type", "item_id", "timestamp", "log_id", "host", "module", "system"])
    for hit in sample:
        filewriter.writerow([hit["_source"]["event_type"], hit["_source"]["item_id"], hit["_source"]["timestamp"], hit["_source"]["log_id"], hit["_source"]["host"], hit["_source"]["module"], hit["_source"]["system"]])
