__author__ = 'Prakasit.S'

from elasticsearch import Elasticsearch
import csv

joe = Elasticsearch('http://xxx:9200')

response = joe.search(
    index = ["logdata-2015.05.04", "logdata-2015.05.05", "logdata-2015.05.06", "logdata-2015.05.07", "logdata-2015.05.08",
             "logdata-2015.05.09", "logdata-2015.05.10", "logdata-2015.05.11", "logdata-2015.05.12"],
    scroll = '30s',
    search_type = 'scan',
    size = 1000,
    body = {
        "_source": [
            "item_id",
            "seller_id",
            "member_id",
            "txn_type",
            "device",
            "timestamp",
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "log_source": "contact"
                        }
                    },
                    {
                        "terms": {
                            "txn_type": [
                                            "email_click", "email_send", "email_cancel", 
                                            "phone_view", "phone_click", "phone_call", "phone_cancel",
                                            "sms_click", "sms_send", "sms_cancel",
                                            "line", "wechat"
                                        ]
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "gte": "2015/05/05",
                                "lt": "2015/05/12"
                            }
                        }
                    }
                ]
            }
        }
    }
)

sample = []
tscroll_size = response['hits']['total']
print '-------------------------'
print 'Total records = ' + str(tscroll_size)
print '-------------------------'


count = 0
xsum = 0
scroll_size = 1
while scroll_size > 0:
    count += 1
    try:
        scroll_id = response['_scroll_id']
        response = joe.scroll(scroll_id=scroll_id, scroll='30s')
        sample += response['hits']['hits']
        scroll_size = len(response['hits']['hits'])
        xsum += scroll_size
        print 'Round ' + str(count) + ' ; Number of records = ' + str(scroll_size) + ' ; Running total = ' + str(xsum) + ' ; Progress = ' + str(int(xsum*100/tscroll_size)) + '%'
    except:
        break


with open('D:/contact_last_7days.txt', 'wb') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    filewriter.writerow(["item_id", "seller_id", "member_id", "txn_type", "device", "timestamp"])
    for hit in sample:
        filewriter.writerow([hit["_source"]["item_id"], hit["_source"]["seller_id"], hit["_source"]["member_id"], hit["_source"]["txn_type"], hit["_source"]["device"], hit["_source"]["timestamp"]])
@asltjoey
 
Add headin
