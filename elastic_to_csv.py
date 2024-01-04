from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
import json
df = pd.read_csv("house.csv", encoding="cp949") #####경로만 바꾸기
json_str = df.to_json(orient='records')
json_records = json.loads(json_str)
es = Elasticsearch("http://localhost:9200", http_compress=True)
index_name = 'house' ##### 인덱스명바꾸기
doctype = 'house_price'
es.indices.delete(index=index_name, ignore=[400, 404])
es.indices.create(index=index_name, ignore=400)
action_list = []
a = 1
for row in json_records:
    record = {
        '_op_type': 'index',
        '_id': a,
        '_index': index_name,#이부분에 필드 추가
        '_source': row
    }
    action_list.append(record)
    a += 1
helpers.bulk(es, action_list)