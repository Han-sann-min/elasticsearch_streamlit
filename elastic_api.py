from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
client = Elasticsearch('http://localhost:9200')
## 일부 값 조회 가능
def search_index(index_name, field_name, match_name):
    s = Search(index=index_name).using(client).query(
        "query_string",
        query=f'{field_name}:*{match_name}*',
        default_field=field_name
    )
    print(s.to_dict())
    response = s.execute()
    return response
def search_index_with_date_range(index_name, field_name, match_name, start_date, end_date):
    s = Search(index=index_name).using(client).query("multi_match", fields=field_name, query=match_name)
    s = s.filter('range', timestamp={'gte': start_date, 'lte': end_date})
    response = s.execute()
    return response
######################################################## bank에 데이터 삽입
def create_data_in_bank(date_match_name, bank_match_name, branch_match_name, location_match_name, customers_match_name):
    INDEX = "bank"
    doc = {
        'date': date_match_name,
        'bank': bank_match_name,
        'branch': branch_match_name,
        'location': location_match_name,
        'customers': customers_match_name
    }
    resp = client.index(index=INDEX, id=(client.count(index="bank")['count'] + 1), document=doc) # id는 현재의 doc 데이터의 개수를 받아온 다음 +1 해서 지정
######################################################## bank의 데이터 id로 삭제
def delete_data_in_bank(will_be_deleted):
    resp = client.search(index="bank", size=100, query={"match_all": {}})
    dic = {hit["_id"]:True for hit in resp['hits']['hits']}
    if will_be_deleted in dic:
        resp = client.delete(index="bank", id=will_be_deleted)
        return True
    else:
        return False

def get_document_by_id(index_name, document_id):
    result = client.get(index=index_name, id=document_id)
    return result['_source']