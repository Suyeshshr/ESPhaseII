from flask import Flask, request, jsonify

from smart_open import smart_open
import pandas as pd
import boto3
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import date
from datetime import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
def ecommerce(df,i):
    for line in df:
        if(line.get('brand')== None):
            brand=None
        else:
            brand=line.get('brand')
            
        if(line.get('category_code')== None):
            category_code=None
        else:
            category_code=line.get('category_code')

        yield{
            '_index':'ecommerce',
            '_type':'_doc',
            '_id':str(line.get('category_id',''))+"-"+str(i),
            '_source':{
                'id':i,
                'timestamp':datetime.now(),
                'brand':brand,
                'category_id':line.get('category_id',''),
                'event_time':line.get('event_time',''),
                'event_type':line.get('event_type',''),
                'price':line.get('price',''),
                'product_id':line.get('product_id',''),
                'user_id':line.get('user_id',''),
                'user_session':line.get('user_session',''),
                'category_code':category_code

            }
        
            

        }
        i=i+1
    print(i)

@app.route('/uploadToElastic', methods=['POST'])
def upload():
    DateY  = request.json['DateY']
    DateM  = request.json['DateM']
    DateD  = request.json['DateD']
    with open("/media/suyesh/01D7B522F84165F0/fuse/elasticSearch/secret.txt","r") as f:
        data = f.readlines()
        aws_key=data[0].strip("\n")
        aws_secret=data[1].strip("\n")


    client=boto3.client("s3",
                        aws_access_key_id=aws_key,
                        aws_secret_access_key=aws_secret,
                        region_name="us-west-1")




    date1=DateM+"-"+DateD+"-"+DateY
    paginator=client.get_paginator('list_objects_v2')
    pages=paginator.paginate(Bucket='suyesh-es-task-26102021',Prefix=date1+'/')

    tmp=[]
    for page in pages:
        for obj in page['Contents']:
            if(obj['Key'].endswith('.json')):
                tmp.append(obj['Key'])


    bucket_name = 'suyesh-es-task-26102021'
    object_key =tmp[0]


    path = 's3://{}:{}@{}/{}'.format(aws_key, aws_secret, bucket_name, object_key)

    df = pd.read_json(smart_open(path), lines=True)



    # Elastic search configuation
    ENDPOINT="http://localhost:9200/"
    es=Elasticsearch(timeout=600,hosts=ENDPOINT)

    df = df.where(pd.notnull(df), None)
    documents = df.to_dict(orient='records')

    count=int(es.cat.count('ecommerce', params={"format": "json"})[0]['count'])
    body={
        "size": 1,
        "sort": {  
            "id": {
            "order": "desc"
                },
            "timestamp": {
            "order": "desc"
                        }
        }
    }
    if(count!=0):
        res = es.search(index="ecommerce", body=body)
        i=int(res['hits']['hits'][0]['_source']['id'])+1
    else:
        i=0


    # attempt to index the dictionary entries using the helpers.bulk() method
    try:
        print ("\nAttempting to index the list of docs using helpers.bulk()")

        # use the helpers library's Bulk API to index list of Elasticsearch docs
        resp = helpers.bulk(    es,    ecommerce(documents,i),    index = "ecommerce",    doc_type = "_doc")
        print("done")

    except Exception as err:
        print(err)
    return f"done {i}"

# Run Server
if __name__ == '__main__':
  app.run(debug=True)
