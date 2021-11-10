#!/usr/bin/env python
# coding: utf-8

# ## Phase II:
# #### The s3 folder system will be date wise for eg: “09-25-2021”, “09-26-2021”. (Future dates) [Manual]
# * Each folder will have data equivalent to 1 million
# * Create a cron job/scheduler/airflow that will read today’s date folder and index to Elasticsearch.* 
# * Create an API using python that receives the parameters called date and copies the given date folder to Elasticsearch. Eg: (http://localhost:8888/data-ingest?date=09-25-2021)
# 

import os

from smart_open import smart_open

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import date
from datetime import datetime
import boto3



def createIndex():
  # Elastic search configuation
  ENDPOINT="http://localhost:9200/"
  es=Elasticsearch(timeout=600,hosts=ENDPOINT)

  settings={
      "mappings" : {
        "properties" : {
          "brand" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "category_code" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "category_id" : {
            "type" : "long"
          },
          "event_time" : {
            "type" : "date"
          },
          "event_type" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "price" : {
            "type" : "float"
          },
          "product_id" : {
            "type" : "long"
          },
          "user_id" : {
            "type" : "long"
          },
          "user_session" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          }
        }
      }
    
  }



  e=es.indices.create(index='ecommerce',ignore=400,body=settings)
  print(e)

#defining schma for ecommerce
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
            '_id':line.get('category_id','')+"-"+line.get('product_id',''),
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

def impData():
  
  with open("/media/suyesh/01D7B522F84165F0/fuse/elasticSearch/secret.txt","r") as f:
    data = f.readlines()
    aws_key=data[0].strip("\n")
    aws_secret=data[1].strip("\n")


  client=boto3.client("s3",
                    aws_access_key_id=aws_key,
                    aws_secret_access_key=aws_secret,
                    region_name="us-west-1")



  today = date.today()

  # Current date time in local system
  date1=today.strftime("%-m-%-d-%Y")
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

  print(df.head())

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
