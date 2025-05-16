#!/usr/bin/env python3

import random
import sys

from elasticsearch import Elasticsearch
import os
from datetime import datetime, timedelta
import hashlib
from pprint import pprint


def transform(data):
    """
    Transform the data. Examples:
    - update the lat/long for a site
    - tag a record as a ACCESS job
    Note: multiple transformations can be applied to a single record.
    """
    data['tags'] = []

    # Colorado
    if data['organization'] == 'COLORADO-AS':
        data['latitude'] = '40.0039'
        data['longitude'] = '-105.2669'
        data['geohash'] = '9xj5'

    # Kent
    if data['organization'] == 'OARNET-AS':
        data['latitude'] = '40.8670'
        data['longitude'] = '-81.4374'
        data['geohash'] = 'dpq4'

    # IU
    if data['organization'] == 'IU-RESEARCH':
        data['latitude'] = '39.1682'
        data['longitude'] = '-86.5230'
        data['geohash'] = 'dnfq'
        if 'jetstream' in data['subdomain'].lower():
            data['tags'].append('ACCESS')

    # LSUHEALTHSCIENCESCTR
    if data['organization'] == 'LSUHEALTHSCIENCESCTR':
        data['latitude'] = '29.967'
        data['longitude'] = '-90.053'
        data['geohash'] = '9wh0'

    # MGHPCC
    if data['organization'] == 'MGHPCC-AS':
        data['latitude'] = '42.2032'
        data['longitude'] = '-72.6254'
        data['geohash'] = 'drt2'

    # Merit
    if data['organization'] == 'MERIT-AS-14':
        data['latitude'] = '42.283'
        data['longitude'] = '-83.735'
        data['geohash'] = 'dps2'

    # NCSA
    if data['organization'] == 	'NCSA-AS':
        data['latitude'] = '40.1106'
        data['longitude'] = '-88.2283'        
        data['geohash'] = 'dp3w'
        data['tags'].append('ACCESS')

    # Optiputer
    if data['organization'] == 	'OPTIPUTER':
        data['latitude'] = '32.8801'
        data['longitude'] = '-117.2340'        
        data['geohash'] = '9mud'

    # Purdue
    if data['organization'] == 'PURDUE' or data['organization'] == 'PURDUE-RESEARCH':
        data['organization'] == 'PURDUE-RESEARCH'
        data['latitude'] = '40.434'
        data['longitude'] = '-86.929'
        data['geohash'] = 'dp4j'
        if 'rcac' in data['subdomain'].lower():
            data['tags'].append('ACCESS')

    # SDSC
    if data['organization'] == 	'SDSC-AS':
        if 'expanse' in data['subdomain'].lower():
            data['tags'].append('ACCESS')

    # U Arkansas
    if data['organization'] == 'UARK-FAYETTEVILLE':
        data['latitude'] = '36.0821'
        data['longitude'] = '-94.1718'
        data['geohash'] = '9ymj'

   # U Chicago
    if data['organization'] == 'U-CHICAGO-AS':
        data['latitude'] = '41.7897'
        data['longitude'] = '-87.5997'
        data['geohash'] = 'dp3t'

    # UW
    if data['organization'] == 'UW-RESEARCH':
        data['latitude'] = '47.659'
        data['longitude'] = '-122.305'
        data['geohash'] = 'c23p'

    # convert tags a string
    if data['tags']:
        data['tags'] = ','.join(sorted(data['tags']))
    else:
        data['tags'] = ''


def insert(data, day, es_client):
    """
    Insert the transformed data into the database.
    """
    
    ts = day.strftime('%Y-%m-%dT23:59:59')
    data['timestamp'] = ts

    ts_year = day.strftime('%Y')

    # calculate the id in a deterministic way
    id_string = f"{data['organization']}-{data['subdomain']}-{ts}"
    id = hashlib.sha256(id_string.encode('utf-8')).hexdigest()

    index_name = f"aggregated-job-locations-{ts_year}"
    es_client.index(index=index_name, id=id, document=data, request_timeout=180)


def process_day(day, es_client):
    
    dayplus1 = day + timedelta(days=1)
    body = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": day.strftime("%Y-%m-%dT00:00:00"),
                    "lt": dayplus1.strftime("%Y-%m-%dT00:00:00")
                }
            },
        },
        "aggs": {
            "job_count": {
                "multi_terms": {
                    "terms": [
                        {
                            "field": "organization.keyword"
                        },
                        {
                            "field": "subdomain.keyword"
                        }
                    ],
                    "size": 10000
                },
                "aggs": {
                    "hits": {
                        "top_hits": {
                            "_source": {
                                "includes": [
                                    "organization",
                                    "subdomain",
                                    "geohash",
                                    "latitude",
                                    "longitude",
                                ]
                            },
                            "size": 1
                        }
                    }
                }
            }
        },
        "size": 0  # Set size to 0 to only get aggregation results
    }
    resp = es_client.search(
        index="job-locations-*",
        body=body
    )
    print(f"\n\nJobs for {day.strftime('%Y-%m-%d')}: {resp['hits']['total']['value']}")
    
    # massage the data into simple records, and insert into new index
    if resp['aggregations']['job_count']['buckets']:
        
        for hit in resp['aggregations']['job_count']['buckets']:

            data = hit['hits']['hits']['hits'][0]['_source'].copy()
            data['job_count'] = hit['doc_count']

            # Skip if the organization is in the ignore list
            if data["organization"] in ["N/A", "ISI-AS"]:
                continue

            transform(data)

            pprint(data)
    
            insert(data, day, es_client)


if __name__ == "__main__":
    # Example usage
    es_url = os.getenv('ES_URL', 'localhost')
    es_user = os.getenv('ES_USER', None)
    es_password = os.getenv('ES_PASSWORD', None)

    es_client = Elasticsearch(
        es_url,
        basic_auth=(es_user, es_password)
    )
    
    if not es_client.ping():
        raise Exception(f"Elasticsearch cluster {es_url} is down!")
     
    # always process the last 5 days
    for i in range(5):
        day = datetime.now() - timedelta(days=i)
        process_day(day, es_client)
   
    # and a few random days in the past, to apply any changes to
    # the transformation function
    for i in range(30):
        days_ago = random.randint(5, 365*3)
        day = datetime.now() - timedelta(days=i*30)
        process_day(day, es_client)
    
   
    
    
