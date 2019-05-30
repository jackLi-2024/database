#!/usr/bin/python
# coding:utf-8

"""
Author:Lijiacai
Email:1050518702@qq.com
===========================================
CopyRight@Baidu.com.xxxxxx
===========================================
"""

import os
import random
import sys
import json
import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class EsDB():
    """
    Mainly for batch read-write encapsulation of database, reduce the load of database
    """

    def __init__(self, cluster=None, index_name=None, schema_mapping=None, **kwargs):
        self.index_name = index_name
        self.cluster = cluster
        self.schema_mapping = schema_mapping
        try:
            self.client = Elasticsearch(cluster, **kwargs)
            if not self.index_exist():
                self.create_index(body=schema_mapping)
            else:
                print("Current index already exists(index name = %s)" % self.index_name)
        except Exception as e:
            self.output(str(e))

    def write(self, data):
        """
        data = [{
                "_index": "1234",
                "_type": "1234",
                "_id": 111,
                "_source": {
                    "1": 1
                    }
                }]
        :param data:
        :return:
        """
        try:
            helpers.bulk(self.client, actions=data)
        except Exception as e:
            self.output(str(e))

    def readByApi(self, query):
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html
        :param query:
        :return:
        """
        try:
            response = self.client.search(index=self.index_name, body=query)
            return response
        except Exception as e:
            self.output(str(e))

    def readByUri(self, query):
        """
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-uri-request.html
        :param query:
        :return:
        """
        try:
            response = requests.get(random.choice(self.cluster) + self.index_name + "/_search?%s" % query)
            return response.json()
        except Exception as e:
            self.output(str(e))

    def __del__(self):
        pass

    def close(self):
        pass

    def index_exist(self):
        return self.client.indices.exists(self.index_name)

    def create_index(self, body):
        self.client.indices.create(index=self.index_name, body=body)

    def delete_index(self):
        self.client.indices.delete(index=self.index_name)
        return True

    def output(self, arg):
        print(str(arg))


def test():
    cluster = ["http://106.12.217.41:9200/"]
    index_name = "1234"
    es = EsDB(cluster=cluster, index_name=index_name)
    data = [{
        "_index": "1234",
        "_type": "1234",
        "_id": 111,
        "_source": {
            "1": 1
        }
    }]
    # es.write(data)
    print(es.readByUri(query="q= 1:1"))
    query = {
        "query": {
            "term": {"1": 2}
        }
    }
    print(es.readByApi(query=query))


if __name__ == '__main__':
    test()
