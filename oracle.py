#!/usr/bin/python
# coding:utf-8

"""
Author:Lijiacai
Email:1050518702@qq.com
===========================================
CopyRight@JackLee.com
===========================================
"""

import os
import sys
import json
from datetime import datetime

try:
    reload(sys)
    sys.setdefaultencoding("utf8")
except:
    pass
import logging
import cx_Oracle


class Oracle():
    def __init__(self, host="116.62.186.185", port="1521", service_name="ORCL", user=None, password=None):
        try:
            dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
            self.client = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
            self.cursor = self.client.cursor()
        except Exception as e:
            self.output(str(e))
            raise Exception("---Connnect Error---")

    def write(self, query=""):
        pass

    def read(self, size=None):
        try:
            return self.cursor.fetchmany(size)
        except Exception as e:
            self.output(str(e))

    def readall(self):
        try:
            return self.cursor.fetchall()
        except Exception as e:
            self.output(str(e))

    def execute(self, query="", args=None):
        try:
            self.cursor.execute(query=query, args=args)
        except Exception as e:
            self.output(str(e))

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def close(self):
        self.cursor.close()
        self.client.close()

    def output(self, args):
        logging.exception(str(args))


def test():
    host = "116.62.186.185"
    port = "1521"
    service_name = "ORCL"
    user = "devcq"
    password = "baIaGbnx9C"
    oracle = Oracle(host=host, port=port, service_name=service_name, user=user, password=password)


if __name__ == '__main__':
    test()
