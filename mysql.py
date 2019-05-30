#!/usr/bin/python
# coding:utf-8

"""
Author:Lijiacai
Email:1050518702@qq.com
===========================================
CopyRight@Baidu.com.xxxxxx
===========================================
"""
import logging
import os
import sys
import json
import pymysql

try:
    reload(sys)
    sys.setdefaultencoding("utf8")
except:
    pass


class MysqlDB():
    """
    Mainly for batch read-write encapsulation of database, reduce the load of database
    http://mysql-python.sourceforge.net/MySQLdb.html#
    """

    def __init__(self, host=None, port=3306, user="root", password="123456", db="test",
                 cursorclass=pymysql.cursors.SSCursor):
        self.db = db
        try:
            self.client = pymysql.connect(host=host, port=port, passwd=password, user=user,
                                          cursorclass=cursorclass)
            self.cursor = self.client.cursor()
            self.create_database()
        except Exception as e:
            raise Exception("---Connect MysqlServer Error---")

    def create_database(self):
        self.cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % self.db)
        self.client.select_db(self.db)

    def create_table(self, sql=""):
        """
        create table
        :param sql: CREATE TABLE test (id int primary key,name varchar(30))
        :return:
        """
        try:
            self.cursor.execute(sql)
        except Exception as e:
            self.output(str(e))

    def write(self, query=""):
        pass

    def read(self, size=None):
        try:
            return self.cursor.fetchmany(size=size)
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

    def output(self, arg):
        # print(str(arg))
        logging.exception(str(arg))


def test():
    host = "52.82.8.156"
    port = 9906
    user = "root"
    password = "123456"
    db = "scrapy"
    mysql = MysqlDB(host=host, port=port, user=user, password=password, db=db)
    # sql = "CREATE TABLE test (id int primary key,name varchar(30))"
    # mysql.create_table(sql=sql)
    query = "select * from cars_count limit 2"
    mysql.execute(query=query)
    size = 1
    print(list(mysql.read(size=size)))
    print(json.loads(json.dumps(mysql.read(size=size), ensure_ascii=False).encode("utf8")))
    print(json.loads(json.dumps(mysql.read(size=size), ensure_ascii=False).encode("utf8")))


if __name__ == '__main__':
    test()
