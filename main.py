#!/usr/bin/env python
#coding:utf-8

'''
This tool is used to recovery mongodb data from oplog, please filter out the data into a single file which is you want to recovery data from the entire oplog file. (Handle performace: 1000 records per second)
'''
import os
import sys
import json
from pymongo import MongoClient
from bson.objectid import ObjectId

def render(d):
    for i in d:
        if isinstance(d[i], dict):
            if len(d[i]) == 1:
                if "$oid" in d[i]:
                    d[i] = ObjectId(d[i]["$oid"])            
                elif "$numberLong" in d[i]:
                    d[i] = long(d[i]["$numberLong"])
                elif isinstance(data[i].values()[0], dict):
                    render(d[i])
            else:
                render(d[i])
                
if __name__ == "__main__":
    try:
        fn = sys.argv[1]
        dbhost = sys.argv[2]
        dbport = sys.argv[3]
        dbname = sys.argv[4]
        user = sys.argv[5]
        passwd = sys.argv[6]
        
        
        client = MongoClient(dbhost,int(dbport))
        db = client[dbname]
        db.authenticate(user, passwd,mechanism='SCRAM-SHA-1')
    except Exception,e:
        print "Exceptin: %s"%e
        print "%sUsage: python %s OPLOG_FILE.json  NEW_DB_HOST  NEW_DB_PORT  NEW_DB_NAME NEW_DB_USER NEW_DB_PASSWORD"%(os.linesep,sys.argv[0])
        print "%s"%__doc__
        sys.exit(1)
        
    with open(fn,'r') as f:
        for line in f:
            a = json.loads(line)
            if a['op'] == 'i':
                cn = a['ns'].split(".")[1]
                data = a['o']
                render(data)
                collection = db[cn]
                collection.save(data)
            elif a['op'] == 'd':
                cn = a['ns'].split(".")[1]
                data = a['o']
                render(data)
                collection = db[cn]
                collection.remove(data)
            elif a['op'] == 'u':
                cn = a['ns'].split(".")[1]
                query = a['o2']
                data = a['o']
                render(query)
                render(data)                
                print query
                print "-"*30
                print data
                collection = db[cn]
                collection.update(query,data,multi=True)
