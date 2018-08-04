#!/usr/bin/env python

import sys
import socket
import threading
import time
import simplejson as json
import redis
import getopt
import requests
from pymongo import MongoClient
from bson import BSON
from struct import *

threading.stack_size(16*1024*1024)
production_dbs = ["local", "config", "admin"]

class ThreadedServer(object):
  def __init__(self, host, port,mip,mport):
    self.host = host
    self.port = port
    self.mongo_ip=mip
    self.mongo_port=mport
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind((self.host, self.port))
    self.redisconn = redis.Redis(host='127.0.0.1', port=6379, db=0)
    self.queue = 'print'

  def printer(self,message):
    self.redisconn.lpush(self.queue,message)

  def listen(self):
    self.sock.listen(128)
    while True:
      client, address = self.sock.accept()
      client.settimeout(3600)
      threading.Thread(target = self.listenToClient,args = (client,address)).start()

  def get_count(self,mongoclient,db,coll,query):
    database = mongoclient[db]
    collection = database[coll]
    return collection.find(query).count()

  def check_index(self,mongoclient,db,coll,query):
    if query == {}:
      return True
    database = mongoclient[db]
    collections = database.collection_names()
    if coll not in collections:
      return True
    collection = database[coll]
    indexes = collection.index_information()
    first_entries = []
    for index in indexes:
      first_entries.append(indexes[index]['key'][0][0])
    for param in query:
      if param in first_entries:
        return True
    return False

  def handle_req(self,code,data,address,my_address,mongoclient):
    if code == 2010:
      start = 16
      i = start
      # Get database
      for c in data[start:]:
        if c == '\0':
          break
        i = i + 1
      db = data[16:i]

      start = i + 1
      i = start

      for c in data[start:] :
        if c == '\0':
          break
        i = i + 1
      command = data[start:i]
      self.printer(command)
      if command in ['create']:
        pass
      elif command in ['find','count']:
        start = i + 1
        i = start
        numlen = unpack('<I', data[start:start+4])
        end = start + numlen[0]
        obj = BSON(data[start:end]).decode()
        start = end
        self.printer(obj)
        collection = obj[command]
        if command == 'find':
          query = (obj['filter'])
        elif command == 'count':
          query = (obj['query'])
        count = self.get_count(mongoclient,db,collection,{})
        if count > 1000000 and self.check_index(mongoclient,db,collection,query) == False:
          raise error('Client disconnected')
      elif 'drop' in command or 'delete' in command:
        start = i + 1
        i = start
        numlen = unpack('<I', data[start:start+4])
        end = start + numlen[0]
        obj = BSON(data[start:end]).decode()

        start = end
        numlen = unpack('<I', data[start:start+4])
        end = start + numlen[0]
        obj1 = BSON(data[start:end]).decode()

        #print repr(data[start:])
        if 'drop' in command:
          if 'drop' in obj:
            msg = 'Drop command issued on '+ my_address[0] + '/' + db + '.' + obj['drop'] + ' by '+ address[0]
            self.printer(msg)
          else:
            msg = 'Drop command issued on ' + my_address[0] + '/' + db + ' by '+ address[0]
            self.printer(msg)
          raise error('Client disconnected')
        if 'delete' in command:
          collection = obj['delete']
          query = obj['deletes'][0]['q']
          count = self.get_count(mongoclient,db,collection,query)
          msg = 'Remove command issued on '+ my_address[0] + '/' + db + '.' + collection + ' by '+ address[0]+ '\nQuery: ' +json.dumps(query)+' which will delete '+ str(count) + ' documents.'
          self.printer(msg)
          if count > 0:
            raise error('Client disconnected')
      elif 'insert' in command or 'update' in command:
        if db in production_dbs:
          raise error('Client disconnected')
    elif code == 2006:
      start = 20
      i = start
      # Get db.collection
      for c in data[start:]:
        if c == '\0':
          break
        i = i + 1
      identifier = data[20:i].split('.')
      db = identifier[0]
      collection = identifier[1]

      start = i + 5
      i = start
      numlen = unpack('<I', data[start:start+4])
      end = start + numlen[0]
      obj = BSON(data[start:end]).decode()
      count = self.get_count(mongoclient,db,collection,obj)
      msg = 'Remove command issued on '+ my_address[0] + '/' + db + '.' + collection + ' by '+ address[0]+ '\nQuery: ' +json.dumps(obj)+' which will delete '+ str(count) + ' documents.'
      self.printer(msg)
      if count > 0:
        raise error('Client disconnected')
    elif code == 2004:
      start = 20
      i = start
      # Get db.collection
      for c in data[start:]:
        if c == '\0':
          break
        i = i + 1
      identifier = data[20:i].split('.')
      db = identifier[0]
      collection = identifier[1]

      start = i + 9
      i = start
      numlen = unpack('<I', data[start:start+4])
      end = start + numlen[0]
      obj = BSON(data[start:end]).decode()
      self.printer(obj)
      if 'find' in obj or 'count' in obj:
        if 'find' in obj:
          query = {}
          if 'filter' in obj:
            query = (obj['filter'])
          collection = obj['find']
        elif 'count' in obj:
          query = (obj['query'])
          collection = obj['count']
        count = self.get_count(mongoclient,db,collection,{})
        if count > 1000000 and self.check_index(mongoclient,db,collection,query) == False:
          raise error('Client disconnected')
      if 'delete' in obj:
        self.printer('Delete in obj')
        query = obj['deletes'][0]['q']
        collection = obj['delete']
        count = self.get_count(mongoclient,db,collection,query)
        msg = 'Remove command issued on '+ my_address[0] + '/' + db + '.' + collection + ' by '+ address[0]+ '\nQuery: ' +json.dumps(query)+' which will delete '+ str(count) + ' documents.'
        self.printer(msg)
        if count > 0:
          raise error('Client disconnected')
      elif 'drop' in obj:
        msg = 'Drop command issued on '+ my_address[0] + '/' + db + '.' + obj['drop'] + ' by '+ address[0]
        self.printer(msg)
        raise error('Client disconnected')
      elif 'dropDatabase' in obj:
        msg = 'Drop command issued on ' + my_address[0] + '/' + db + ' by '+ address[0]
        self.printer(msg)
        raise error('Client disconnected')
      elif 'insert' in obj or 'update' in obj:
        if db in production_dbs:
          raise error('Client disconnected')
    elif code in [2001,2002]:
      start = 20
      i = start
      # Get db.collection
      for c in data[start:]:
        if c == '\0':
          break
        i = i + 1
      identifier = data[20:i].split('.')
      db = identifier[0]
      collection = identifier[1]
      start = i + 1
      obj = BSON(data[start:]).decode()

      if 'ns' in obj:
        identifier = obj['ns'].split('.')
        db = identifier[0]
        collection = identifier[1]
      self.printer(db)
      self.printer(collection)
      if db in production_dbs:
        raise error('Client disconnected')
    elif code in [2007]:
      raise error('Client disconnected')

  def listenToClient(self, client, address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (self.mongo_ip, self.mongo_port)
    sock.settimeout(3600)
    sock.connect(server_address)
    mongoclient = MongoClient(self.mongo_ip+':'+str(self.mongo_port))

    while True:
      try:
        part = client.recv(16)
        if part:
          header = unpack('IIII', part[0:16])
          total_size = header[0]
          data = part
          while(len(data) < total_size):
            self.printer('partial request')
            part = client.recv(total_size - len(data))
            if part:
              data = data + part
            else:
              raise error('Client disconnected')
          self.printer('Request'+str(header)+str(len(data)))
          self.handle_req(header[3],data,address,client.getsockname(),mongoclient)
          sock.sendall(data)

          # Deal with reply
          reply_part = sock.recv(16)
          sent = 0
          if reply_part:
            reply_header = unpack('IIII', reply_part[0:16])
            reply_size = reply_header[0]
            client.sendall(reply_part)
            reply_size = reply_size - len(reply_part)
            sent = sent + len(reply_part)
            while(reply_size > 0):
              self.printer('partial reply')
              reply_part = sock.recv(reply_size)
              if reply_part:
                reply_size = reply_size - len(reply_part)
                sent = sent + len(reply_part)
                client.sendall(reply_part)
              else:
                raise error('Server disconnected')
            self.printer('Reply'+str(reply_header)+str(sent))
          else:
            raise error('Server disconnected')
        else:
          raise error('Client disconnected')
      except Exception, e:
        self.printer(e)
        client.close()
        sock.close()
        mongoclient.close()
        return False


def main(argv):
  ip = '127.0.0.1'
  port = 27017
  listen_port = 10000
  try:
    opts, args=getopt.getopt(argv,"h:p:i:l:")
  except getopt.GetoptError:
    print('Usage: proxy.py [-i <ip>] [-p <port>] [-l <listen_port>]')
    sys.exit(1)
  for opt,arg in opts:
    if opt=='-h':
      print('Usage: proxy.py [-i <ip>] [-p <port>] [-l <listen_port>]')
      sys.exit(0)
    elif opt=='-p':
      port = int(arg)
    elif opt=='-i':
      ip = arg
    elif opt=='-l':
      listen_port = int(arg)
    if port>65535 or port<1024:
      print('Check port number')
      sys.exit(1)
    if listen_port>65535 or listen_port<1024:
      print('Check listen port number')
      sys.exit(1)
  ThreadedServer('',listen_port,ip,port).listen()

if __name__ == "__main__":
  main(sys.argv[1:])
