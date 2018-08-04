#To pop print messages
import redis
import time

redisconn = redis.Redis(host='127.0.0.1', port=6379, db=0)
while True:
  val = redisconn.rpop('print')
  if val == None:
    time.sleep(0.1)
  else:
    print val
