#!/usr/bin/env python
#-*- coding:utf-8 -*-

import json
import traceback
import threading
import msgpack
import pymongo
from bson import ObjectId
from langs import enum, ctime, StatNameSpace

CACHETYPE = enum("string", "hash", "list", "set")
NAME="dal"
REDIS_STAT_NAME="redis"

"""
@author xiejueheng

Dal提供访问mongodb和redis的简便方法，不需捕捉任何异常 
代理访问redis方式：Dal.redis_proxy.strict_set(...)
简易访问mongodb方式：Dal.update, Dal.insert_if_absent
"""
class RedisProxy(object):
    def __init__(self, dal):
        self.dal = dal
        
    def generateKey(self, table, prefix="", query={}, sort=None, limit=None, name="tablecache", criteria=None, pack=True):
        key = "%s_%s" %(name,table)
        
        if prefix:
            key = "%s_%s" %(key, prefix) 
        
        if query:
            key = key + "_" + "_".join(sorted(["%s_%s" %(key,value) for key,value in query.iteritems()], key=lambda a:a))
        
        if sort and isinstance(sort, tuple):
            key = key + "_$sort_" + "_".join([ "%s" %val for val in sort])
        
        if criteria:
            key = key + "_$criteria_" + "_".join(sorted(["%s_%s" %(key,value) for key,value in criteria.iteritems()], key=lambda a:a))

        if limit:
            key = key + "_$limit_%s" %(limit)

        if pack:
            key = key +"_$pack_1"
        else:
            key = key +"_$pack_0"
            
        return key
    
    @ctime(REDIS_STAT_NAME)
    def strict_set(self, key, value, cache_time=0):
        try:
            packb = msgpack.packb(value)
            result = self.dal.get_redis().set(key, packb)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_set]key=%s, value=%s, cache_time=%s, result=%s" %(key, value, cache_time, result))
            if cache_time:
                self.dal.get_redis().expire(key, cache_time)
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_set]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_get(self, key, pack=True):
        try:
            result = self.dal.get_redis().get(key)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_get]key=%s" %(key))
            if result:
                if pack:
                    return msgpack.unpackb(result, use_list = True)
                else:
                    return result
            else:
                return None
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_get]error, %s" %traceback.format_exc())
            return None
    
    @ctime(REDIS_STAT_NAME)
    def strict_setex(self, key, seconds, value, pack=True):
        try:
            if pack:
                value = msgpack.packb(value)
            result = self.dal.get_redis().setex(key,seconds,value)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_setex]key=%s, seconds=%s, result=%s" %(key, seconds, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_setex]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_setnx(self, key, value, cache_time=43200):
        try:
            result = self.dal.get_redis().setnx(key,value)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_setnx]key=%s,result=%s" %(key, result))
            if cache_time:
                self.dal.get_redis().expire(key, cache_time)
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_setnx]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_incr(self,key):
        try:
            result = self.dal.get_redis().incr(key)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_incr]key=%s, result=%s" %(key,result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_incr]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_incrby(self,key,increment):
        try:
            result = self.dal.get_redis().incr(key,increment)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_incrby]key=%s, increment=%s, result=%s" %(key,increment,result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_incrby]error, %s" %traceback.format_exc())

    @ctime(REDIS_STAT_NAME)
    def set(self, table, prefix="", value={}, query={}, cache_time=3600,sort=None,limit=None,cache_kw=None,criteria=None,pack=True):
        key = self.generateKey(table, prefix, query, sort=sort, limit=limit, criteria=criteria, pack=pack)
        
        try:
            if pack:
                result = self.dal.get_redis().set(key, msgpack.packb(value))
            else:
                result = self.dal.get_redis().set(key, json.dumps(value))

            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.set]key=%s, result=%s" %(key, result))
            if cache_time:
                self.dal.get_redis().expire(key, cache_time)
            if cache_kw:
                self.dal.cacheKeyword(key,query,cache_kw)
        except Exception:
            self.dal.logger.error("[RedisProxy.set]error, %s" %traceback.format_exc())

    @ctime(REDIS_STAT_NAME)
    def get(self, table, prefix="", query={}, cache_time=0, sort=None, limit=None, criteria=None, pack=True):
        key = self.generateKey(table, prefix, query, sort=sort, limit=limit, criteria=criteria, pack=pack)
        status = None
        result = None
        try:
            status = self.dal.get_redis().exists(key)
            if status:
                result = self.dal.get_redis().get(key)
                if pack:
                    return msgpack.unpackb(result, use_list = True)
                elif result:
                    return json.loads(result, "UTF-8")
            return None
        except Exception:
            self.dal.logger.error("[RedisProxy.get]error, result=%s, %s" %(result, traceback.format_exc()))
            return None
        finally:
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.get]key=%s, status=%s" %(key, status))
    
    @ctime(REDIS_STAT_NAME)       
    def strict_lpush(self, key, value, prefix="", pack=True):
        if prefix:
            key = key + "_" + prefix
        try:
            if pack:
                value = msgpack.packb(value)
            result = self.dal.get_redis().lpush(key, value)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_lpush]key=%s,result=%s" %(key, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_lpush]error, %s" %traceback.format_exc()) 
    
    @ctime(REDIS_STAT_NAME)  
    def strict_lrange(self, key, start, stop, prefix="", pack=True):
        if prefix:
            key = key + "_" + prefix
        
        try:
            result = self.dal.get_redis().lrange(key, start, stop)
            if pack:
                return [msgpack.packb(r) for r in result]
            else:
                return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_lpush]error, %s" %traceback.format_exc()) 
    
    @ctime(REDIS_STAT_NAME)
    def strict_pipeline_sadd(self, key, sets, prefix="", pack=True):
        if prefix:
            key = key + "_" + prefix
        try:
            pipe_cmd = self.dal.get_redis().pipeline()
            for value in sets:
                if pack and value:
                    packb = msgpack.packb(value)
                else:
                    packb = value
                pipe_cmd.sadd(key, packb)
            pipe_cmd.execute()
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_pipeline_sadd]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_scard(self, key, prefix=""):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().scard(key)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_scard]key=%s,result=%s" %(key, result))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_scard]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_sismember(self, key, member, prefix=""):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().sismember(key, member)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_sismember]key=%s,result=%s" %(key, result))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_sismember]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_pipeline_hset(self, key, key_value_dict, prefix="", pack=True):
        if (not key_value_dict) or (len(key_value_dict) == 0):
            return
        
        if prefix:
            key = key + "_" + prefix
        
        try:
            pipe_cmd = self.dal.get_redis().pipeline()
            for hkey, value in key_value_dict.iteritems():
                if pack and value:
                    packb = msgpack.packb(value)
                else:
                    packb = value
                pipe_cmd.hset(key, hkey, packb)
            pipe_cmd.execute()
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_pipeline_hset]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_srem(self, key, member, prefix=""):
        if prefix:
            key = key + "_" + prefix
        try:
            if isinstance(member, list):
                result = self.dal.get_redis().srem(key, *member)
            else:
                result = self.dal.get_redis().srem(key, member)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_srem]key=%s,result=%s" %(key, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_srem]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_sadd(self, key, member, prefix="", pack=True, cache_time=0):
        if prefix:
            key = key + "_" + prefix
        try:
            if pack:
                packb = msgpack.packb(member)
            else:
                packb = member
            result = self.dal.get_redis().sadd(key, packb)  
            if cache_time:
                self.dal.get_redis().expire(key, cache_time)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_sadd]key=%s,result=%s" %(key, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_sadd]error, %s" %traceback.format_exc())
            
    @ctime(REDIS_STAT_NAME)
    def strict_sinter(self, key, prefix="", pack=True):
        if prefix:
            key = key + "_" + prefix
        try:
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_sinter]key=%s" %(key))
            result = self.dal.get_redis().sinter(key)
            if pack and result:
                return [ msgpack.unpackb(value, use_list = True) for value in result ]
            else:
                return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_sinter]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_zrange(self, key, start, stop, prefix="", withscores=False):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().zrange(key, start, stop, withscores=withscores)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_zrange]key=%s" %(key))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_zadd]error, %s" %traceback.format_exc())
         
    @ctime(REDIS_STAT_NAME)   
    def strict_zrevrange(self, key, start, stop, prefix="", withscores=False):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().zrevrange(key, start, stop, withscores=withscores)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_zreverange]key=%s" %(key))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_zreverange]error, %s" %traceback.format_exc())
         
    @ctime(REDIS_STAT_NAME)   
    def strict_zrangebyscore(self, key, start, stop, prefix="", withscores=False):
        if prefix:
            key = key + "_" + prefix
        try:
            result = self.dal.get_redis().zrangebyscore(key, start, stop, withscores=withscores)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_zrangebyscore]key=%s" %(key))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_zrangebyscore]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_zcard(self,key,prefix=""):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().zcard(key)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_zcard]key=%s, result=%s" %(key, result))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_zcard]error, %s" %traceback.format_exc())
            return 0
    
    @ctime(REDIS_STAT_NAME)
    def strict_zadd(self, key, value, score=0, prefix="",cache_time=0):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().zadd(key, score, value)
            if cache_time:
                self.dal.get_redis().expire(key, cache_time)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_zadd]key=%s, result=%s" %(key, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_zadd]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_pipeline_zadd(self, key, sets, prefix=""):
        if prefix:
            key = key + "_" + prefix
        try:
            pipe_cmd = self.dal.get_redis().pipeline()
            for score,value in sets:
                pipe_cmd.zadd(key,value,score)
            pipe_cmd.execute()
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_pipeline_zadd]error, %s" %traceback.format_exc())
        
    @ctime(REDIS_STAT_NAME)
    def strict_zrem(self, key, member, prefix=""):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().zrem(key, *member)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_zrem]key=%s, result=%s" %(key, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_zrem]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_hset(self, key, hkey, value, prefix="", pack=True,cache_time=0):
        if prefix:
            key = key + "_" + prefix
        try:
            if pack and value:
                packb = msgpack.packb(value)
            else:
                packb = value
                
            result = self.dal.get_redis().hset(key, hkey, packb)
            if cache_time:
                self.dal.get_redis().expire(key, cache_time)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_hset]key=%s, hkey=%s, result=%s" %(key, hkey, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_hset]error, %s" %traceback.format_exc())
          
    @ctime(REDIS_STAT_NAME)  
    def strict_hget(self, key, hkey, prefix="", pack=True):
        if prefix:
            key = key + "_" + prefix
        try:
            result = self.dal.get_redis().hget(key, hkey)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_hget]key=%s, hkey=%s" %(key, hkey))
            if pack and result:
                return msgpack.unpackb(result, use_list = True)
            else:
                return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_hget]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_hmget(self, key, hkeys, prefix="", pack=True):
        if prefix:
            key = key + "_" + prefix
        
        try:
            result = self.dal.get_redis().hmget(key, hkeys)
            if pack and result:
                return [msgpack.unpackb(value, use_list = True) for value in result if value]
            else:
                return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_hmget]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def strict_hdel(self, key, hkeys, prefix=""):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().hdel(key, hkeys)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_hdel]key=%s, result=%s" %(key, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_hdel]error, %s" %traceback.format_exc())
          
    @ctime(REDIS_STAT_NAME)  
    def strict_hkeys(self, key, prefix=""):
        if prefix:
            key = key + "_" + prefix
            
        try:
            result = self.dal.get_redis().hkeys(key)
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_hexists]error, %s" %traceback.format_exc())
            
    @ctime(REDIS_STAT_NAME)
    def strict_hexists(self, key, hkey, prefix=""):
        if prefix:
            key = key + "_" + prefix
        try:
            result = self.dal.get_redis().hexists(key, hkey)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_hexists]key=%s, hkey=%s, result=%s" %(key, hkey, result))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_hexists]error, %s" %traceback.format_exc())
            
    @ctime(REDIS_STAT_NAME)
    def strict_hincrby(self, key, hkey, prefix="", increment=1):
        if prefix:
            key = key + "_" + prefix
        try:
            result = self.dal.get_redis().hincrby(key, hkey)    
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.strict_hexists]key=%s, hkey=%s, result=%s" %(key, hkey, result))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.strict_hincrby]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def hashset(self, table, prefix="", value={}, query={}, cache_time=43200, hkey=None):
        key = self.generateKey(table, prefix, query, pack = False)
        try:
            #packb = msgpack.packb(value)
            result = self.dal.get_redis().hset(key, hkey, json.dumps(value))
            if cache_time:
                self.dal.get_redis().expire(key, cache_time)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.hashset]key=%s, hkey=%s, value=%s, result=%s" %(key, hkey, value, result))
        except Exception:
            self.dal.logger.error("[RedisProxy.hashset]error, %s" %traceback.format_exc())
            self.dal.get_redis().delete(key)
        
    @ctime(REDIS_STAT_NAME)
    def hashget(self, table, prefix="", query={}, cache_time=0, hkey=None):
        key = self.generateKey(table, prefix, query, pack = False)
        try:
            result = self.dal.get_redis().hget(key, hkey)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.hashget]key=%s, hkey=%s, result=%s" %(key, hkey, result))
                    
            if result:
                #return msgpack.unpackb(result, use_list = True)
                return json.loads(result)
            return None
        except Exception:
            self.dal.logger.error("[RedisProxy.hashget]error, %s" %traceback.format_exc())
            self.dal.get_redis().delete(key)

    @ctime(REDIS_STAT_NAME)
    def hash_get_all(self, table, prefix=""):
        key = self.generateKey(table, prefix, {}, pack = False)
        try:
            key_exists = self.dal.get_redis().exists(key)
            if not key_exists:
                return None
            members = self.dal.get_redis().hkeys(key)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.hash_get_all]table=%s, prefix=%s" %(table, prefix))

            result = []
            for hash_field in members:
                result_str = self.dal.get_redis().hget(key, hash_field)

                if result_str:
                    result.append(json.loads(result_str))
        except Exception, e:
            self.dal.get_redis().delete(key)
            self.dal.logger.error("[RedisProxy.hash_get_all] error, %s, bt:%s" %(e, traceback.format_exc()))
            return None

        return result

    @ctime(REDIS_STAT_NAME)
    def hashdel(self, table, prefix="", hkey=None):
        key = self.generateKey(table, prefix, {}, pack = False)
        try:
            key_exists = self.dal.get_redis().exists(key)
            if key_exists:
                type_str = self.dal.get_redis().type(key)
                if 'hash' != type_str:
                    self.dal.logger.error('[RedisProxy.hashdel]key=%s, prefix=%s,hkey=%s, not hash_type(%s)'
                        %(key, prefix, hkey, type_str))
                    return

            self.dal.get_redis().hdel(key, hkey)
        except Exception, e:
            self.dal.logger.error('hash del error')
        
    def write_by_cache_type(self, cache_type, *params, **dict_params):
        if CACHETYPE.string == cache_type:
            self.set(*params, **dict_params)
        elif CACHETYPE.hash == cache_type:
            self.hashset(*params, **dict_params)
            
    def read_by_cache_type(self, cache_type, *params, **dict_params):
        if CACHETYPE.string == cache_type:
            return self.get(*params, **dict_params)
        elif CACHETYPE.hash == cache_type:
            return self.hashget(*params, **dict_params)
    
    def clearCache(self, table, prefix="", query={}):
        key = self.generateKey(table, prefix, query)
        return self.clearCacheByKey(key)
    
    @ctime(REDIS_STAT_NAME)
    def exists(self, key, prefix=""):
        try:
            if prefix:
                key = key + "_" + prefix
            return self.dal.get_redis().exists(key)
        except Exception:
            self.dal.logger.error("[RedisProxy.exists]error, %s" %traceback.format_exc())   
    
    def keys(self, table, prefix="", query={}):
        try:
            key = "%s*" %(self.generateKey(table, prefix, query))
            result = self.dal.get_redis().keys(key)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.keys]key=%s, result=%s" %(key, result))
            return result
        except Exception:
            self.dal.logger.error("[RedisProxy.keys]error, %s" %traceback.format_exc())
    
    @ctime(REDIS_STAT_NAME)
    def delete(self, key, prefix=""):
        if prefix:
            key = key + "_" + prefix
        try:
            result = self.dal.get_redis().delete(key)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.delete]key=%s, result=%s" %(key, result))
            return True
        except Exception:
            self.dal.logger.error("[RedisProxy.delete] error, %s" %traceback.format_exc())
            return False

    def clearCacheByKey(self, *keys):
        try:
            result = self.dal.get_redis().delete(*keys)
            if self.dal.debug:
                self.dal.logger.debug("[RedisProxy.clearCacheByKey]keys=%s, result=%s" %(keys, result))
            return True
        except Exception:
            self.dal.logger.error("[RedisProxy.clearCacheByKey] error, %s" %traceback.format_exc())
            return False

class Dal(object):
    def __init__(self, redis_pool, mongodb_pool, logger, debug=True, pubsub=None, ddb_pool=None, reset_ddb_conn=None):
        self.redis_pool = redis_pool
        self.redis_proxy = RedisProxy(self)
        self.mongodb_pool = mongodb_pool
        self.redis_list = {}
        self.ddb_pool = ddb_pool
        self.reset_ddb_conn = reset_ddb_conn
        self.pubsub = pubsub 
        self.logger = logger
        self.debug = debug

    def add_redis(self,name,redisPool):
        self.redis_list[name] = redisPool

    def get_mongodb(self):
        num = 1
        if "." in threading.currentThread().name:
            num = threading.currentThread().name.split(".")[1]
        mongo_db = self.mongodb_pool.get_mongo_db(int(num))
        if None == mongo_db:
            raise Exception("Dal.getDBClient error,get mongodb from pool error ")    
        else:
            return mongo_db
        
    def get_redis(self,name=None):
        if name:
            redis_client = self.redis_list.get(name)
            if None == redis_client:
                self.logger.error("Dal.get_redis error,get %s redis_client from pool error " %name)    
                return None
            else:
                return redis_client

        num = 1
        if "." in threading.currentThread().name:
            num = threading.currentThread().name.split(".")[1]
        redis_client = self.redis_pool.get_redis(int(num))
        if None == redis_client:
            raise Exception("Dal.get_redis error,get redis_client from pool error ")    
        else:
            return redis_client
        
    def get_ddb(self):
        if not self.ddb_pool:
            raise Exception("Dal.ddb_client error,ddb_pool init failed")
        
        num = 1
        if "." in threading.currentThread().name:
            num = threading.currentThread().name.split(".")[1]
        ddb_client = self.ddb_pool.get(int(num))
        if None == ddb_client:
            raise Exception("Dal.ddb_client error,get ddb_client from pool error ")    
        else:
            if not ddb_client.IsConnAlive():
                if self.reset_ddb_conn:
                    self.reset_ddb_conn(num)
                    ddb_client = self.ddb_pool.get(int(num))
                else:
                    self.logger("get_ddb error: no reset_ddb_conn func")
            return ddb_client
        
    def pubsub_subscribe(self, *channels):
        self.logger.debug("[Dal.pubsub_subscribe]channels:%s" %channels)
        for c in channels:
            self.pubsub.subscribe(c)
    
    def pubsub_publish(self, key, msg, useJson=True):
        self.logger.debug("[Dal.pubsub_publish]key:%s" %(key))
        if key and msg:
            data = msg
            if useJson:
                data = msgpack.packb(msg)
            return self.get_redis().publish(key, data)
        return 0
    
    def pubsub_listen_message(self, useJson=True):
        self.logger.debug("[Dal.pubsub_listen_message]useJson:%s" %(useJson))
        result = self.pubsub.listen()
        if not result:
            return None, None
        if isinstance(result, dict):
            rtype = result.get("type")
            channel = result.get("channel")
            data  = result.get("data")
            if "message" == rtype:
                if useJson:
                    try:
                        data = msgpack.unpackb(data, use_list = True)
                    except Exception:
                        self.logger.error(traceback.format_exc())
                return (channel, data)
        return None, None
        
    
    @ctime(NAME)
    def update(self, table, prefix="", value={}, query={}, cache=True, cache_time=0, multi=False,upsert=True, cache_kw=None):
        try:
            result = self.get_mongodb()[table].update(query, value, multi=multi, upsert=upsert)
            if self.debug:
                    self.logger.debug("[Dal.update]table=%s, prefix=%s, value=%s, query=%s, cache=%s, cache_time=%s, multi=%s, result=%s" %(table, prefix, value, query, cache, cache_time, multi, result))
            if result and cache:
                self.redis_proxy.clearCache(table, prefix, query)
            if cache_kw:
                self.clearKwCache(cache_kw)
            return True
        except Exception:
            self.logger.error(traceback.format_exc())
            return False
        
    @ctime(NAME)
    def insert(self, table, prefix="", value={}, cache=True, cache_kw=None):
        try:
            result = self.get_mongodb()[table].insert(value)
            if self.debug:
                self.logger.debug("[Dal.insert]table=%s, prefix=%s, value=%s, cache=%s, result=%s" %(table, prefix, value, cache, result))
            if result and cache:
                self.clearCache(table, prefix)
            if cache_kw:
                self.clearKwCache(cache_kw)
            return True
        except Exception:
            self.logger.error(traceback.format_exc())
            return False
    
    @ctime(NAME)
    def insert_if_absent(self, table, prefix="", value={}, query={}, cache=True, cache_time=0, cache_kw=None):
        try:
            result = self.get_mongodb()[table].update(query, value, multi=False, upsert=True)
            if self.debug:
                self.logger.debug("[Dal.insert_if_absent]table=%s, prefix=%s, value=%s, query=%s, cache=%s, cache_time=%s, result=%s" %(table, prefix, value, query, cache, cache_time, result))
            if result and cache:
                self.clearCache(table, prefix)
            if cache_kw:
                self.clearKwCache(cache_kw)
            return True
        except Exception:
            self.logger.error(traceback.format_exc())
            return False
    
    @ctime(NAME)
    def find_one(self, table, prefix="", query={}, cache=True, cache_time=3600, criteria=None, cache_kw=None, pack=True):
        try:
            prefix = "find_one" if not prefix else "%s_find_one" %prefix
            if cache:
                result = self.redis_proxy.read_by_cache_type(CACHETYPE.string ,table, prefix, query, cache_time, criteria=criteria, pack=pack)
                if result is not None:
                    return result

            if criteria:
                result=self.get_mongodb()[table].find_one(query,criteria)
            else:
                result=self.get_mongodb()[table].find_one(query)

            if result and "_id" in result:
                result["_id"] = str(result.get("_id"))

            if cache:
                if self.debug:
                    self.logger.debug("[Dal.find_one]write_by_cache_type table=%s, prefix=%s, query=%s" %(table, prefix, query))
                
                self.redis_proxy.write_by_cache_type(CACHETYPE.string, table, prefix=prefix, value=result, query=query, cache_time=cache_time, criteria=criteria, cache_kw=cache_kw, pack=pack)
            
            return result
        except Exception, e:
            self.logger.error("[Dal.find_one]error: %s, bt: %s" %(e, traceback.format_exc()))
            return None
        finally:
            if self.debug:
                self.logger.debug("[Dal.find_one]finally table=%s, prefix=%s, query=%s, cache=%s, cache_time=%s" %(table, prefix, query, cache, cache_time))

    """
        Deprecated 方法已过时,逐步由nfind取缔
    """
    @ctime(NAME)
    def find(self, table, prefix="", query={}, cache=True, cache_time=43200, sort=None, criteria={"_id": 0}, limit=None, cache_kw=None):
        result = None
        try:
            if cache:
                result = self.redis_proxy.read_by_cache_type(CACHETYPE.string ,table, prefix, query, cache_time, sort=sort, limit=limit)
                if result is not None:
                    return result
            
            cursor = self.get_mongodb()[table].find(query)
            if sort:
                if isinstance(sort, tuple):
                    cursor = cursor.sort(*sort)
                else:
                    cursor = cursor.sort(sort)
                    
            if limit and limit > 0:
                cursor = cursor.limit(limit)
                
            result = list(cursor)
            
            id_flag = criteria.get("_id")
            if "_id" in criteria:
                del criteria["_id"]
            
            if 0<id_flag :
                for r in result:
                    r["_id"] = str(r.get("_id"))
                    for key, value in criteria.iteritems():
                        if value < 1:
                            del r[key]
            else:
                for r in result:
                    del r["_id"]
                    for key, value in criteria.iteritems():
                        if value < 1:
                            del r[key]
            
            if cache:
                if self.debug:
                    self.logger.debug("[Dal.find]write_by_cache_type table=%s, prefix=%s, query=%s" %(table, prefix, query))
                self.redis_proxy.write_by_cache_type(CACHETYPE.string, table, prefix=prefix, value=result, query=query, cache_time=cache_time,sort=sort,limit=limit,cache_kw=cache_kw)
            
            return result
        except Exception, e:
            self.logger.error("[Dal.find]error: %s, bt: %s" %(e, traceback.format_exc()))
            return None
        finally:
            if self.debug:
                self.logger.debug("[Dal.find]finally table=%s, prefix=%s, query=%s, cache=%s, cache_time=%s, criteria=%s" %(table, prefix, query, cache, cache_time, criteria))           

    """
        new find 遵循pymongo的查询规则,取代find
    """
    @ctime(NAME)
    def nfind(self, table, prefix="", query={}, cache=True, cache_time=43200, sort=None, criteria=None, limit=None, cache_kw=None, pack=True):
        result = None
        try:
            if cache:
                result = self.redis_proxy.read_by_cache_type(CACHETYPE.string ,table, prefix, query, cache_time, sort=sort, limit=limit, criteria=criteria, pack=pack)
                if result is not None:
                    return result
            
            if criteria:
                cursor = self.get_mongodb()[table].find(query,criteria)
            else:
                cursor = self.get_mongodb()[table].find(query)

            if sort:
                if isinstance(sort, tuple):
                    cursor = cursor.sort(*sort)
                else:
                    cursor = cursor.sort(sort)
                    
            if limit and limit > 0:
                cursor = cursor.limit(limit)
                
            result = list(cursor)
            
            for r in result:
                if "_id" in r:
                    r["_id"] = str(r.get("_id"))

            if cache:
                if self.debug:
                    self.logger.debug("[Dal.nfind]write_by_cache_type table=%s, prefix=%s, query=%s" %(table, prefix, query))

                self.redis_proxy.write_by_cache_type(CACHETYPE.string, table, prefix=prefix, value=result, query=query,criteria=criteria,cache_time=cache_time,sort=sort,limit=limit,cache_kw=cache_kw, pack=pack)
            
            return result
        except Exception, e:
            self.logger.error("[Dal.nfind]error: %s, bt: %s" %(e, traceback.format_exc()))
            return None
        finally:
            if self.debug:
                self.logger.debug("[Dal.find]finally table=%s, prefix=%s, query=%s, cache=%s, cache_time=%s, criteria=%s" %(table, prefix, query, cache, cache_time, criteria))
    
    @ctime(NAME)
    def delete(self, table, query, prefix="", cache=True, cache_type=CACHETYPE.string, cache_kw=None):
        try:
            result = self.get_mongodb()[table].remove(query)
            if self.debug:
                self.logger.debug("[Dal.delete]table=%s, query=%s, prefix=%s, cache=%s, cache_type=%s, result=%s" %(table, query, prefix, cache, cache_type, result))
            if result and cache:
                self.clearCache(table, prefix,query)
            if cache_kw:
                self.clearKwCache(cache_kw)
            return True
        except Exception:
            self.logger.error("[Dal.delete]error %s" %traceback.format_exc())
            return False

    #从table_name表中读取符合query条件的值,并以hash表的数据结构缓存到redis中.hash的key为query
    @ctime(NAME)
    def hash_get_one(self, table_name, prefix, query, fields = {"_id":0}, cache=True, reload=False):
        result = None
        try:
            if cache:
                if False == reload:
                    result = self.redis_proxy.hashget(table_name, prefix, query={}, hkey = '%s'%(query))
                    if result is not None:
                        return result

            result = self.get_mongodb()[table_name].find_one(query, fields)
            if result and '_id' in result:
                id = str(result.get('_id'))
                result.pop('_id', None)
                result['_id'] = id

            if cache and result is not None:
                self.check_utf8_dict(query)
                self.redis_proxy.hashset(table_name, query = {}, prefix = prefix, 
                    value = result, hkey = '%s'%(query))
        except Exception, e:
            self.logger.error("[Dal.hash_get_one]error %s, bt:%s" %(e,traceback.format_exc()))

        return result

    #删除一个hash的数据
    @ctime(NAME)
    def hash_del_one(self, table_name, query, prefix = ''):
        result = True
        try:
            #删除掉数据库中的值
            self.delete(table_name, query = query, prefix = prefix, cache = False)
            #默认值读取到缓存中
            self.hash_get_one(table_name, prefix = prefix, query = query, reload = True)
        except Exception, e:
            result = False

        return result

    @ctime(NAME)
    def hash_set_one(self, table_name, prefix, query, value, cache=True):
        try:
            update_fields = {}
            update_fields['$set'] = value
            db_ret = self.get_mongodb()[table_name].update(query, update_fields, upsert=True)
            if cache:
                self.check_utf8_dict(query)
                self.redis_proxy.hashdel(table_name, prefix, hkey='%s'%(query))
        except Exception, e:
            self.logger.error('[Dal.hash_set_one]error %s, bt: %s' %(e, traceback.format_exc()))
            return False

        return True

    
    #检查query中的str并将其转换成utf8 格式
    def check_utf8_dict(self, query):
        for k, v in query.iteritems():
            if isinstance(v, str):
                utf8_v = v.decode('utf-8')
                query[k] = utf8_v


    #将所有符合条件的数据以index_key作为hash_key读取出来,并添加到缓存中, 
    #index_key目前只支持一个,如果有必要再增加多个key
    @ctime(NAME)
    def hash_get_all(self, table_name, prefix, query, index_key, fields={"_id":0}, cache=True, reload=False, cache_time = 43200, cache_kw = None):
        result = []
        try:
            if cache:
                #如果要设置缓存
                if False == reload:
                    #读取缓存中的数据
                    redis_ret = self.redis_proxy.hash_get_all(table_name, prefix)
                    if redis_ret is not None:
                        return redis_ret 

            cursor = self.get_mongodb()[table_name].find(query, fields)
            for db_item in cursor:
                if '_id' in db_item:
                    id = str(db_item.get('_id'))
                    db_item.pop('_id', None)
                    db_item['_id'] = id

                if cache:
                    hkeyvalue = {}
                    if index_key not in db_item:
                        continue

                    hkeyvalue[index_key] = db_item[index_key]
                    self.redis_proxy.hashset(table_name, query={}, prefix = prefix,
                        value = db_item, cache_time = cache_time, hkey='%s'%(hkeyvalue))

                else:
                    pass                 

                result.append(db_item)

            hash_key = self.redis_proxy.generateKey(table_name, prefix = prefix, query = query, pack=False)
            if cache_time:
                self.get_redis().expire(hash_key, cache_time)
            if cache_kw:
                self.cacheKeyword(hash_key,{},cache_kw,prefix="")
        except Exception, e:
            self.logger.error('[Dal.hash_get_all] error %s, bt: %s' %(e, traceback.format_exc()))

        return result
    
    def get_range_by_page(self, page, count):
        if page <= 0:
            begin_i = 0
            end_i = -1
        else:
            begin_i = (page - 1) * count
            end_i = begin_i + count - 1
        
        return (begin_i, end_i)

    #--------------sorted-set部分---------------------------
    #获取全部列表
    @ctime(NAME)
    def get_sortedset_all(self, table, prefix="", query={}, fields={}, score_key="", reload=False):
        return None

    #更新一项
    @ctime(NAME)
    def update_sortedset_value(self, table, prefix="", query={}, value={}):
        return None

    @ctime(NAME)
    def remove_sortedset_value(self, table, prefix="", query={}, value={}, score_key=""):
        return None

    @ctime(NAME)
    def get_sortedset_rank(self, table, prefix="", query={}):
        return None

    #--------------sorted-set部分---------------------------

    #重新加载分页数据
    @ctime(NAME)
    def load_page_data(self, table, prefix="", query={}, cache_time=43200, sort=None, cache_kw=None, criteria=None):
        key = self.redis_proxy.generateKey(table, prefix, query, sort, name="pagecache", criteria=criteria, pack=True)
        fields = {"_id":1}
        if sort:
            fields[sort[0]] = 1
        if sort:
            sort_field =sort[0]
        else:
            sort_field = "sort_field"

        cursor = self.get_mongodb()[table].find(query,fields)
        if sort:
            if isinstance(sort, tuple):
                cursor = cursor.sort(*sort)
            else:
                cursor = cursor.sort(sort)
        
        sorted_id_result = list(cursor)
        for r in sorted_id_result:
            r["_id"] = str(r.get("_id"))
            if sort:
                score = r.get(sort_field, 0.0)
            else:
                score = 0

            if score:
                try:
                    score = float(score)
                except ValueError:
                    score = 0
            r[sort_field] = score
        
        z_id_score_list = [(r.get("_id"),0.0 if not sort else r.get(sort_field)) for r in sorted_id_result]
        self.redis_proxy.strict_pipeline_zadd(key, z_id_score_list)
        self.get_redis().expire(key, cache_time)
        if cache_kw:
            self.cacheKeyword(key,query,cache_kw)
        return sorted_id_result

    #分页获取表数据
    @ctime(NAME)
    def find_by_page(self, table, prefix="", query={}, cache_time=43200, sort=None, page=1, count=20, cache_kw=None, criteria=None):
        result = []
        total = 0
        page_count = 0
        current_count = 0
        try:
            sorted_id_result = None
            key = self.redis_proxy.generateKey(table, prefix, query, sort, name="pagecache", criteria=criteria)
            start, stop = self.get_range_by_page(page, count)
            
            if self.redis_proxy.exists(key):
                total = self.redis_proxy.strict_zcard(key)

                esc = True
                if sort and len(sort) > 1 and sort[1] < 0:
                    esc = False

                if esc:
                    sorted_id_result = self.redis_proxy.strict_zrange(key, start, stop, prefix)
                else:
                    sorted_id_result = self.redis_proxy.strict_zrevrange(key, start, stop, prefix)
            else:
                sorted_id_result = self.load_page_data(table, prefix, query, cache_time, sort, cache_kw=cache_kw, criteria=criteria)
                if cache_kw:
                    self.cacheKeyword(key,query,cache_kw)
                total = len(sorted_id_result)
                if count > 0:
                    sorted_id_result = [r.get("_id") for r in sorted_id_result][start:stop+1]
                else:
                    sorted_id_result = [r.get("_id") for r in sorted_id_result]

            current_count = len(sorted_id_result)
            if self.debug:
                self.logger.debug('[Dal.find_by_page] table=%s,prefix=%s,query=%s,sort=%s,page=%s,count=%s,criteria=%s' %(table,prefix,query,sort,page,count,criteria))
                
            if not sorted_id_result:
                return result,page_count,current_count,total
            
            page_count = len(sorted_id_result)
            for _id in sorted_id_result:
                item = self.find_one(table, query={"_id":ObjectId(_id)},criteria=criteria,cache=True,cache_time=300)
                find_one_key=self.redis_proxy.generateKey(table, "find_one",{"_id":_id},criteria=criteria,pack=True)
                self.cacheKeyword(find_one_key,{},cache_kw,prefix="")
                if item:
                    result.append(item)
            
            return result,page_count,current_count,total
        except Exception, e:
            self.logger.error('[Dal.find_by_page] error %s, bt: %s' %(e, traceback.format_exc()))
            return result,page_count,current_count,total

    @ctime(NAME)
    def clearCachesByKeys(self, table, prefix="", query={}):
        keys = self.redis_proxy.keys(table, prefix, query)
        if keys:
            self.redis_proxy.clearCacheByKey(*keys)

    @ctime(NAME)
    def clearCache(self, table, prefix="", query={}):
        self.redis_proxy.clearCache(table, prefix, query)

    #缓存关联的key集合
    def cacheKeyword(self,key,query,cache_kw,prefix=""):
        cache_time=86400
        if prefix is None:
            prefix = ""
        if cache_kw:
            if isinstance(cache_kw,(str,unicode)):
                vkey = query.get(cache_kw) if cache_kw in query else cache_kw
                cache_key = prefix+vkey
                self.redis_proxy.strict_sadd(cache_key,key,pack=False,cache_time=cache_time)
            elif isinstance(cache_kw,tuple):
                for kw in cache_kw:
                    vkey = query.get(cache_kw) if cache_kw in query else cache_kw
                    cache_key = prefix+vkey
                    self.redis_proxy.strict_sadd(cache_key,key,pack=False,cache_time=cache_time)
            elif isinstance(cache_kw,dict):
                for key,value in cache_kw.iteritems():
                    cache_key =  "%s%s_%s" %(prefix,key,value)
                    self.redis_proxy.strict_sadd(cache_key,key,pack=False,cache_time=cache_time)

    #清除关联的key集合
    @ctime(NAME)
    def clearKwCache(self,cache_kw,prefix=""):
        if not cache_kw:
            return
        if prefix is None:
            prefix = ""

        kw_list = []
        cachekey_list = []
        if isinstance(cache_kw,(str,unicode)):
            cache_key = prefix + cache_kw
            cachekey_list = self.redis_proxy.strict_sinter(cache_key,pack=False)
            kw_list.append(cache_key)
        elif isinstance(cache_kw,tuple):
            for kw in cache_kw:
                cache_key = prefix + kw
                cachekey_list = cachekey_list + self.redis_proxy.strict_sinter(cache_key,pack=False)
                kw_list.append(cache_key)
        elif isinstance(cache_kw,dict):
            for key,value in cache_kw.iteritems():
                cache_key = "%s%s_%s" %(prefix,key,value)
                cachekey_list = cachekey_list + self.redis_proxy.strict_sinter(cache_key,pack=False)
                kw_list.append(cache_key)

        if cachekey_list and kw_list:
            for key in cachekey_list:
                self.redis_proxy.delete(key)
            for kw in kw_list:
                self.redis_proxy.delete(kw)
            del cachekey_list
            del kw_list


    def get_stat(self,func):
        stat_infos = StatNameSpace.get_stat(NAME,prefix="dal")
        if func and stat_infos:
            func(stat_infos)
        if stat_infos:
            self.logger.info(stat_infos)

        stat_infos = StatNameSpace.get_stat(REDIS_STAT_NAME,prefix="redis")
        if func and stat_infos:
            func(stat_infos)
        if stat_infos:
            self.logger.info(stat_infos)

#测试代码
if '__main__' == __name__:
    import sys
    sys.path.append("../ptsvr/common")
    sys.path.append("../channel/common")
    sys.path.append("../lib")
    sys.path.append("../channel/common/protocol")
    
    from redispool import GREDISPOOL
    from mongodbpool import GMongoDBPOOL
    import utils

    redis_ip="192.168.11.42"
    redis_port=6379
    thread_num=3
    
    GREDISPOOL.start(redis_ip, int(redis_port), thread_num)
    
    mongo_ip = "192.168.11.42"
    mongo_port = 27017
    mongo_user = "gamble"
    mongo_pass = "gamblepwd"
    mongo_db   = "gamble"
    
    GMongoDBPOOL.start(mongo_ip,int(mongo_port), thread_num, mongo_db,mongo_user,mongo_pass)
    from asyncredis import AsyncPubSub
    pubsub = AsyncPubSub(redis_ip, int(redis_port))
    pubsub.start()

    #dal.pubsub_subscribe("first")
    #dal.redis_listen_message()
    
    #dal.update("test_table", value={"name": "aaa", "sn":"abc"}, query={"sn":"abc"}, cache=False)
    #dal.redis_proxy.strict_setnx("test", "xixi")
    #print dal.redis_proxy.strict_get("test",pack=False)
    table = "mobile_clan_gametype"
    #print dal.find_by_page(table, query=query, sort=("priority",pymongo.DESCENDING), page=1, count=2)
    dal = Dal(GREDISPOOL, GMongoDBPOOL, pubsub=pubsub, logger=utils.Log)
    #dal.clearKwCache("device_token121211212")
    #print dal.find_one("clan_info",query={"gametype" : 400}, criteria={"user_num":1,"clanname":1})
    print dal.find_by_page(table,criteria={"priority":1},sort=("priority",-1),page=0,count=0)
    """
    from server import MobileServerBase

    try:
        svr = MobileServerBase('test')
    except Exception, e:
        print 'Error: %s' %(e)

    dal = svr.dal 

    result = dal.find('mobile_record_gametype', query={'type':400})
    for ret_item in result:
        print 'ret_item: %s, type(%s)' %(ret_item, type(ret_item))

    result = dal.hash_get_one('mobile_record_gametype', prefix='hash', query={'type':400}, cache=True)
    print 'result: %s, type(%s)' %(result, type(result))

    result = dal.hash_set_one('mobile_record_gametype', prefix='hash',
        query={'type':400}, value = {'and_addr':'notcc.163.com'}, cache=True)
    print 'set result: %s' %(result)

    result = dal.hash_get_one('mobile_record_gametype', prefix='hash', query={'type':400})
    print 'after set! get result: %s, type %s' %(result, type(result))

    result = dal.hash_get_all('mobile_record_gametype', prefix='hash', query={}, index_key='type', reload=False)

    print 'dal.hash_get_all! result: %s' %(result)

    result = dal.find('mobile_record_gametype', query={'type':401})
    for ret_item in result:
        print 'ret_item: %s, type(%s)' %(ret_item, type(ret_item))
    """