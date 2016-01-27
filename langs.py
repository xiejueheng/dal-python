#!/usr/bin/env python
#-*- coding:utf-8 -*-

import time

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

def safe_int(value, default=None):
    try:
        result = int(value)
        return result
    except Exception:
        return default

class StatNameSpace(object):
    OPT_TIMES="opt_times"
    OPT_COST ="opt_cost"
    name_stat = {}
    
    @classmethod
    def get_times(cls, name):
        return cls.name_stat.get(name, {}).get(StatNameSpace.OPT_TIMES, {})
    
    @classmethod
    def get_cost(cls, name):
        return cls.name_stat.get(name, {}).get(StatNameSpace.OPT_COST, {})
    
    @classmethod
    def get_stat(cls, name, prefix=""):
        name_statinfo = cls.name_stat.get(name)
        if not name_statinfo:
            return ""
        
        opt_times = name_statinfo.get(cls.OPT_TIMES)
        opt_cost = name_statinfo.get(cls.OPT_COST)
        handler_times = ["%s:%s" %(name, opt_times.get(name)) for name in opt_times.iterkeys()]
        handler_cost =  ["%s:%ss" %(name, opt_cost.get(name))  for name in opt_cost.iterkeys()]
        average_cost = ["%s:%ss" %(name, round(opt_cost.get(name, 1)/opt_times.get(name, 1), 3)) for name in opt_cost.iterkeys()]
        opt_cost.clear()
        opt_times.clear()
        return "STAT-%s-exec count:%s - cost:%s - average:%s" %(prefix, handler_times, handler_cost, average_cost)
    
def ctime(name):
    def _ctime(func):
        def __ctime(*args, **kwargs):
            #print("before %s called." % func.__name__)
            begin = time.time()
            ret = func(*args, **kwargs)
            end = time.time()
            opt_times = StatNameSpace.get_times(name)
            opt_cost = StatNameSpace.get_cost(name)
            
            n_times = opt_times.get(func.__name__)
            if n_times:
                opt_times[func.__name__] += 1
            else:
                opt_times[func.__name__] = 1
                
            n_cost = opt_cost.get(func.__name__)
            if n_cost:
                opt_cost[func.__name__] += round(end-begin, 3)
            else:
                opt_cost[func.__name__] = round(end-begin, 3)
            
            StatNameSpace.name_stat[name] = {StatNameSpace.OPT_COST: opt_cost, StatNameSpace.OPT_TIMES: opt_times}
            return ret
            #print("after %s called. result: %s, cost:%ss" % (func.__name__, ret, round(end - begin, 3)))
        return __ctime
    return _ctime
