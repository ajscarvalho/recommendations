# -*- coding: utf-8 -*-

import json


class GeneralKVS (object):


    def __init__(self, config, log, kvs, keyPrefix):

        self.config = config
        self.log = log
        self.kvs = kvs      # e.g. a redis instance        
        self.keyPrefix = keyPrefix # base key name which will prefix every key


    def get_key(self, itemId):
        return self.keyPrefix + ':' + itemId


    def get_item(self, itemId):
        strValue = self.kvs.getKeyValue(self.get_key(itemId))
        return json.loads(strValue)


    def set_item(self, itemId, obj):
        strValue = json.dumps(obj)
        self.redis.setKeyValue(self.get_key(itemId), strValue)
