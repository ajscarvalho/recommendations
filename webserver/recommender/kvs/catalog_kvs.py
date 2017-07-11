# -*- coding: utf-8 -*-

import json


class CatalogKVS (object):


    def __init__(self, config, log, redis, keyName):

        self.config = config
        self.log = log
        self.redis = redis      # redis instance

        self.conn = self.redis  # redis instance or pipe
        
        self.keyName = keyName # base key name which will prefix every key


    def get_key(self, catalogType):
        return self.keyName + ':' + catalogType


    def get_catalog(self, catalogType):
        value = self.redis.getKeyValue(self.get_key(catalogType))
        return json.loads(value)

#TODO requires locks (write)
    def set_user_profile(self, userId, features):
        featuresAsStr = json.dumps(features)
        self.redis.setKeyValue(self.get_profile_key(userId), featuresAsStr)


    def get_change_set(self, userId, features, consumptionScore):
        
        profile = self.get_user_profile(userId)

        for feature, score in features: #list of tuples
            userFeatureScore = score * consumptionScore
            profile[feature] += userFeatureScore

        self.set_user_profile(userId, profile)

