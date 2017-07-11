# -*- coding: utf-8 -*-


from  .general_kvs import GeneralKVS


class UserProfileKVS (GeneralKVS):

    def __init__(self, config, log, kvs):
        super(UserProfileKVS, self).__init(config, log, kvs, 'user-profile')


    def get_change_set(self, userId, features, consumptionScore):
        
        profile = self.get_item(userId)

        for feature, score in features: #list of tuples
            profile[feature] += score * consumptionScore

        self.set_item(userId, profile)

