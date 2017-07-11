# -*- coding: utf-8 -*-

# value object to hold data, and be able to manipulate cache
class CacheProxy(object):

    def __init__(self, date, data):
        self.date = date
        self.data = data

    def get_data(self, *args, **kwargs): return self.data

    def last_modification_date_for_call(self, *args, **kwargs): return self.date # fixed date