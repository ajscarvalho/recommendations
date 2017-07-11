# -*- coding: utf-8 -*-

from datetime import datetime

from  .general_kvs import GeneralKVS

from helper.rwlock import RWLock



epoch = datetime.utcfromtimestamp(0)
def current_unix_time(): return (datetime.now() - epoch).total_seconds()



class CatalogKVS (GeneralKVS):

# TO Config:
#    CATALOG_STALE_GRACE = 5*60 # 5 minutes between fetches
#    CATALOG_CACHE_TTL = 24*60*60*1.2 # 20% over a day for the catalog to be considered stale


    def __init__(self, config, log, kvs):
        super(CatalogKVS, self).__init(config, log, kvs, 'catalog')
        
        self.rwlocks = {}# [x] = RWLock()
        self.catalogCache = {}

        self.creationLock = RWLock()


    def time_key (self, catalogType): return 'ts-'    + catalogType
    def items_key(self, catalogType): return 'items-' + catalogType
     
    def post_catalog(self, catalogType, catalog):
        
        if not catalog: return # prevent accidental cleanup
        
        lock = self.fetch_lock(catalogType)
        lock.acquire_write()

        # write catalog and time, also keep in cache
        self.set_item( self.items_key(catalogType), catalog             )
        self.set_item( self.time_key (catalogType), current_unix_time() )
        self.catalogCache[catalogType] = catalog

        lock.release()
        

    def get_catalog(self, catalogType):        
        cachedCatalog = self.catalogCache.get(catalogType)
        if self.is_expired(catalogType) or cachedCatalog is None:
            return self.fetch_catalog()

        return cachedCatalog


    def is_expired(self, catalogType):
        catalogTS = self.get_item( self.time_key(catalogType) )
        currentTS = current_unix_time()
        return currentTS - catalogTS > self.config['CATALOG_CACHE_TTL']


    def calculate_grace_time(self):
        return current_unix_time() - self.config['CATALOG_CACHE_TTL'] + self.config['CATALOG_STALE_GRACE']


    def fetch_lock(catalogType):
        
        self.creationLock.acquire_write()

        lock = self.rwlocks.get(catalogType)
        if not lock:
            lock = RWLock()
            self.rwlocks[catalogType] = lock

        self.creationLock.release()
        return lock


    def fetch_catalog(self, catalogType):
        # aquire lock to fetch full catalog from kvs
        lock = self.fetch_lock(catalogType)
        lock.acquire_write()

        currentTime = current_unix_time()

        catalog     = self.get_item(self.items_key(catalogType))
        catalogTime = self.get_item(self.time_key (catalogType))
        
        if self.is_expired(catalogType) and catalog: # expired add some grace period
            self.set_item( self.time_key(catalogType), self.calculate_grace_time())
        
        #add to cache
        self.catalogCache[catalogType] = catalog

        lock.release()
        return catalog

