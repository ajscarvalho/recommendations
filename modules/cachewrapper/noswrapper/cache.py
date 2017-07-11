# -*- coding: utf-8 -*-

import os
import shutil
import pandas as pd
import re

from datetime import datetime

from noshelper.filesystem import modification_date as file_mtime
from noshelper.get_char import wait_break_char as get_char, user_exit, is_user_exit


class CacheWrapper(object):

#    ENCODING = 'utf-8'
    unique_id_exclusion = re.compile('[|/\\:\*?"<>]')


    def __init__(self, config, log, analysisName, sourceDataHandler):
        self.config = config
        self.log = log
        self.analysisName = analysisName
        self.sourceDataHandler = sourceDataHandler

#        self.outsideDefinedFilename = False
        self.baseCacheFilename = os.path.join(config['paths.cachedData'], analysisName) #+ '.cache'
        self.cacheFilename = self.baseCacheFilename
        
        self.analysisCache = None
        self.lastCacheId = None
        self.usingSameCacheId = False
        self.filenameExtension = ".cache"
        print (config['cache'])
        self.cacheDataFrameLocally = config['cache.memory'].get(analysisName) # default: None == False

        self.showNotifications = True # could be better instead of multiple (if self.showNotifications: )
        self.dataGathererProcess = None
        self.readOnly = False # if read only no write operations are permitted

        self.keepReferenceCopy = False
        self.refCopyName = "reference"
        self.b_shouldReadFromSource = None # b_shouldReadFromSource - must be careful with this flag, only fix this value for chunk reading
        self.b_forceWrite = False


    def force_rewrite(self): 
        self.b_forceWrite = True


    def set_read_only(self): self.readOnly = True
    def set_data_gatherer(self, dataGatherer): self.dataGathererProcess = dataGatherer

    #deprecated    
    def set_cache_base_path(self, path): 
        self.baseCacheFilename = path
#        self.outsideDefinedFilename = True

    def generate_filename(self, *args, **kwargs):
        cacheId = self.unique_cache_id(*args, **kwargs)
#        if self.outsideDefinedFilename:
#            return os.path.join(self.baseCacheFilename, cacheId + self.filenameExtension)

        return os.path.join(self.baseCacheFilename, cacheId + self.filenameExtension)


    def last_modification_date_for_call(self, *args, **kwargs):
        """return input file last modification date, for different calls"""
        return file_mtime(self.generate_filename(*args, **kwargs))


    def last_modification_date(self):
        """return input file last modification date"""
        return file_mtime(self.cacheFilename)


    def source_modification_date(self, *args, **kwargs):
        """delegates last modification date to the source data"""
        return max(
            self.sourceDataHandler.last_modification_date_for_call(*args, **kwargs),
            self.last_modification_date_for_call(*args, **kwargs)
        )            


    def unique_cache_id(self, *args, **kwargs):
        cacheId = '_'
        for val in args:
            cacheId += str(val) + '_'

        for key, val in kwargs.items():
            cacheId += key + "=" + str(val) + '_'

        cacheId = self.unique_id_exclusion.sub('_', cacheId)
        return cacheId


    def get_data(self, *args, **kwargs):
        """gets dataframe from cache or from another source (abstracted by another object)"""

        cacheId = self.unique_cache_id(*args, **kwargs)
        self.usingSameCacheId = self.lastCacheId == cacheId
        if not self.usingSameCacheId: self.showNotifications = True #reset showNotifications

        # fetch from memory
        if self.analysisCache is not None and self.usingSameCacheId:
            if self.showNotifications: self.log.info("Cache Hit: returning " + self.analysisName + " cached dataframe")
            return self.analysisCache

        self.cacheFilename = self.generate_filename(*args, **kwargs)

        #fetch data from disk cache or from source
        fetchedData = self.fetch_data(*args, **kwargs)
        self.lastCacheId = cacheId # remember last cache Id

        if self.cacheDataFrameLocally: self.analysisCache = fetchedData
        #request from source
        return fetchedData


    def shouldReadFromSource(self, *args, **kwargs):
        if self.b_forceWrite: return True
        if self.usingSameCacheId and self.b_shouldReadFromSource is not None: return self.b_shouldReadFromSource

        # set up dependency (for later calling last_modification_date)
        #if self.sourceDataHandler: self.sourceDataHandler.prepare(*args, **kwargs)

        try:
            allwaysRun = self.config['cache']['allwaysRun'][self.analysisName]
        except:
            if self.showNotifications: self.log.warn("No config['cache']['allwaysRun']['" + self.analysisName + "'] configuration found: assuming True - No cache!")
            allwaysRun = True

        
        cacheLastMod = self.last_modification_date()  

        #print "cacheLastMod: ", cacheLastMod
        # if sourceDataHandler Exists - compare the dates (TODO abstract into a function)
        if self.sourceDataHandler:
            sourceModDate = self.sourceDataHandler.last_modification_date_for_call(*args, **kwargs)
            if self.showNotifications: self.log.debug("lastMods: " + str(sourceModDate) + " vs " + str(cacheLastMod) + " " + self.analysisName + " allwaysRun: " + ("True" if allwaysRun else "False"))

            # if source doesn't exist keep going if cache exists!
            # if sourceModDate == datetime.min: 
            #     if self.showNotifications: self.log.info("Source doesn't exist: " + self.sourceDataHandler.get_filename() + ". Regenerating...")
            #     allwaysRun = False

            if cacheLastMod == datetime.min:
                if self.showNotifications: self.log.info("Cache doesn't exist: " + self.cacheFilename + ". Running analysis")
                allwaysRun = True

            self.b_shouldReadFromSource = sourceModDate > cacheLastMod or allwaysRun
            return self.b_shouldReadFromSource

        #print "cacheLastMod", cacheLastMod, self.analysisName, self.cacheFilename
        if cacheLastMod != datetime.min: # if no sourceDataHandler exists - if a cache exists Use it
            self.b_shouldReadFromSource = False

        elif self.dataGathererProcess: # gather data, if a process is defined for it
            self.b_shouldReadFromSource = True

        elif self.keepReferenceCopy: # maybe there's a backup filename
            refCopyLastMod = file_mtime(self._refCopyFilename())
            if refCopyLastMod != datetime.min: # if no sourceDataHandler exists - if a cache exists Use it
                self.b_shouldReadFromSource = False

        if  self.b_shouldReadFromSource is None:
            raise Exception("No way to fetch data.\nPlease define either a Source Data Handler or a Data Gatherer Process")

        return self.b_shouldReadFromSource


    # import dataframe
    def fetch_data(self, *args, **kwargs):
        """if processed data is newer then source data, just read the cache, 
        otherwise read source"""
#        self.log.info("Fetching data for {}. ".format(self.analysisName))

        if not self.checks_before_fetch(): return None

        readFromSource = self.shouldReadFromSource(*args, **kwargs)
        if readFromSource:
            if self.showNotifications: self.log.debug("Source is Newer: re-analysing " + self.analysisName + " dataframe")
            data = self.fetch_and_save(*args, **kwargs)
        else:
            if self.showNotifications: self.log.debug("File Hit: Loading pre-analysed " + self.analysisName + " dataframe")
            data = self.load_data(*args, **kwargs)


        self.showNotifications = False # from this point on - no further notifications
        self.b_forceWrite = False # reset force flag
        return self.process_data_after_fetch(data, readFromSource) 


    def checks_before_fetch(self):
        """default - all OK"""
        return True


    def process_data_after_fetch(self, data, readFromSource):
        """default - no processing"""
        return data


    def load_data(self, *args, **kwargs):
        """reads dataframe from pickle file according to passed params"""
        if self.showNotifications: self.log.debug("reading " + self.analysisName + " data from cached file")
        try:
            return pd.read_pickle(self.cacheFilename)
        except:
            if self.keepReferenceCopy:
                try:
                    return pd.read_pickle(self._refCopyFilename())
                except: pass

            if self.showNotifications: self.log.warn("Failed loading from cache: re-analysing " + self.analysisName + " dataframe")
            return self.fetch_and_save(*args, **kwargs)


    def fetch_and_save(self, *args, **kwargs):
        self.log.debug("  * fetch and save {}".format(self.analysisName))
        if self.dataGathererProcess: # calls a process (synchronous)
            self.log.debug("    * Running DataGatherer for {}".format(self.analysisName))
            #prnt "Gathering Data Positional Params:", args, "Keyword Params", kwargs
            self.dataGathererProcess.set_params(*args, **kwargs)
            ok = self.dataGathererProcess.get_data()

            if is_user_exit(ok): user_exit()
            elif ok: # after gathering just load the file and return the data!
                self.b_shouldReadFromSource = None #reset flag - could also set to False...
                data = self.load_data(*args, **kwargs)
                return data 
            else: raise Exception("{} process failed".format(self.analysisName))

        elif self.sourceDataHandler: # handles it in the same process
            self.log.debug("    * Running Source Data Handler for {}".format(self.analysisName))
            data = self.sourceDataHandler.get_data(*args, **kwargs)
        else: # no alternative
            raise Exception("No way to fetch data.\nPlease define either a Source Data Handler or a Data Gatherer Process")

        self.save_data(data)
        return data


    # writes pickle
    def save_data(self, dataframe):
        """writes dataframe to file"""
        if self.readOnly: return self.readonly_fail()

        try:
            doPersistData = self.config['Persist'][self.analysisName]
        except:
            doPersistData = True # default allways persist computations

        if not doPersistData: 
            self.log.debug("Not Persisting " + self.analysisName + " dataframe to cache, according to configuration")
            return 
        
        self.log.debug("writing " + self.analysisName + " dataframe to cache")

        while True:
            try:
                self.write_data(dataframe)
                return
            except IOError as e:
                self.log.error("\n\nError: could not write {}".format(self.cacheFilename))
                self.log.error("\nPlease close file, if open. Press any key to try again") 
                #print e
                #TODO - interactive mode or shell mode... shell mode should just die
                get_char()


    def write_data(self, dataframe):
        """by default saves to pickle"""
        if self.readOnly: return self.readonly_fail()

        if dataframe is None or dataframe.empty: # error conditions - more readable this way - should raise?
            return self.log.warn('Trying to write empty dataframe for {}'.format(self.analysisName)) 

        dataframe.to_pickle(self._tempFilename())
        self.post_write_file()


    def _tempFilename(self, suf=''): return self.cacheFilename + ".temp" + suf
    def _refCopyFilename(self): return os.path.join(self.baseCacheFilename, self.refCopyName + self.filenameExtension) # Null cache id

    def post_write_file(self):
        tempFilename = self._tempFilename()
        
        if self.keepReferenceCopy:
            secTempFilename = self._tempFilename('2')
            shutil.copy(tempFilename, secTempFilename)
            shutil.move(secTempFilename, self._refCopyFilename())

        shutil.move(tempFilename, self.cacheFilename)


    def get_filename(self): return self.cacheFilename # TODO? ... pass to next class in line
    #def prepare(self, *args, **kwargs):
    #    if self.sourceDataHandler:
    #        return self.sourceDataHandler.prepare(*args, **kwargs)


    def readonly_fail(self):
        self.log.warning("CacheWrapper set to readonly mode. {} not saved").format(self.cacheFilename)
        return None