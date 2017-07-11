# -*- coding: utf-8 -*-

import pandas as pd
import os #lock files

from .cache import CacheWrapper
from noshelper.get_char import get_char


class CsvCacheWrapper(CacheWrapper):

#    ENCODING = 'utf-8'

    def __init__(self, config, log, analysisName, sourceDataHandler):
        super(CsvCacheWrapper, self).__init__(config, log, analysisName, sourceDataHandler)

        self.csvConfig = {
            'encoding': config['cache.csv.defaultEncoding'],
            'sep': ',',
        }
        self.csvLoadConfig = {'na_filter': False}
        self.csvSaveConfig = {}
        self.filenameExtension = ".csv"

        #eventually set column types / converters while building the class

        self.alreadyNotified = False

        #can be applyed __init__ = chunking.add_after_init_behavior(__init__) for chunks
        self.init_chunk_writing_module()


    #rewritten for chunks
    def load_data(self, *args, **kwargs):
        """reads dataframe from csv file according to passed params"""
        if self.showNotifications: 
            self.log.info("reading " + self.analysisName + " data from csv")
           
        csvConfig = self.csvConfig.copy()
        csvConfig.update(self.csvLoadConfig)

        readWholeFile = self.chunkSize is None
        noChunkRead   = self.readCursor is None

#        if readWholeFile: prnt 'readWholeFile'
#        if   noChunkRead: prnt 'noChunkRead'        
#        prnt "load data", self.cacheFilename, csvConfig

        if readWholeFile or noChunkRead:

            try:
                readResult = pd.read_csv(self.cacheFilename, **csvConfig)
                
            except IOError as ioe:
                if not self.keepReferenceCopy: raise ioe
                self.log.warn("Daily Copy not found! Reading Reference Copy")    
                readResult = pd.read_csv(self._refCopyFilename(), **csvConfig)

            if readWholeFile:
#                prnt " -- Reading whole file --"
                return readResult
            else: 
                self.readCursor = readResult

#        prnt " ++ Reading chunk ++"

        return self.readCursor.get_chunk(self.chunkSize)


    #def save_data(self, dataframe):
     #   """will do some checks as to not save the dataframe if reading chunks"""
        #if self.csvLoadConfig.get('chunksize'): return # Do not save a read chunk on a full file

    #can be applyed write_data = chunking.pre_write_data(write_data) for chunks
    def write_data(self, dataframe):
        """ saves to csv format"""
        if self.readOnly: return self.readonly_fail()

        if self.chunkProcessing: return self.end_chunk_write()

        if dataframe is None or dataframe.empty: # error conditions - more readable this way - should raise?
            return self.log.warn('Trying to write empty dataframe for {}'.format(self.analysisName)) 

        csvConfig = self.csvConfig.copy()
        csvConfig.update(self.csvSaveConfig)

        dataframe.to_csv(self._tempFilename(), **csvConfig)
        self.post_write_file()


    def add_converters(self, converters):
        """adds converters to csv loading, e.g.
            cacheWrapper = CacheWrapper(...)
            cacheWrapper.add_converters({'EventDate': str})
        """
        if not converters: return
        
        if self.csvConfig.get('converters') is None: self.csvConfig['converters'] = {}
        self.csvConfig['converters'].update(converters)


    def import_converters(self): self.add_converters(self.sourceDataHandler.get_converters())


#region chunk writing

    #can be applyed fetch = chunking.pre_fetch_check(fetch) for chunks
    def checks_before_fetch(self):
        """default - all OK
            either we're not processing in chunks or if we are, make certain that the process completed (not locked).
        """
        #prnt ("+++ checks_before_fetch: ", self.analysisName, 
        #    '*   chunkProcessing', self.chunkProcessing, '*   IsLocked: ', self.is_locked(), "*")
        if not self.chunkProcessing: return True
        while self.is_locked():
            print ("File is locked for writing: {} - Action required: ".format(self.cacheFilename))
            print ("  - wait for concurrent process to finish.")
            print ("  - remove file and lock (and start over).")
            print ("  - just remove lock file (and use potencial incomplete results).")
            print ("Press any key to try again")
            get_char()

        return True


    #can be applyed fetch = chunking.post_fetch_processing(fetch) for chunks
    def process_data_after_fetch(self, data, readFromSource):
        """override the default - no processing, to check for chunk processing flag
        if chunkProcessing is on start the process"""
        #prnt ("process_data_after_fetch", self.analysisName, "readFromSource", readFromSource, "Chunk Processing", self.chunkProcessing)
        if readFromSource:
            #prnt "Process data after fetch - read from source"
#            if self.chunkProcessing: self.begin_chunk_write()
            return self.load_data() # force to read in chunks
        return data


    #can be applyed __init__ = chunking.add_after_init_behavior(__init__) for chunks
    def init_chunk_writing_module(self):
        #read in chunks
        self.chunkSize = None
        self.readCursor = None

        #Write in chunks
        self.chunkProcessing = False
        self.chunksWritten = 0


    def reset_chunk_reading(self): self.readCursor = None
    def reset_chunk_writing(self): self.chunksWritten = 0


    def set_reading_chunksize(self, rowCount):
        self.chunkSize = rowCount
        self.cacheDataFrameLocally = False
        self.csvLoadConfig['chunksize'] = rowCount


    def will_process_in_chunks(self):
        """Request to set up chunk writing. sourceDataHandler will need to know how to
                notify the beginning and end of the process and to write a chunk"""
        self.chunkProcessing = True
#        self.sourceDataHandler.begin_chunk_write    = self.begin_chunk_write
#        self.sourceDataHandler.end_chunk_write      = self.end_chunk_write
        self.sourceDataHandler.write_chunk          = self.write_chunk


    def begin_chunk_write(self):
        """Truncate File"""
        if self.readOnly: return self.readonly_fail()

        self.log.info("truncating " + self.analysisName + " processed csv")
        self.lock()
        target = open(self._tempFilename(), 'w')
        target.close()


    def write_chunk(self, dataframeChunk):
        #prnt "   * Writing a chunk of " + self.cacheFilename
        if self.readOnly: return self.readonly_fail()

        if dataframeChunk is None: return # stop condition

        if self.chunksWritten == 0: self.begin_chunk_write()

        #self.log.debug("writing chunk of " + self.alias + " dataframe to csv")
        csvConfig = self.csvConfig.copy()
        csvConfig.update(self.csvSaveConfig)
        #dataframe.to_csv(self.csvFilename, **csvConfig)
        csvConfig['index'] = False
        csvConfig['header'] = False
        csvConfig['mode'] = 'a' #append data to csv file
        #needed?    chunksize=chunksize)#size of data to append for each loop

        while True:
            try:
                dataframeChunk.to_csv(self._tempFilename(), **csvConfig)
                break

            except IOError:
                print ("\n\nError: could not write {}".format(self.cacheFilename))
                print ("\nPlease close file, if open. Press any key to try again")
                get_char()
        
        self.chunksWritten+= 1


    def end_chunk_write(self):
        self.post_write_file()
        self.unlock()

#end region chunk writing


#region locking

    def lockFilename(self): return self.cacheFilename + '.lock'
    def lock(self):
        """Creates a lock file"""
#        self.log.debug("*** Creating lock file: " + self.lockFilename())
        target = open(self.lockFilename(), 'w')
        target.close()


    def unlock(self):
        """removes a lock file"""
#        self.log.debug("*** Removing lock file: " + self.lockFilename())
        try:
            os.remove(self.lockFilename())
        except: pass
        

    def is_locked(self):
        """check that file exists"""
        return os.path.isfile(self.lockFilename())

#end region locking



# general chunk reading
    def get_data_chunk(self, *args, **kwargs):
        self.reset_chunk_reading()
        while True:
            try:
                data = self.get_data(*args, **kwargs)
                yield data
            except StopIteration: break

