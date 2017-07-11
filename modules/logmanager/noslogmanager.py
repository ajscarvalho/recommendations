# -*- coding: utf-8 -*-

import logging


class LogManager:
	def __init__(self, config):
		self.log = None
		self.config = config
		self.prepare()

	def prepare(self):
		if self.log is not None: return # first use

		print ('  LogLevel:')
		print (self.config['log.default.level'])
		print()
		self.defaultLogLevel  = getattr(logging, self.config['log.default.level'])
		
		defaultLogFormat = self.config['log.default.format']
		self.defaultFormatter = logging.Formatter(defaultLogFormat)

		self.log = logging.getLogger(self.config['app.name'])
		self.log.setLevel(self.defaultLogLevel)

		if (self.log.handlers): return # do not add duplicate handlers 

		self.streamHandler = None
		if 'stream' in self.config['log'] and self.config['log.stream.enabled']: 
			self.streamHandler = logging.StreamHandler()
			self.streamHandler = self.set_log_handler(self.streamHandler, 'stream')
			

		self.fileHandler = None
		if "file" in self.config['log'] and self.config['log.file.enabled']: 
			self.fileHandler = logging.FileHandler(self.config['log.file.filename'], mode='a', encoding='utf-8')
			self.fileHandler = self.set_log_handler(self.fileHandler, 'file')


	def set_log_handler(self, handler, logType):

		logLevelDesc = self.config['log'][logType]['level']
		logLevel = getattr(logging, logLevelDesc) if logLevelDesc else self.defaultLogLevel
		
		logFormat = self.config['log'][logType]['format']
		formatter = logging.Formatter(logFormat) if logFormat else self.defaultFormatter

		handler.setLevel(logLevel)
		handler.setFormatter(formatter)

		self.log.addHandler(handler)

		return handler


	def get_log(self):
		return self.log


	def addTimestampToStreamLogging(self):
		formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
		self.streamHandler.setFormatter(formatter)
	


def set_log_file_template(config, fileTemplate, logLevel=None):
    """disables stream logging and associates a file for logging.
    fileTemplate str fully qualified filename with placeholder for date, e.g. '/var/log/log_fetcher_{}.log'"""

    logFilename = fileTemplate.format(datetime.now().strftime('%Y-%m-%d'))
    logFilename = os.path.join(config['paths.appLog'], logFilename)
    fileConfig = {'filename': logFilename}

    if logLevel: fileConfig['level'] = logLevel

    updLogConfig = {'log': {'file': fileConfig}}

    config.update(updLogConfig)

