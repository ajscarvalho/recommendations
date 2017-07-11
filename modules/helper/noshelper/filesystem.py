# -*- coding: utf-8 -*-

import os, errno
import datetime

import re, os


# gets file modification time as datetime object
def modification_date(filename):
	try:	
		t = os.path.getmtime(filename)
		return datetime.datetime.fromtimestamp(t)
	except:
		return datetime.datetime.min


def create_dir(directoryName):
	try: 
		os.makedirs(directoryName)
	except OSError:
		if not os.path.isdir(directoryName):
			raise


def remove_file(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured


#### NOT IN USE - too slow for lots of files
def get_files_containing(path, matchString):
    files = os.listdir(path)
    matcher = re.compile("^{}".format(re.escape(matchString)))

    foundInFiles = []
    for f in files:
        file = os.path.join(path, f)
        text = open(file, "r")
        for line in text:
            if matcher.search(line):
                foundInFiles.append(f)
                #print f, line.split(',')[1]

    return foundInFiles
