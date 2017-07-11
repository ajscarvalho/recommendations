# -*- coding: utf-8 -*-

import sys
import argparse
import re


def fail(code, message):
    sys.stderr.write(message)
    sys.exit(code)


def usage(usageMessage):
    return '\nUsage: ' + get_cli_command() + ' ' + usageMessage + "\n\n" 


def get_cli_command():
    return sys.argv[0]


def get_cli_params(pos):
    return sys.argv[pos]


def assert_param_cardinality(cardinality, usageMessage):
    if len(sys.argv) < cardinality + 1: 
        fail(1, usage(usageMessage))



#region Command Line Argument parsing

class CommandLineInterfaceParser(object):

    DATE_FORMAT          = 'YYYY-MM-DD'
    DATETIME_FORMAT      = 'YYYY-MM-DD HH:MM:SS'
    DATETIME_HELP        = 'the {:5s} Date in {} format'
    INVALID_DATETIME_MSG = '{:5s} Date not in the format {}: {}'

    DATETIME_REGEXP = '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    DATE_REGEXP     = '^\d{4}-\d{2}-\d{2}$'


    def __init__(self, usageDescription):
        self.parser = argparse.ArgumentParser(usageDescription)
        self.parsedArguments = False
        self.arguments = None


    def add_day_interval_params(self):
        """adds two more positional params: start day and end day"""
        return self.add_period_params(suffix='Day', dtFmt=self.DATE_FORMAT)
        

    def add_period_params(self, suffix='Date', dtFmt=None):
        """adds two more positional params: start date and end date
            @param string suffix meta variable name suffix
            @param string dtFmt datetime format"""
        if self.parsedArguments: raise Exception("Cannot add params. Already parsed command line")

        if dtFmt is None: dtFmt=self.DATETIME_FORMAT
        self.parser.add_argument('start', metavar='Start' + suffix, type=str, help=self.__get_dt_help('Start', dtFmt))
        self.parser.add_argument('end',   metavar='End'   + suffix, type=str, help=self.__get_dt_help('End',   dtFmt))


    def add_optional_param(self, shortName=None, longName=None, metaVar=None, paramType=str, helpMsg=''):
        """adds two more positional params: start date and end date"""
        if self.parsedArguments: raise Exception("Cannot add params. Already parsed command line")
        if not shortName and not longName: raise Exception("Must specify either a short or long name")

        if not shortName: shortName = str(longName )[0]
        if not longName:  longName  = str(shortName)
        if not metaVar: metaVar = longName

        shortName = '-'  + shortName
        longName  = '--' +  longName

        self.parser.add_argument(shortName, longName, metavar=metaVar, type=paramType, help=helpMsg, required=False)


    def add_required_param(self, name=None, paramType=str, helpMsg=''):
        """adds a required positional param: """
        if self.parsedArguments: raise Exception("Cannot add params. Already parsed command line")
        if not name: raise Exception("Must specify either a short or long name")

        self.parser.add_argument(name, metavar=name, type=paramType, help=helpMsg)


    def parse_args(self):
        if self.parsedArguments: return
        self.arguments = self.parser.parse_args()
        self.parsedArguments = True


    def get_period_params(self, dtFormat=None, datetimeRegExp=None):
        if dtFormat       is None: dtFormat       = self.DATETIME_FORMAT
        if datetimeRegExp is None: datetimeRegExp = self.DATETIME_REGEXP

        self.parse_args()
        startDateStr = self.arguments.start
        endDateStr   = self.arguments.  end

        datetimeMatch  = re.compile(datetimeRegExp)

        if not datetimeMatch.match(startDateStr): fail(2 , self.__get_invalid_dt_msg("Start", startDateStr, dtFormat))
        if not datetimeMatch.match(  endDateStr): fail(3 , self.__get_invalid_dt_msg("End",   endDateStr, dtFormat))

        return (startDateStr, endDateStr)

    
    def get_day_interval_params(self):
        return self.get_period_params(dtFormat=self.DATE_FORMAT, datetimeRegExp=self.DATE_REGEXP)


    def get_param_value(self, paramName):
        self.parse_args()
        as_dict = vars(self.arguments)
        return as_dict.get(paramName)


    def add_day_param(self):
        if self.parsedArguments: raise Exception("Cannot add params. Already parsed command line")

        dtFmt=self.DATE_FORMAT
        self.parser.add_argument('day', metavar='Day', type=str, help=self.__get_dt_help('Day', dtFmt))


    #region Helpers

    def __get_dt_help(self, paramName, dtFormat=None):
        if dtFormat is None: dtFormat = self.DATETIME_FORMAT
        return self.DATETIME_HELP.format(paramName, dtFormat)

    def __get_invalid_dt_msg(self, paramName, paramValue, dtFormat=None):
        if dtFormat is None: dtFormat = self.DATETIME_FORMAT
        return self.INVALID_DATETIME_MSG.format(paramName, dtFormat, paramValue)

    #end region Helpers

#end region Command Line Argument parsing
