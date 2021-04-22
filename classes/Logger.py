# -*- coding: utf-8 -*-
"""
Created on Wed May 16 16:08:54 2018
This class implement the basic logging functions
@author: jzhou
"""
class Logger(object):
    def __init__(self, logfilename="",logLevel=0):
        self.setLogLevel(logLevel)
        self.setLogFile(logfilename)
        self._logFile =""
        
    def setLogFile(self,logfilename):
        self._logFileName=logfilename
        
    def setLogLevel(self, logLevel):
        self._logLevel=logLevel
        
    def log(self, message,nLogLevel=0):
        if (nLogLevel>=self._logLevel):
            print(message)
            if (self._logFileName):
                self._logFile=open(self._logFileName,'a')
                self._logFile.write(message)
                self._logFile.close()
           