# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 13:57:48 2019

@author: jzhou
"""
#import numpy
import pandas as pd
import pymssql
import pickle

from classes.Logger import Logger


class MSSQLDBInterface(Logger):
    def __init__(self, dataFileName,sql, dbi):
        self._dbi= dbi
        self._dataFileName=dataFileName
        self._sql = sql
        super().__init__()
