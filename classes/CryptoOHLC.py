# -*- coding: utf-8 -*-
"""
Created on Sat Feb 27 22:36:09 2021

@author: jzhou
"""


import platform
import datetime
import numpy as np
import pandas as pd
import json
import os
import pandas
import csv
import io
import requests
import xml.etree.ElementTree as ET
from classes.Logger import Logger
from classes.MSSQLUtilities import MSSQLDBInterface


class CryptoOHLC(Object):
    def __init__(self, symbol, term, begindate, enddate):
        self.dfOHLC = None

        super().__init__()
    def loadDataFromMSSQL(self, sql, dbi):
        # sql624_dbi = MSSQLDBInterface('10.36.0.89', 'cryptoDB', 'cyc', '5432')
        if (dbi.connectToDatabase()):
            # self.logMessage("Failed to connect to database!")
            self.dfOHLC = self.sql624_dbi.getDataFrameFromSQL(
                "select TOP 10 *  from  cryptoDB.dbo.BitCoinPrices_1min order by trade_date desc ")

    def getOHLCCSV(self):
        if (self.dfOHLC is None):
            return ""
        
        s = io.StringIO()        
        self.dfOHLC.to_csv(s)
        return s.getvalue()