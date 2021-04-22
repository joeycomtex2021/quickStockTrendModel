# -*- coding: utf-8 -*-
"""
Created on Wed May 16 10:19:21 2018
This program includes all the Microsoft SQL server related class & functions
@author: jzhou
"""

#import numpy
import pandas as pd
import pymssql
import pickle

from classes.Logger import Logger


class MSSQLDBInterface(Logger):
    def __init__(self, servername,databasename, username, userpassword):
        self.setDBConnection(servername,databasename,username,userpassword)
        self._connected=False
        self._logLevel=0 
        self._logfile=None
        self.columnNames=[]
        super().__init__()
        
    def setLogLevel(self, loglevel):
        self._logLevel=loglevel
    
        
    def setDBConnection(self,servername, databasename,username,userpassword):
        self._server=servername
        self._database=databasename
        self._user=username
        self._password=userpassword
        
    def disconnectFromDatabase(self):
        if self.isConnected():
            self._dbConnection.close()
            
    def connectToDatabase(self):
        try: 
            self._dbConnection=pymssql.connect(server=self._server,user=self._user,password=self._password,database=self._database)
            self._connected=True
            self.log(("Succeeded to connect to the database! server: %s") % (self._server),0)
        except pymssql.StandardError as se:
            self.log(("Failed to connect to the database! server: %s") % (self._server),6)
            self.log(se)
        finally:
            return self._connected
        
    def isConnected(self):
        return self._connected
    
    def executeSQL(self,sql):
        # check in
        if not sql:
            return None   # no sql 
        if not self._connected: 
            return None                       # database was not connected
        if not self._dbConnection: 
            return None   #no database object

        #start to execute sql
        myResult=False
        try: 
            mycursor=self._dbConnection.cursor()
            mycursor.execute(sql)            
            self._dbConnection.commit()
            myResult=True
            #self.log(("Succeeded to execute a SQL! sql:%s") % (sql),0)
        except pymssql.StandardError as se:
            self.log(("Failed to execute a SQL! sql:%s") % (sql),6)
            self.log(se)
        
            myResult=False
        finally:
            mycursor.close()
        return myResult



    def getDataRowFromSQL(self,sql):
        data_array=self.getDataFromSQL(sql)
        datarow=[record[0] for record in data_array]
        return datarow
    
    def getDataFromSQL(self,sql):
        # get tuples data from sql
        # check in
        if not sql:
            return None   # no sql 
        if not self._connected: 
            return None                       # database was not connected
        if not self._dbConnection: 
            return None   #no database object
        
        #start to run the sql and return the results
        mydata=None
        try: 
            mycursor=self._dbConnection.cursor()
            mycursor.execute(sql)
            mydata=mycursor.fetchall()  #fetch all the data into a tuplit 
            if len(mydata)>0:
                #prepare column names
                self.columnNames=[]
                for i in range(len(mycursor.description)):
                    desc=mycursor.description[i]
                    self.columnNames.append(desc[0])

        except pymssql.StandardError as se:
            self.log(("Failed to fetech data from SQL! sql:%s") % (sql))
            self.log(se,6)
            mydata=None
        finally:
            mycursor.close()
        return mydata
    
    
    def getDictDataFromSQL(self,sql):
        return dict(self.getDataFromSQL(sql))
    
    def getDataFrameFromSQL(self,sql):        
        dataRecords=self.getDataFromSQL(sql)
        if (len(dataRecords)>0):
            pd_data=pd.DataFrame.from_records(dataRecords)        
            pd_data.columns=self.columnNames
            return pd_data
        else:
            return None
