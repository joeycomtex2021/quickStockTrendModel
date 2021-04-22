
import pandas as pd

# -*- coding: utf-8 -*-
"""
Created on 2020-07-10
This module includes all the QuoteMedeia related functions and classes 
@author: jzhou
"""
import platform
import sys
import datetime
import numpy as np
import json
import os
import pandas as pd
import csv
import pickle
import pymysql
import sqlalchemy
from classes.CreptoOHLCAnalysis import CreptoOHLCAnalysis
from classes.CreptoOHLCAnalysis import PriceTrend
from classes.MSSQLUtilities import MSSQLDBInterface


#
# '''
# New Ideas to be tested this week:
# Idea#1:                 % of UP intervals in past x(10,?30) intervals
# •	Calculate an indicator to reflect the % of UP intervals in the past x intervals, the indicator value will range from 0 - 100
#
# Idea#2:   slope indicator calculation
# •	Calculate the slope of last price curve to see whether there is a threshold we can use to screen out good up trends ( or
#                                 to see a good correlation for the good up trends )
def logMessage(strMessage):
    strCurrentDate=datetime.datetime.now().strftime("%Y%m%d")
    strLogFileName=(("cryptoCSVDataParser_%s.log")%(strCurrentDate))
    file =open(strLogFileName,"a")
    strCurrentTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');
    file.write(""+strCurrentTime+strMessage)
    file.close()
    print(strMessage)

def saveNewIndicatorDataIntoDatabase(dataSavingIntervalID,ohlcsAnalysisData,strProcdureName):
    '''

    :param dataSavingIntervalID:
    :param ohlcsAnalysisData:
    :return:
    '''
    #save the new indicator data into databse

    #check in
    if (ohlcsAnalysisData is None):
        return

    print("Start to save the trend data into MYSQL database ...")
    cryptoDB = pymysql.connect(host="10.37.0.81", user="pythonuser", password="crypto2021!", database="cryptoSysDB",cursorclass=pymysql.cursors.DictCursor)
    cur = cryptoDB.cursor()

    symbol = ohlcsAnalysisData.symbol
    interval_type = ohlcsAnalysisData.interval_type
    ohlcs = ohlcsAnalysisData.getOHLCS()

    #self.dfOHLCS.columns = ['interval_id','trade_date', 'openprice', 'highprice','lowprice','lastprice', 'volume' ]
    #calculated indicator:  chg, winningBarPct
    for ohlc in ohlcs.itertuples():
        interval_id = ohlc.interval_id
        chg = ohlc.chg
        #winningBarPct_5
        winningBarPct_5 = ohlc.winningBarPct_5
        winningBarPct_10 = ohlc.winningBarPct_10
        winningBarPct_15 = ohlc.winningBarPct_15
        MIN1BUYSIGNAL = ohlc.BUYSIGNAL

        #sp_BitCoinPrices_1min_indicators_addData
        if ((not np.isnan(chg)) and (not np.isnan(winningBarPct_5))  and (not np.isnan(winningBarPct_10))  and (not np.isnan(winningBarPct_15))):
            #sp_BitCoinPrices_1min_indicators_addData
            sql = (" call %s('%s',%d,%f,%f,%f,%f,%d);")%(strProcdureName,
                    symbol, interval_id, chg,winningBarPct_5,winningBarPct_10,winningBarPct_15,MIN1BUYSIGNAL)
            try:
                cur.execute(sql)
                cryptoDB.commit()
                print(("IntervalID:%d chg:%f winningBarPct_5:%f")%(interval_id, chg, winningBarPct_5))
            except Exception as e:
                print(("Failed to save trend back to database! sql:%s") % (sql))
                print(e)
                cryptoDB.rollback()

    cryptoDB.commit()
    cryptoDB.close()
    return


def saveModelTrendDataIntoDatabase(dataSavingIntervalID, modelname, trendDatas):
    #save trend data into database
    nTrendIndex = 0
    nTrendCount = len(trendDatas)
    #dialect+driver://username:password@host:port/database
    #mysql://pythonuser:crypto2021!@10.37.0.81/cryptoSysDB
    #save trend into mySQL database
    print("Start to save the trend data into MYSQL database ...")
    cryptoDB = pymysql.connect(host="10.37.0.81", user="pythonuser", password="crypto2021!", database="cryptoSysDB",cursorclass=pymysql.cursors.DictCursor)
    for nTrendIndex in range(0,nTrendCount):
        priceTrend = trendDatas[nTrendIndex]
        if (priceTrend is not None):
            trend_interval = priceTrend.trend_interval
            if (trend_interval>=dataSavingIntervalID):
                if (priceTrend is not None):
                    # call sp_BitCoinTrends_addTrendCALL `cryptoSysDB`.`sp_BitCoinTrends_addTrend`(<{IN my_symbol varchar(20)}>, <{IN my_data_type int}>, <{IN my_trend_flag int}>, <{IN my_trend_interval bigint}>, <{IN my_trend_price float}>, <{IN my_trendAnchorInterval bigint}>, <{IN my_trendAnchorPrice float}>, <{IN my_trendPeakInterval bigint}>, <{IN my_trendPeakPrice float}>, <{IN my_trendEndInterval bigint}>, <{IN my_trendEndPrice float}>);
                    sql = (" call sp_BitCoinModelTrends_addTrend('%s','%s',%d,%d,%d,%f ,%d,%f,%d,%f,%d,%f);")%(
                            modelname, priceTrend.symbol, priceTrend.interval_type,priceTrend.trend_flag,priceTrend.trend_interval,priceTrend.trend_price
                            ,priceTrend.trend_anchor_interval,priceTrend.trend_anchor_price,priceTrend.trend_peak_interval,priceTrend.trend_peak_price,priceTrend.trend_end_interval,priceTrend.trend_end_price)
                    try:
                        cur = cryptoDB.cursor()
                        cur.execute(sql)
                        cryptoDB.commit()
                        print(("Trend:%d IntervalID:%d")%(priceTrend.trend_flag, priceTrend.trend_interval))
                        cur.close()
                    except Exception as e:
                        print(("Failed to save trend back to database! sql:%s") % (sql))
                        print(e)
                        cryptoDB.rollback()

    cryptoDB.commit()
    cryptoDB.close()
    return

def saveBuySellSignalsIntoDatabase(symbol, signalType, dfSignals, dataSavingIntervalID):
    #save trend data into database
    nSignalIndex = 0
    nSignalCount = len(dfSignals)

    #dialect+driver://username:password@host:port/database
    #mysql://pythonuser:crypto2021!@10.37.0.81/cryptoSysDB
    #save trend into mySQL database
    print("Start to save the trend data into MYSQL database ...")
    cryptoDB = pymysql.connect(host="10.37.0.81", user="pythonuser", password="crypto2021!", database="cryptoSysDB",cursorclass=pymysql.cursors.DictCursor)
    for row in dfSignals.itertuples():
        signal_interval_id = row.interval_id
        signal_date = row.trade_date
        signal_price = row.lastprice
        if (signal_interval_id>=dataSavingIntervalID):
            sql = (" call sp_TradingSignals_addSignal('%s',%d,'%s',%f);")%(
                    symbol, signalType, signal_date, signal_price)
            try:
                cur = cryptoDB.cursor()
                cur.execute(sql)
                cryptoDB.commit()
                print(("SignalPrice:%f IntervalID:%d")%(signal_price, signal_interval_id))
                cur.close()
            except Exception as e:
                print(("Failed to save trend back to database! sql:%s") % (sql))
                print(e)
                cryptoDB.rollback()

    cryptoDB.commit()
    cryptoDB.close()
    return


# main program entry
#***************  beginning of the main program ******************
if __name__=="__main__":
    logMessage("Comtex Quick Stock Trend Model  Analysis ...")
    logMessage(('Python version:%s') % (platform.python_version()))

    symbol='MSFT'
    modelbegindate = '2015-01-01'
    modelenddate = '2050-01-01'
    datasavingdate = '2018-01-01'
    strDataSavingDate = None

    #read the command line arguments
    if (len(sys.argv)>=2):
        symbol = (sys.argv[1])
    if (len(sys.argv)>=3):
        modelbegindate = (sys.argv[2])
    if (len(sys.argv)>=4):
        modelenddate = (sys.argv[3])
    if (len(sys.argv)>=5):
        strDataSavingDate = (sys.argv[4])
    if (strDataSavingDate is not None):
        datasavingdate = pd.Timestamp(datetime.datetime.strptime(strDataSavingDate,'%Y-%m-%d').date())

    #
    ohlcAnalysis = CreptoOHLCAnalysis(symbol)
    servername = "sfo-prod-sql603"
    dbname = "cyccert"
    username="cyc"
    userpassword = "5432"
    dbi = MSSQLDBInterface(servername, dbname, username, userpassword)
    sql=("exec cyccert.[dbo].[sp_getSymbolDailyData] '%s','%s','%s'")%(symbol, modelbegindate, modelenddate)

    nDataCount = ohlcAnalysis.loadDataFromMSSQL(sql,dbi)
    if (nDataCount<100):
        print("Not enough data to model!")
        exit(1)

    print(("Succeeded to load %d data!")%(nDataCount))

    #determine the dataSavingIntervalID
    dataSavingIntervalID = 0
    dfOHLCS = ohlcAnalysis.getOHLCS()
    if (datasavingdate is None):
        datasavingdate = dfOHLCS.iloc[nDataCount - 100].trade_date

    dataSvingStartData = dfOHLCS.loc[(dfOHLCS['trade_date']>=datasavingdate),['interval_id']].iloc[0]
    if (len(dataSvingStartData.index)>0):
        dataSavingIntervalID=dataSvingStartData.interval_id

    trends = ohlcAnalysis.getHVToleranceModelTrends(1.0)
    # save trends into database
    saveModelTrendDataIntoDatabase(dataSavingIntervalID, 'HV100x1TolModel', trends)

    trends = ohlcAnalysis.getHVToleranceModelTrends(2.0)
    # save trends into database
    saveModelTrendDataIntoDatabase(dataSavingIntervalID, 'HV100x2TolModel', trends)

    trends = ohlcAnalysis.getHVToleranceModelTrends(3.0)
    # save trends into database
    saveModelTrendDataIntoDatabase(dataSavingIntervalID, 'HV100x3TolModel', trends)

    trends = ohlcAnalysis.getToleranceModelTrends(0.03)
    # save trends into database
    saveModelTrendDataIntoDatabase(dataSavingIntervalID, 'Pct3TolModel', trends)

    ohlcAnalysis.doTrendModelAnalysis()
    dfBuySignals = dfOHLCS.loc[(dfOHLCS['BUYSIGNAL']>0)].copy()
    dfSellSignals = dfOHLCS.loc[(dfOHLCS['SELLSIGNAL'] > 0)].copy()
    #save BUY SIGNALS/SELL SIGNALS into database
    saveBuySellSignalsIntoDatabase(symbol, 1, dfBuySignals, dataSavingIntervalID)
    saveBuySellSignalsIntoDatabase(symbol, -1, dfSellSignals, dataSavingIntervalID)

    #save 5min data
    print ("Modeling Finished ....!")

