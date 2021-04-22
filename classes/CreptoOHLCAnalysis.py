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

import pickle
import pymysql
import sqlalchemy

class PriceTrend(object):
    def __init__(self,symbol, interval_type, trend_flag, trend_interval, trend_price, anchor_interval, anchor_price ):
        self.symbol=symbol
        self.interval_type = interval_type
        self.trend_flag = trend_flag
        self.trend_interval = trend_interval
        self.trend_price = trend_price
        self.trend_anchor_interval = anchor_interval
        self.trend_anchor_price = anchor_price
        self.trend_peak_interval = -1
        self.trend_peak_price = 0
        self.trend_end_interval = -1
        self.trend_end_price = 0
    def setTrendPeakInfo(self, peak_interval, peak_price):
        self.trend_peak_interval = peak_interval
        self.trend_peak_price = peak_price
    def setTrendEndInfo(self, end_interval, end_price):
        self.trend_end_interval = end_interval
        self.trend_end_price = end_price


class CreptoOHLCAnalysis(object):
    def __init__(self, symbol):
        # self.dfOHLC = None
        # sql624_dbi=MSSQLDBInterface('10.36.0.89','cryptoDB','cyc','5432')
        # if (sql624_dbi.connectToDatabase()):
        #     #self.logMessage("Failed to connect to database!")
        #     self.dfOHLC = self.sql624_dbi.getDataFrameFromSQL("select TOP 10 *  from  cryptoDB.dbo.BitCoinPrices_1min order by trade_date desc ")

        self.symbol = symbol
        self.dfOHLCS = None
        self.interval_type = 0
        self.begin_date = None
        self.end_date = None
        super().__init__()

    def getAggregated5MINOHLCs(self):
        df = self.getOHLCS()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        # df.columns = ['trade_date', 'open', 'high', 'low', 'close', 'volume', 'interval_id']
        df.set_index("trade_date", inplace=True)
        dfAgg = df.groupby(pd.Grouper(freq='5Min', closed='right', label='right')).agg({
            "openprice": "first",
            "highprice": "max",
            "lowprice": "min",
            "lastprice": "last",
            "volume": "sum",
            "interval_id":"first"
        })
        dfAgg.reset_index(inplace=True)
        df.reset_index(inplace=True)
        # dfAgg.columns = ['trade_date', 'openprice', 'highprice','lowprice','lastprice', 'volume','interval_id' ]
        return dfAgg

    def getOHLCS(self):
        return self.dfOHLCS
    def setOHLCData (self, df, interval_type, begindate, enddate):
        self.dfOHLCS = df
        self.interval_type = interval_type
        self.begin_date = begindate
        self.end_date = enddate
        return

    def saveDataIntoMySQL(self, dataSavingIntervalID=0 ):
        if (self.dfOHLCS is None):
            return

        print("Start to save data into MYSQL database ...")
        cryptoDB = pymysql.connect(host="10.37.0.81", user="pythonuser", password="crypto2021!", database="cryptoSysDB",cursorclass=pymysql.cursors.DictCursor)
        cur = cryptoDB.cursor()

        for ohlc in self.dfOHLCS.itertuples():
            # ['trade_date', 'openprice', 'highprice','lowprice','lastprice', 'volume','interval_id' ]
            if (ohlc is None):
                continue

            # sp_BitCoinPrices_1min_indicators_addData
            if (        (not np.isnan(ohlc.lastprice))
                    and (not np.isnan(ohlc.openprice))
                ):
                if (ohlc.interval_id>dataSavingIntervalID):
                    # sp_BitCoinPrices_1min_indicators_addData
                    #CALL `cryptoSysDB`.`sp_BitCoinPrices_5min_addData`(<{IN my_symbol varchar(20)}>, <{IN my_tradedate datetime}>
                    # , <{IN my_openprice float}>, <{IN my_highprice float}>, <{IN my_lowprice float}>, <{IN my_closeprice float}>, <{IN my_volume float}>);
                    sql = (" call cryptoSysDB.sp_BitCoinPrices_5min_addData('%s',%d,'%s',%f,%f,%f,%f,%f);")%(
                                                                   self.symbol
                                                                   ,ohlc.interval_id
                                                                   ,ohlc.trade_date
                                                                   ,ohlc.openprice
                                                                    , ohlc.highprice
                                                                    , ohlc.lowprice
                                                                    , ohlc.lastprice
                                                                    , ohlc.volume
                                                                   )
                    try:
                        cur.execute(sql)
                        cryptoDB.commit()
                        print(("5min data saved! trade_date:%s lastprice:%f") % (ohlc.trade_date,ohlc.lastprice))
                    except Exception as e:
                        print(("Failed to save trend back to database! sql:%s") % (sql))
                        print(e)
                        cryptoDB.rollback()

        cryptoDB.commit()
        cur.close()
        cryptoDB.close()

        return

    def loadDataFromMSSQL(self, sql, dbi):
                # self.dfOHLCS.columns = ['interval_id','trade_date', 'openprice', 'highprice','lowprice','lastprice', 'volume' ]
        self.dfOHLCS = None
        nDataCount = 0
        if (dbi.connectToDatabase()):
            # self.logMessage("Failed to connect to database!")
            self.dfOHLCS = dbi.getDataFrameFromSQL(sql)
            self.dfOHLCS.columns = ['interval_id', 'trade_date', 'openprice', 'highprice', 'lowprice', 'lastprice',
                                    'volume']
            #    "select TOP 10 *  from  cryptoDB.dbo.BitCoinPrices_1min order by trade_date desc ")
            self.dfOHLCS['openprice']=self.dfOHLCS['openprice'].astype(float)
            self.dfOHLCS['highprice'] = self.dfOHLCS['highprice'].astype(float)
            self.dfOHLCS['lowprice'] = self.dfOHLCS['lowprice'].astype(float)
            self.dfOHLCS['lastprice'] = self.dfOHLCS['lastprice'].astype(float)
            self.dfOHLCS['volume'] = self.dfOHLCS['volume'].astype(float)

            nDataCount = len(self.dfOHLCS.index)
        return nDataCount

    def loadDataFromMySQL(self, interval_type, begindate, enddate):
        #load data from MySQL database
        print("Start to load data from MYSQL database ...")
        self.interval_type = interval_type
        self.begin_date = begindate
        self.end_date = enddate

        cryptoDB = pymysql.connect(host="10.37.0.81", user="pythonuser", password="crypto2021!", database="cryptoSysDB",cursorclass=pymysql.cursors.DictCursor)
        sql = (" call sp_BitCoinPrices_1min_getData('%s','%s','%s')") % (self.symbol, begindate, enddate)
        self.dfOHLCS = pd.read_sql(sql,cryptoDB)
        #self.dfOHLCS.columns = ['interval_id','trade_date', 'openprice', 'highprice','lowprice','lastprice', 'volume' ]
        self.dfOHLCS['trade_date'] = self.dfOHLCS['trade_date'].astype('datetime64[ns]')  # convert the date into "datetime64"

        nDataCount = len(self.dfOHLCS.index)
        print(("Succeeded to load %d data!")%(nDataCount))
        cryptoDB.close()
        return nDataCount

        
    def getOHLCCSV(self):
        if (self.dfOHLC is None):
            return ""
        s = io.StringIO()
        self.dfOHLC.to_csv(s)
        return s.getvalue()

    def getToleranceModelTrends(self, tolerance):
        dfOHLCS=self.dfOHLCS
        if (dfOHLCS is None):
            return None

        symbol=self.symbol
        intervalType=self.interval_type
        trendDatas = []
        # start to run the tolerance trend model
        nTrendFlag = -1
        fLowestLowPrice =float (dfOHLCS.iloc[0].lastprice)
        nLowestLowInterval = dfOHLCS.iloc[0].interval_id
        fHighestHighPrice = float(0)
        nHighestHighInterval =float(0)

        nIntervalIndex = 0
        newTrend = None
        priorTrend = None
        for row in dfOHLCS.itertuples():
            nIntervalID = row.interval_id
            openPrice = float(row.openprice)
            highprice = float(row.highprice)
            lowprice = float(row.lowprice)
            lastprice = float(row.lastprice)
            volume = float(row.volume)
            mhl = 0.5 * (highprice + lowprice)

            if (nTrendFlag == -1):
                # in down trend: look for up trend
                if (mhl > (1 + tolerance) * fLowestLowPrice):
                    nTrendFlag = 1
                    TrendPrice = mhl
                    TrendIntervalID = nIntervalID

                    # set up prior trend info
                    if (priorTrend is not None):
                        priorTrend.setTrendPeakInfo(nLowestLowInterval, fLowestLowPrice)
                        priorTrend.setTrendEndInfo(TrendIntervalID, TrendPrice)
                    # add new trend
                    newTrend = PriceTrend(symbol, intervalType, nTrendFlag, TrendIntervalID, TrendPrice,
                                          nLowestLowInterval, fLowestLowPrice)
                    trendDatas.append(newTrend)
                    priorTrend = newTrend
                    # prepare for the new trend stats
                    fHighestHighPrice = mhl
                    nHighestHighInterval = nIntervalID

                    print(("Trend:%d Interval:%d Prie:%f") % (nTrendFlag, TrendIntervalID, TrendPrice))


                else:
                    # update the lowestlow
                    if (fLowestLowPrice > mhl):
                        fLowestLowPrice, nLowestLowInterval = mhl, nIntervalID

            if (nTrendFlag == 1):
                # in up trend: look for up trend
                if (mhl < (1 - tolerance) * fHighestHighPrice):
                    nTrendFlag = -1
                    TrendPrice = mhl
                    TrendIntervalID = nIntervalID

                    # set up prior trend info
                    if (priorTrend is not None):
                        priorTrend.setTrendPeakInfo(nHighestHighInterval, fHighestHighPrice)
                        priorTrend.setTrendEndInfo(TrendIntervalID, TrendPrice)

                    # add new trend
                    newTrend = PriceTrend(symbol, intervalType, nTrendFlag, TrendIntervalID, TrendPrice,
                                          nLowestLowInterval, fLowestLowPrice)
                    trendDatas.append(newTrend)
                    priorTrend = newTrend
                    # prepare for the new trend
                    fLowestLowPrice = mhl
                    nLowestLowInterval = nIntervalID

                    print(("Trend:%d Interval:%d Prie:%f") % (nTrendFlag, TrendIntervalID, TrendPrice))

                else:
                    # update the lowestlow
                    if (fHighestHighPrice < mhl):
                        nHighestHighInterval, fHighestHighPrice = nIntervalID, mhl

        return trendDatas


    def getHVToleranceModelTrends(self, multiple):
        dfOHLCS=self.dfOHLCS

        #calculate the Historical Volatility
        dfOHLCS['chg1'] = dfOHLCS['lastprice']/dfOHLCS['lastprice'].shift(1)-1.0
        dfOHLCS['hv100']=dfOHLCS['chg1'].rolling(100).std()

        if (dfOHLCS is None):
            return None

        symbol=self.symbol
        intervalType=self.interval_type
        trendDatas = []
        # start to run the tolerance trend model
        nTrendFlag = -1
        fLowestLowPrice =float (dfOHLCS.iloc[0].lastprice)
        nLowestLowInterval = dfOHLCS.iloc[0].interval_id
        fHighestHighPrice = float(0)
        nHighestHighInterval =float(0)

        nIntervalIndex = 0
        newTrend = None
        priorTrend = None
        MIN_TOLERANCE = 0.015
        tolerance = MIN_TOLERANCE
        for row in dfOHLCS.itertuples():
            nIntervalID = row.interval_id
            openPrice = float(row.openprice)
            highprice = float(row.highprice)
            lowprice = float(row.lowprice)
            lastprice = float(row.lastprice)
            volume = float(row.volume)
            mhl = 0.5 * (highprice + lowprice)
            hv100 = float(row.hv100)

            if (nTrendFlag == -1):
                # in down trend: look for up trend
                if (mhl > (1 + tolerance) * fLowestLowPrice):
                    nTrendFlag = 1
                    TrendPrice = mhl
                    TrendIntervalID = nIntervalID

                    # set up prior trend info
                    if (priorTrend is not None):
                        priorTrend.setTrendPeakInfo(nLowestLowInterval, fLowestLowPrice)
                        priorTrend.setTrendEndInfo(TrendIntervalID, TrendPrice)
                    # add new trend
                    newTrend = PriceTrend(symbol, intervalType, nTrendFlag, TrendIntervalID, TrendPrice,
                                          nLowestLowInterval, fLowestLowPrice)
                    trendDatas.append(newTrend)
                    priorTrend = newTrend
                    # prepare for the new trend stats
                    fHighestHighPrice = mhl
                    nHighestHighInterval = nIntervalID

                    print(("Trend:%d Interval:%d Prie:%f") % (nTrendFlag, TrendIntervalID, TrendPrice))

                else:
                    # update the lowestlow
                    if (fLowestLowPrice > mhl):
                        fLowestLowPrice, nLowestLowInterval = mhl, nIntervalID
                        if (nIntervalIndex > 100):
                            tolerance = hv100 * float(multiple)
                        if (tolerance<MIN_TOLERANCE):
                            tolerance = MIN_TOLERANCE

            if (nTrendFlag == 1):
                # in up trend: look for up trend
                if (mhl < (1 - tolerance) * fHighestHighPrice):
                    nTrendFlag = -1
                    TrendPrice = mhl
                    TrendIntervalID = nIntervalID

                    # set up prior trend info
                    if (priorTrend is not None):
                        priorTrend.setTrendPeakInfo(nHighestHighInterval, fHighestHighPrice)
                        priorTrend.setTrendEndInfo(TrendIntervalID, TrendPrice)

                    # add new trend
                    newTrend = PriceTrend(symbol, intervalType, nTrendFlag, TrendIntervalID, TrendPrice,
                                          nLowestLowInterval, fLowestLowPrice)
                    trendDatas.append(newTrend)
                    priorTrend = newTrend
                    # prepare for the new trend
                    fLowestLowPrice = mhl
                    nLowestLowInterval = nIntervalID

                    print(("Trend:%d Interval:%d Prie:%f") % (nTrendFlag, TrendIntervalID, TrendPrice))

                else:
                    # update the lowestlow
                    if (fHighestHighPrice < mhl):
                        nHighestHighInterval, fHighestHighPrice = nIntervalID, mhl
                        if (nIntervalIndex > 100):
                            tolerance = hv100 * float(multiple)
                        if (tolerance<MIN_TOLERANCE):
                            tolerance = MIN_TOLERANCE

            # prepare for next interval
            nIntervalIndex +=1
        return trendDatas

    def calculateTrendIndicator20210323(self, targetIndicatorName):
        '''
    (2) How do we combine the "win5, win10, win15" and the "chg" :
        * Up trend:
        => win5>win15
        and slope is above a certain value( 0.25%) : => BUY
        AND slope of ema20 to be positive
        * Down Trend: (Sell)
        calculate the trend indicator
        :param targetFieldName:
        :return:
        '''
        if (self.dfOHLCS is None):
            return

        nDataCount = len (self.dfOHLCS.index)
        indicatorValues = [0.0] * nDataCount

        nIntervalIndex = 0
        indicatorValue = 0
        prior_ohlc = None
        for ohlc in self.dfOHLCS.itertuples():
            trade_date = ohlc.trade_date
            indicatorValue = 0
            if (prior_ohlc is not None):
                chg = ohlc.chg
                chg_ma5 = ohlc.chg_ma5
                sma20  = ohlc.sma20
                winningBarPct_5 = ohlc.winningBarPct_5
                winningBarPct_10 = ohlc.winningBarPct_10
                winningBarPct_15 = ohlc.winningBarPct_15

                prior_chg_ma5=prior_ohlc.chg_ma5
                prior_sma20 = prior_ohlc.sma20

                if (chg>0.001 and chg_ma5>0.001 and sma20>prior_sma20 and winningBarPct_5>winningBarPct_10 and winningBarPct_10>winningBarPct_15 and winningBarPct_15>=0.5):
                    indicatorValue = 1

            #prepare for next
            indicatorValues[nIntervalIndex] = indicatorValue
            nIntervalIndex += 1
            prior_ohlc = ohlc

        self.dfOHLCS[targetIndicatorName] = indicatorValues
        return

    def calculateDownTrendIndicator20210323(self, targetIndicatorName):
        '''
        * Up trend:
        => win5>win15
        and slope is above a certain value( 0.25%) : => BUY
        AND slope of ema20 to be positive
        * Down Trend: (Sell)
        => win5<win15
        and slope is below a certain value( -0.25%) : => BUY
        AND slope of ema20 to be negative
        '''
        if (self.dfOHLCS is None):
            return

        nDataCount = len (self.dfOHLCS.index)
        indicatorValues = [0.0] * nDataCount

        nIntervalIndex = 0
        indicatorValue = 0
        prior_ohlc = None
        for ohlc in self.dfOHLCS.itertuples():
            trade_date = ohlc.trade_date
            indicatorValue = 0
            if (prior_ohlc is not None):
                chg = ohlc.chg
                chg_ma5 = ohlc.chg_ma5
                sma20  = ohlc.sma20
                winningBarPct_5 = ohlc.winningBarPct_5
                winningBarPct_10 = ohlc.winningBarPct_10
                winningBarPct_15 = ohlc.winningBarPct_15

                prior_chg_ma5=prior_ohlc.chg_ma5
                prior_sma20 = prior_ohlc.sma20

                if (    chg<-0.001
                        and chg_ma5<-0.001
                        and sma20<prior_sma20
                        and winningBarPct_5<winningBarPct_10
                        and winningBarPct_10<winningBarPct_15
                        and winningBarPct_15<0.5):
                    indicatorValue = 1

            #prepare for next
            indicatorValues[nIntervalIndex] = indicatorValue
            nIntervalIndex += 1
            prior_ohlc = ohlc

        self.dfOHLCS[targetIndicatorName] = indicatorValues
        return


    def calclulateWinningBarPctIndicator(self, nPeriod):
            #
        # New Ideas to be tested this week:         =>winningBarPct
        # Idea#1:                 % of UP intervals in past x(10,?30) intervals
        # •	Calculate an indicator to reflect the % of UP intervals in the past x intervals, the indicator value will range from 0 - 100
        # Idea#2:   slope indicator calculation     => chg
        # •	Calculate the slope of last price curve to see whether there is a threshold we can use to screen out good up trends ( or
        #                                 to see a good correlation for the good up trends )
        if (self.dfOHLCS is None):
            return

        if not ('prior_lastprice' in self.dfOHLCS.columns ):
            self.dfOHLCS['prior_lastprice'] = self.dfOHLCS['lastprice'].shift(1)
        if not ('chg' in self.dfOHLCS.columns ):
            self.dfOHLCS['chg']=self.dfOHLCS['lastprice']/self.dfOHLCS['prior_lastprice']-1.0
            self.dfOHLCS['chg_flag']=0
            self.dfOHLCS.loc[(self.dfOHLCS['chg']>0),'chg_flag']=1
            self.dfOHLCS['chg_ma5']=self.dfOHLCS['chg'].rolling(window=5).mean()

        self.dfOHLCS['winner_count']=self.dfOHLCS['chg_flag'].rolling(window=nPeriod).sum()
        self.dfOHLCS['winningBarPct']=self.dfOHLCS['winner_count']/float(nPeriod)

        return

    def doTrendModelAnalysis(self):
        # calcualte the new indicator
        self.calclulateWinningBarPctIndicator(5)
        self.dfOHLCS['winningBarPct_5'] = self.dfOHLCS['winningBarPct']
        self.calclulateWinningBarPctIndicator(10)
        self.dfOHLCS['winningBarPct_10'] = self.dfOHLCS['winningBarPct']
        self.calclulateWinningBarPctIndicator(15)
        self.dfOHLCS['winningBarPct_15'] = self.dfOHLCS['winningBarPct']
        self.dfOHLCS['sma20'] = self.dfOHLCS['lastprice'].rolling(20).mean()
        # determine the up trend according to rules below:
        self.calculateTrendIndicator20210323('BUYSIGNAL')
        self.calculateDownTrendIndicator20210323('SELLSIGNAL')
        return

