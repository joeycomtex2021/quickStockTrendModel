# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 16:41:20 2019

@author: jzhou
"""
import sys
import pandas as pd
#import pickle

from classes.Logger import Logger


class MktTimingPortfolio(object):
    def __init__(self, initialValue,DictIntervalIDs):
        self.InitialCashValue  = initialValue 
        self.initializePortfolio()
        self._DictIntervalIDs = DictIntervalIDs
        
    def initializePortfolio(self):
        self.CashValue = self.InitialCashValue
        self.MarketShares = 0
        self.PortfolioValues = pd.DataFrame(data=None,columns=['trade_date','portfoliovalue','cashvalue'])
        self.PortfolioAnnualReturns = pd.DataFrame(data=None,columns=['yearend_date','portfoliovalue','daycount','avgportfoliovalue','avgcashvalue','prioryearendvalue','return'])
    
    def closeMarketPositions(self, price):
        #close
        if (self.MarketShares>0):
            #add cash value
            self.CashValue = self.CashValue + self.MarketShares * price
            #reset the market shares to 0 
            self.MarketShares = 0
            
        #print(('Close market position! price:%f')%(price))

    def openMarketPositions(self, investmentpct, price):
        #check in 
        if (price<=0):
            return

        if (self.MarketShares>0):
            #close the existing postion
            self.closeMarketPositions(price)
        
        #establish new position
        investment = self.CashValue  * investmentpct
        self.MarketShares = investment / price
        self.CashValue = self.CashValue - investment 
        #print(('Open market position! investment:%f price:%f')%(investment,price))

    def refreshPortfolioValue(self, trade_date, price):
        positionvalue = self.MarketShares * price
        portfoliovalue=self.CashValue + positionvalue
        self.PortfolioValues= self.PortfolioValues.append({'trade_date':trade_date,'portfoliovalue':portfoliovalue,'cashvalue':self.CashValue},ignore_index=True)
        
#        #refresh the annual returns
#        annualReturnCount =  len(self.PortfolioAnnualReturns.index)
#        if (annualReturnCount<=0):
#            # it is the first year
#            self.PortfolioAnnualReturns= self.PortfolioAnnualReturns.append({'yearend_date':trade_date,'portfoliovalue':portfoliovalue,'daycount':1,'avgportfoliovalue':portfoliovalue,'avgcashvalue':self.CashValue,'prioryearendvalue':portfoliovalue,'return':0},ignore_index=True)
#        else:
#            currentAnnualReturn = self.PortfolioAnnualReturns.iloc[-1] 
#            prior_value = currentAnnualReturn.portfoliovalue 
#            prior_updateDate = currentAnnualReturn.yearend_date
#            daycount  = currentAnnualReturn.daycount
#            prioryearendvalue = currentAnnualReturn.prioryearendvalue
#            
#            newAvgPortfolioValue = (currentAnnualReturn.avgportfoliovalue * daycount + portfoliovalue ) / ( daycount + 1)
#            newAvgCashValue = (currentAnnualReturn.avgcashvalue * daycount + self.CashValue ) / (daycount + 1)
#            
#            if (prioryearendvalue>0):
#                annualreturn = portfoliovalue / prioryearendvalue -1.0
#                
#            if (trade_date.year == prior_updateDate.year):
#                self.PortfolioAnnualReturns.iloc[-1,self.PortfolioAnnualReturns.columns.get_loc('yearend_date')]=trade_date
#                self.PortfolioAnnualReturns.iloc[-1,self.PortfolioAnnualReturns.columns.get_loc('portfoliovalue')]=portfoliovalue
#                self.PortfolioAnnualReturns.iloc[-1,self.PortfolioAnnualReturns.columns.get_loc('avgportfoliovalue')]=newAvgPortfolioValue
#                self.PortfolioAnnualReturns.iloc[-1,self.PortfolioAnnualReturns.columns.get_loc('avgcashvalue')]=newAvgCashValue                
#                self.PortfolioAnnualReturns.iloc[-1,self.PortfolioAnnualReturns.columns.get_loc('daycount')]= daycount + 1
#                self.PortfolioAnnualReturns.iloc[-1,self.PortfolioAnnualReturns.columns.get_loc('return')]= annualreturn
#            else:
#                # it is a new year
#                if (prior_value>0):
#                    annualreturn = portfoliovalue / prior_value -1.0
#                self.PortfolioAnnualReturns= self.PortfolioAnnualReturns.append({'yearend_date':trade_date,'portfoliovalue':portfoliovalue,'daycount':1,'avgportfoliovalue':portfoliovalue,'avgcashvalue':self.CashValue,'prioryearendvalue':prior_value,'return':annualreturn},ignore_index=True)
#
        sys.stdout.write(('\rDate:%s PortfolioValue:%6f.0')%(trade_date,portfoliovalue))


    def getPortfolioTotalReturn(self):
        beginValue = self.PortfolioValues.iloc[0].portfoliovalue
        endValue = self.PortfolioValues.iloc[-1].portfoliovalue
        return endValue/beginValue-1.0 
    
    #     trade:  open_interval_id [ index ] , tradetype, entry_price, close_interval_id, close_price
    def runPortfolioTesting(self,beginIntervalID,endIntervalID,dfTrades,dfIndicators):
        self.initializePortfolio()
        
        prior_daily_interval_id=0
        prior_interval_time =  self._DictIntervalIDs[beginIntervalID]
        prior_lastprice = 0 
        positionFlag = 0
        
        #for all intervals starting from 2018-1-1 to current, do the portfolio testing 
        for row in dfIndicators.itertuples():
            interval_id = row.Index            
            last_price = row.LAST_PRICE
            interval_date = self._DictIntervalIDs[interval_id]
            
            if (interval_id < beginIntervalID):
                continue
            if (interval_id > endIntervalID):
                break
            
            # test dfTrades
            # (1) test buy trades
            if (positionFlag == 0 ):
                if (interval_id in dfTrades.index):
                    # new buy signal arrived 
                    trade = dfTrades.loc[interval_id]
                    signalprice = trade.ENTRY_PRICE 
                    positionFlag = trade.TRADETYPE
                    # def openMarketPositions(self, investmentpct, price):
                    self.openMarketPositions(1.0,signalprice)
                
            # (2) test sell trades
            if (positionFlag != 0 ):
                trade = dfTrades.loc[(dfTrades['CLOSE_INTERVAL_ID']==interval_id)]
                if (len(trade.index)>0):
                    # new sell signal arrived
                    closeprice = trade.iloc[0].CLOSE_PRICE
                    self.closeMarketPositions(closeprice)
                    positionFlag = 0
            #
            if (interval_id > prior_daily_interval_id):
                if (prior_lastprice<=0):
                    prior_lastprice = last_price
                self.refreshPortfolioValue(prior_interval_time,prior_lastprice)
            
            #prepare for the next signal
            prior_daily_interval_id=interval_id
            prior_interval_time=interval_date
            prior_lastprice = last_price

        return 


    #     trade:  open_interval_id [ index ] , tradetype, entry_price, close_interval_id, close_price
    def runTradeFlagPortfolioTesting(self,beginIntervalID,endIntervalID,dfIndicators,nTradeFlagIndex):
        self.initializePortfolio()
        
        prior_daily_interval_id=0
        prior_interval_time =  self._DictIntervalIDs[beginIntervalID]
        prior_lastprice = 0 
        positionFlag = 0
        tradeFlag=0
        prior_tradeFlag = 0 
        
        #for all intervals starting from 2018-1-1 to current, do the portfolio testing 
        for row in dfIndicators.itertuples():
            interval_id = row.Index            
            last_price = row.LAST_PRICE
            tradeFlag = row[nTradeFlagIndex+1]
            interval_date = self._DictIntervalIDs[interval_id]
            
            if (interval_id < beginIntervalID):
                continue
            if (interval_id > endIntervalID):
                break
            
            # test dfTrades
            # (1) test buy trades
            if (positionFlag == 0 ):
                if (tradeFlag>0 and tradeFlag != prior_tradeFlag):
                    # new buy signal arrived 
                    signalprice = last_price
                    positionFlag = 1
                    # def openMarketPositions(self, investmentpct, price):
                    self.openMarketPositions(1.0,signalprice)
                
            # (2) test sell trades
            if (positionFlag != 0 ):
                if (tradeFlag<=0 and tradeFlag != prior_tradeFlag):
                    # sell signal arrived
                    closeprice = last_price
                    self.closeMarketPositions(closeprice)
                    positionFlag = 0
            #
            if (interval_id > prior_daily_interval_id):
                if (prior_lastprice<=0):
                    prior_lastprice = last_price
                self.refreshPortfolioValue(prior_interval_time,prior_lastprice)
            
            #prepare for the next signal
            prior_daily_interval_id=interval_id
            prior_interval_time=interval_date
            prior_lastprice = last_price
            prior_tradeFlag = tradeFlag

        return 


    def getPortfolioAnnualReturns(self):
        return self.PortfolioAnnualReturns
    def savePortfolioValues(self, filename):
        self.PortfolioValues.to_csv(filename)
    def savePortfolioValueIntoDatabase(self, userid, dbi):
#        self.PortfolioValues = pd.DataFrame(data=None,columns=['trade_date','portfoliovalue','cashvalue'])
        for rowindex, portfolioValueRow in self.PortfolioValues.iterrows():
            trade_date = portfolioValueRow.trade_date
            portfolioValue = portfolioValueRow.portfoliovalue
            cashvalue = portfolioValueRow.cashvalue
            #    exec cycportfolio.[dbo].[sp_catnipportfoliovalues_addPortfolioValue]  	2020	,'	1/1/2018	',	500000.00	,	500000.00
            sql = ("exec cycportfolio.dbo.sp_catnipportfoliovalues_addPortfolioValue %d,'%s',%f,%f")%(userid,trade_date,portfolioValue, cashvalue)
            dbi.executeSQL(sql)              
            #print(sql)
            
        




