# -*- coding: utf-8 -*-
"""
Created on Mon May 14 13:47:25 2018

@author: jzhou
"""
#import numpy
#import platform
#import pymssql
import pandas as pd
from classes.MSSQLUtilities import MSSQLDBInterface


class PortfolioCapitalAllocation(object):
    def __init__(self, portfolioMaxExposure,stockMaxAllocation):
        self.portfolioMaxExposure = portfolioMaxExposure    
        self.stockMaxAllocation = stockMaxAllocation
        self.symbol_marketcap_dict = {}
            
        
    def loadStockMarketCap(self):
        sql="select symbol,marketcap from cycportfolio.[dbo].[SymbolFundamentals]"
        self._dbi=MSSQLDBInterface('SFO-PROD-SQL603','cyccert','cyc','5432')
        self._dbi.connectToDatabase()
        self.symbol_marketcap_dict=self._dbi.getDictDataFromSQL(sql)
        self._dbi.disconnectFromDatabase()    
#        conn_dat603=pymssql.connect(server='SFO-PROD-SQL603',user='cyc',password='5432',database='cyccert')
#        cursor_dat603=conn_dat603.cursor()
#        #load symbol fundamental info
#        sql="select symbol,marketcap from cycportfolio.[dbo].[SymbolFundamentals]"
#        cursor_dat603.execute(sql)
#        symbol_marketcap=cursor_dat603.fetchall()  #symbol_marketcap will be a dictonary ??
#        cursor_dat603.close()
#        conn_dat603.close()
#        self.symbol_marketcap_dict = dict(symbol_marketcap)
        
    # The function below will calculate the dollar investment for a stock 
    def __getInvestmentSizeForStock_MarketCapRuleOn2MillionPortfolio(self,symbol):
        """get the dollar investment allocated for a stock according to the market cap rule on $M portfolio"""    
        allocation = 0  # 10k per billion
#        if (symbol in self.symbol_marketcap_dict.keys()):
#            marketcap = self.symbol_marketcap_dict[symbol]
#            allocation = 1. * int(marketcap)*10000  # 10k per billion
        if (symbol in self.symbol_marketcap_dict.keys()):
            marketcap = self.symbol_marketcap_dict[symbol]
            allocation = 1.0 * int(marketcap) * 10000   #10,000 per billion
        return allocation 

    #  The function below will calculate the dollar allocation for a particular stock for a particular portfolio     
    def getInvestmentSizeForStockOnPortfolio(self,symbol, portfoliovalue,cashvalue):
        """get the dollar investment allocated for a stock for a particular portfolio """
        # -- 1. apply market cap allocation rule 
        stockAllocation = self.__getInvestmentSizeForStock_MarketCapRuleOn2MillionPortfolio(symbol)
        #	-- 1b. limit to only 15% of $2.0 Million portfolio
        #	select @allocation=case when @allocation>300000 then 300000 else @allocation end
        if (stockAllocation>300000):
            stockAllocation = 300000

        #	-- 2. size up to the portfolio 
        #	select @allocation=@allocation*@portfoliovalue/2000000.0
        stockAllocation = stockAllocation * portfoliovalue/2000000.0
        if (stockAllocation>self.stockMaxAllocation*portfoliovalue):
            stockAllocation=self.stockMaxAllocation*portfoliovalue
        #	-- 3. size down according to portfoli Max exposure rule:  the allocation we calculated above is based on 200% investment 
        stockAllocation = stockAllocation * self.portfolioMaxExposure / 2.0 
        
        
        #	-- 4. apply the @maxexposure rule
        if (cashvalue - stockAllocation < (1.0-self.portfolioMaxExposure)*portfoliovalue):
            stockAllocation=cashvalue-(1.0-self.portfolioMaxExposure)*portfoliovalue
        #5 floor to 0 
        if (stockAllocation<0):
            stockAllocation =0 
        
        return stockAllocation 
#***************** end of PortfolioCapitalAllocation class **********************#
        
    
class StockPosition(object):
    def __init__(self, symbol, shares, begindate) :
        # for SHORT position: shares will be NEGATIVE number 
        self.symbol=symbol
        self.shares=shares
        self.begindate=begindate
        #self.enddate=enddate
        self.current_interval_id=0
        self.current_positionvalue=0
        self.loadSymbolDailyPricingdata(self.symbol, self.begindate)
        
    def loadSymbolDailyPricingdata(self,symbol, begindate):            
        self._dbi=MSSQLDBInterface('SFO-PROD-SQL603','cyccert','cyc','5432')        
        self._dbi.connectToDatabase()
        sql="exec cyccert.dbo.[sp_getSymbolDailyPriceDataForPortfolioTesting] '"+str(symbol)+"\',\'"+str(begindate)+"\'"
        self.df_dailydata=self._dbi.getDataFrameFromSQL(sql)
        self.df_dailydata.columns = ['interval_id','high','low','open','last','volume']
        self.df_dailydata.index=self.df_dailydata['interval_id']
        self._dbi.disconnectFromDatabase()
        
    
    def getSymbolPrice(self,interval_id):        
        return float(self.df_dailydata.loc[([interval_id]),'last'])
    
    def getPositionValue(self,interval_id):
        self.current_interval_id=interval_id
        if (interval_id in self.df_dailydata.index):
            self.current_positionvalue=float(self.shares)*float(self.df_dailydata.loc[([interval_id]),'last'])
        else:
            #if we did not have records for the "interval_id", return prior day value 
            self.current_positionvalue=0
        return self.current_positionvalue 
#***************** end of StockPosition class **********************#

                
class Portfolio(object):
    def __init__(self,initialvalue,maxexposure,stockMaxAllocation):
        self.cashvalue=initialvalue
        self.portfoliovalue=initialvalue
        self.shortpositionvalue=0
        self.stockOpenPositions=dict()   # 'symbol'  as the key, value is an array: ['opendate','shares','entryprice', positionObject]
        self.stocktrades=pd.DataFrame(data=None,columns=['opendate', 'symbol', 'entryprice','shares', 'closedate', 'closeprice','openrule','closerule'])

        #prepare the portfolio capital allocation object
        self.PortfolioCapitalAllocation = PortfolioCapitalAllocation(maxexposure,stockMaxAllocation) 
        self.maxexposure = maxexposure 
        self.PortfolioCapitalAllocation.loadStockMarketCap()
        self.PortfolioDailyValues = pd.DataFrame(data=None,columns=['interval_date','portfoliovalue','cashvalue','positioncount'])
        self.totalpositionvalue=0
        self.shortpositionvalue=0
        

    def __existsPositionWithSymbolOpenDate(self,symbol,opendate):
        if symbol in self.stockOpenPositions.keys():
            currentPosition=self.stockOpenPositions[symbol]
            if (currentPosition[0]==opendate):
                return True
            return False
        else:
            return False

    def __existsPosition(self,symbol):
        if symbol in self.stockOpenPositions.keys():
            return True
        else:
            return False
    
    #add a stock position
    def __addPosition(self,symbol,opendate,shares, entryprice):
        if self.__existsPosition(symbol):
            return False  #position already exists
        else:
            position=StockPosition(symbol,shares,opendate)
            if not (shares is None):
                self.stockOpenPositions[symbol]=[opendate,shares, entryprice,position]    
            return True

    def __removePosition(self,symbol):
        self.stockOpenPositions.pop(symbol, None)
        
    def getOpenPositionSizeForSymbol(self,symbol):
        if (symbol in self.stockOpenPositions.keys()):
            return self.stockOpenPositions[symbol][1]
        else:
            return 0
        
    def getOpenPositionCount(self):
        return len(self.stockOpenPositions)
    
    def refreshPortfolioValue(self, interval_id,updatedate):
        #refresh the portfolio value 
        #loop through all positions
        totalpositionvalue=float(0)
        totalshortpositionvalue=float(0)
        
        for key in self.stockOpenPositions:            
            positionvalue=self.stockOpenPositions[key][3].getPositionValue(interval_id)
            print(("symbol:%s  shares:%d  positionvalue:%f") % (key, self.stockOpenPositions[key][1],positionvalue))           
#            print (key)
#            print (positionvalue)
            if not (positionvalue is None):
                totalpositionvalue += float(positionvalue)
            else:
                self.debugvalue=2
            
            #update short position value
            if (self.stockOpenPositions[key][1]<0):
                totalshortpositionvalue=totalshortpositionvalue-positionvalue
                
        self.portfoliovalue=self.cashvalue + totalpositionvalue
        self.PortfolioDailyValues= self.PortfolioDailyValues.append({'interval_date':updatedate,'portfoliovalue':self.portfoliovalue,'cashvalue':self.cashvalue,'positioncount':self.getOpenPositionCount()},ignore_index=True)
        self.shortpositionvalue=totalshortpositionvalue
        self.totalpositionvalue=totalpositionvalue
        
    def getPortfolioShortPositionValue(self):
        return self.shortpositionvalue

    def getPortfolioLongPositoinValue(self):
        longPositionValue=self.totalpositionvalue+self.shortpositionvalue
        return longPositionValue

    def __openStockPosition(self,symbol,opendate,openrule,shares,entryprice):        
        #step1: add the stock positions
        positionAdded =  self.__addPosition(symbol,opendate,shares, entryprice)
        #step 2:  add trade
        if (positionAdded and not (shares is None)):
            #step3:  update cash value
            self.cashvalue -= float(shares)*entryprice   #adjust cash value
            #step 4:  add trades
            self.stocktrades = self.stocktrades.append({'opendate':opendate,'symbol':symbol,'entryprice':entryprice,'shares':shares,'openrule':openrule,'closedate':'2020-1-1','closeprice':0,'closerule':0},ignore_index=True)
            #step 5:  reporting
            if (shares>0):            
                print (('  - Bought %d shares of stock %s @ %.2f !  Total open position count:%d Total trade # so far:%d Cashleft:%d') % (shares,symbol,entryprice, self.getOpenPositionCount(),self.stocktrades['opendate'].count(),self.cashvalue))
            
            if (shares<0):
                print (('  - Short %d shares of stock %s @ %.2f !  Total open position count:%d Total trade # so far:%d Cashleft:%d') % ((int)(-1*shares),symbol,entryprice, self.getOpenPositionCount(),self.stocktrades['opendate'].count(),self.cashvalue))
            
            return True
        else:
            print ('  -  Failed to add position!  ')
            return False

    def __closeStockPosition(self,symbol,opendate,closedate,closeprice,closerule):
        if (self.__existsPositionWithSymbolOpenDate(symbol,opendate)):
            shares=self.stockOpenPositions[symbol][1]
            
            #update trades
            self.stocktrades.loc[(self.stocktrades['symbol']==symbol)&(self.stocktrades['opendate']==opendate),'closedate']=closedate
            self.stocktrades.loc[(self.stocktrades['symbol']==symbol)&(self.stocktrades['opendate']==opendate),'closeprice']=closeprice
            
            #remove the position
            self.__removePosition(symbol)
            
            #update cash value
            self.cashvalue += float(shares)*closeprice
            
            #reporting
            if (shares>0):
                print (('Sold stock!  Open position count:%d Total trade count:%d Cashleft:%d') % (self.getOpenPositionCount(),self.stocktrades['opendate'].count(),self.cashvalue))
            if (shares<0):
                print (('Cover stock!  Open position count:%d Total trade count:%d Cashleft:%d') % (self.getOpenPositionCount(),self.stocktrades['opendate'].count(),self.cashvalue))
            
            return True
        else:
            #print ('Failed to sell stock!')
            return False
        


    def buystockWithAllocation(self,symbol,opendate,entryprice,selldate,openrule,allocation):
        # return false if position already exists
        if (self.__existsPosition(symbol)):
            print ('  -  Position already exists, failed to follow buy signal !')
            return False
        
        #step 1: determine the stock allocation 
        shares = int(allocation/entryprice)
        if (shares<=0):
            print ('  -  Portfolio was maxed out, failed to follow buy signal !')
            return False;
        
        return self.__openStockPosition(symbol,opendate,openrule,shares,entryprice)
        
    def buystock(self,symbol,opendate,entryprice,selldate,openrule):
        # return false if position already exists
        if (self.__existsPosition(symbol)):
            print ('  -  Position already exists, failed to follow buy signal !')
            return False
        
        #step 1: determine the stock allocation 
        allocation = self.PortfolioCapitalAllocation.getInvestmentSizeForStockOnPortfolio(symbol,self.portfoliovalue,self.cashvalue)
        allocation = self.PortfolioCapitalAllocation.getInvestmentSizeForStockOnPortfolio(symbol,self.portfoliovalue,self.cashvalue-self.shortpositionvalue)

        #step 2:  MAX exposure test         
        portfolioLongValue=self.getPortfolioLongPositoinValue()
        portfolioShortValue=self.getPortfolioShortPositionValue()
        #if (portfolioShortValue+portfolioLongValue+allocation>2.0*self.portfoliovalue):
        #    allocation=2.0*self.portfoliovalue-(portfolioShortValue+portfolioLongValue)   #make sure long+short<200%*portfoliovalue
        if (portfolioLongValue+allocation>2.0*self.portfoliovalue):
            allocation=2.0*self.portfoliovalue-(portfolioLongValue)   #make sure long+short<200%*portfoliovalue
        
        if (allocation<float(0.001)*self.portfoliovalue):
            print ('  -  Portfolio was maxed out, failed to follow buy signal !')
            return False;
        
        shares = int(allocation/entryprice)
        if (shares<=100):
            print ('  -  Portfolio was maxed out, failed to follow buy signal !')
            return False;
        
        #step2: add the stock long positions
        return self.__openStockPosition(symbol,opendate,openrule,shares,entryprice)
           
        
    def shortStockWithAllocation(self,symbol,opendate,entryprice,openrule,allocation):
        # return false if position already exists
        if (self.__existsPosition(symbol)):
            print ('  -  Position already exists, failed to follow buy signal !')
            return False
        
        #step 1: determine the stock allocation 
        if (allocation<500.0):
            return False
        
        shares = int(float(allocation)/float(entryprice))
        if (shares<=0):
            print ('  -  Portfolio was maxed out, failed to follow buy signal !')
            return False
        shares = -1 * shares 
        
        #step2: add the stock long positions
        return self.__openStockPosition(symbol,opendate,openrule,shares,entryprice)
        
    def shortStock(self,symbol,opendate,entryprice,openrule):
        return self.shortStockWithAllocationPct(symbol,opendate,entryprice,openrule,0.1)

    def shortStockWithAllocationPct(self,symbol,opendate,entryprice,openrule,allocationPct):
        if (self.__existsPosition(symbol)):
            print ('  There was a position for the stock, failed to follow short signal !')
            return False

        #determine allocations
#        allocation = self.PortfolioCapitalAllocation.getInvestmentSizeForStockOnPortfolio(symbol,self.portfoliovalue,self.cashvalue)
        allocation = allocationPct*self.portfoliovalue         
        
        portfolioLongValue=self.getPortfolioLongPositoinValue()
        portfolioShortValue=self.getPortfolioShortPositionValue()
        
        #if (portfolioShortValue+allocation>=portfolioLongValue):
        #    allocation = portfolioLongValue-portfolioShortValue   #make sure short no more than long value 
        
        #if (portfolioShortValue+portfolioLongValue+allocation>3.0*self.portfoliovalue):
        #    allocation=3.0*self.portfoliovalue-(portfolioShortValue+portfolioLongValue)   #make sure long+short<300%*portfoliovalue
        
        #short stock with allocations
        print(('Portfolio value:%f Portfolio long position value: %f  Portfolio Short positon value: %f New Short allocation:%f')%(self.portfoliovalue, portfolioLongValue,portfolioShortValue,allocation))
        return self.shortStockWithAllocation(symbol,opendate,entryprice,openrule,allocation)


    def coverStock(self,symbol,shortdate,coverdate,coverprice,coverrule):
        return self.__closeStockPosition(symbol,shortdate,coverdate,coverprice,coverrule)
               
    def sellstock(self,symbol,opendate,closedate,sellprice):
        return self.__closeStockPosition(symbol,opendate,closedate,sellprice,0)
#***************  end of Portfolio class ******************

