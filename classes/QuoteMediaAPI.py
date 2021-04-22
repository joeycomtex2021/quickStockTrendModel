# -*- coding: utf-8 -*-
"""
Created on 2020-07-10
This module includes all the QuoteMedeia related functions and classes 
@author: jzhou
"""
import datetime
import json
import os
import requests 
import xml.etree.ElementTree as ET 
from classes.Logger import Logger
from classes.MSSQLUtilities import MSSQLDBInterface


def validateDataItem(dataItem):
    if (dataItem is None):
        return None
    #1. replace the ' with ''
    dataItem=dataItem.replace("'","''")
    return dataItem

def getChildNodeText(parentNode, childNodeTag):
    childNodes = parentNode.findall(childNodeTag)
    if not childNodes is None: 
        if (len(childNodes)>0):
            if (childNodes[0] is not None):
                return childNodes[0].text
    return ""

def getChildNodeTextAndAttr(parentNode, childNodeTag,attrname):
    childNodes = parentNode.findall(childNodeTag)
    if not childNodes is None: 
        if (len(childNodes)>0):
            if (childNodes[0] is not None):
                if (attrname in childNodes[0].attrib):
                    return (childNodes[0].text, childNodes[0].attrib[attrname])
    return ()
def replaceEmptyStringAsNone(value):
    if (value == ""):
        return "None"
    else:
        return value
#    return lambda value : value if (value == "") else None
def getDictDataKeyValue(dict_data, keyname):
    if (dict_data is None):
        return None 
    if (keyname not in dict_data.keys()):
        return None
    return validateDataItem(dict_data[keyname])

def validateCompanyData(companyData,keyname):
    if (companyData is None):
        return
    
    if (keyname not in companyData.keys()):
        companyData[keyname]='None'
    else:
        # string validation
        data=companyData[keyname]
        companyData[keyname]=validateDataItem(data)
    return

def qm_parseXMLBatsQuoteDataFromFile(xmlDataFile): 
     # create element tree object 
    #root = ET.fromstring(country_data_as_string)  #parse from string 
    tree = ET.parse(xmlDataFile) #parse from file 
      # get root element 
    root = tree.getroot() 
    # create empty list for news items 
    priceDataLists = [] 
 
    # iterate news items 
    for child in root:
        if (child.tag == "quote"):
            #parsing a quote
            attr=child.attrib
            symbol_bats=attr['symbolstring']
            symbol = symbol_bats.replace(":EGX","")
            priceData = {}
            priceData['symbol_bats']=symbol_bats
            priceData['symbol']=symbol
            
            
            #getChildNodeText
            for quoteChild in child: 
                if (quoteChild.tag == 'key'):
                    exchange = getChildNodeText(quoteChild,'exchange')
                    priceData['exchange']=exchange
                    
                if (quoteChild.tag == 'pricedata'):
                    for quote in quoteChild:
                        priceData[quote.tag]=quote.text
            #
            print(("symbol:%s last:%s") %(priceData['symbol'],priceData['last']))
            priceDataLists.append(priceData)
    return priceDataLists 



#save company  data into database
def qm_saveCompanyDescriptionIntoDatabase(companyData,dbi):
    if (companyData is None or dbi is None):
        return
    
    if ( ('symbol' in companyData) 
        ):
            symbol = companyData['symbol']
            print(("symbol:%s")%(symbol))
            validateCompanyData(companyData,'exchange')     
            validateCompanyData(companyData,'longdescription') 
            
            
            sql = " exec cycreal.dbo.sp_QuoteMediaSymbols_updateSymbolLongDescription " 
            sql =("%s '%s'")%(sql,symbol)
            sql =("%s ,'%s'")%(sql,companyData['exchange'])
            sql = ("%s, '%s'")%(sql,companyData['longdescription'])
            #REPLACED 'None' to NULL
            sql = sql.replace("'None'","NULL")
            #sql = sql.replace(","''")
            
            if (dbi.executeSQL(sql)):
                print("b.")
            else:
                print(("Sql failed! sql:%s")%(sql))
                file =open('sqlerror.log',"a",encoding="utf-8")
                file.write(sql) 
                file.close() 


    return

#save company  data into database
def qm_saveCompanyBasicInfoIntoDatabase(companyData,dbi):
    if (companyData is None or dbi is None):
        return
    
    if ( ('symbol' in companyData) 
        ):
            symbol = companyData['symbol']
            print(("symbol:%s")%(symbol))
            validateCompanyData(companyData,'exchange')     
            validateCompanyData(companyData,'exLgName') 
            validateCompanyData(companyData,'exShName') 
            validateCompanyData(companyData,'longname') 
            validateCompanyData(companyData,'shortname') 
            validateCompanyData(companyData,'shortdescription') 
            validateCompanyData(companyData,'longdescription') 
            validateCompanyData(companyData,'address_city') 
            validateCompanyData(companyData,'address_state') 
            validateCompanyData(companyData,'address_country') 
            validateCompanyData(companyData,'address_postcode') 
            validateCompanyData(companyData,'website') 
            validateCompanyData(companyData,'email') 
            validateCompanyData(companyData,'ceo') 
            validateCompanyData(companyData,'employees') 
            validateCompanyData(companyData,'auditor') 
            validateCompanyData(companyData,'marketcap') 
            validateCompanyData(companyData,'sector') 
            validateCompanyData(companyData,'industry') 
            
            validateCompanyData(companyData,'qmid') 
            validateCompanyData(companyData,'qmdescription') 
            validateCompanyData(companyData,'cik') 
            validateCompanyData(companyData,'naics') 
            validateCompanyData(companyData,'sics') 
            validateCompanyData(companyData,'annualinfo_latestfiscaldate') 
            validateCompanyData(companyData,'annualinfo_latestfiscalrevenue') 
            validateCompanyData(companyData,'annualinfo_latestfiscalEPS') 
            validateCompanyData(companyData,'annualinfo_latestfiscaldividendspershare') 
            validateCompanyData(companyData,'priceinfo_weeks52high') 
            validateCompanyData(companyData,'priceinfo_weeks52low') 
            validateCompanyData(companyData,'priceinfo_weeks52change') 
            validateCompanyData(companyData,'priceinfo_day21movingavg') 
            validateCompanyData(companyData,'priceinfo_day50movingavg') 
            validateCompanyData(companyData,'priceinfo_day200movingavg') 
            validateCompanyData(companyData,'priceinfo_day21ema') 
            validateCompanyData(companyData,'priceinfo_day50ema') 
            validateCompanyData(companyData,'priceinfo_day200ema') 
            validateCompanyData(companyData,'priceinfo_alpha') 
            validateCompanyData(companyData,'priceinfo_beta1year') 
            validateCompanyData(companyData,'priceinfo_beta3year') 
            validateCompanyData(companyData,'priceinfo_beta') 
            validateCompanyData(companyData,'priceinfo_r2') 
            validateCompanyData(companyData,'priceinfo_stddev1year') 
            validateCompanyData(companyData,'priceinfo_stddev3year') 
            validateCompanyData(companyData,'priceinfo_stddev') 
            validateCompanyData(companyData,'priceinfo_periods') 
            validateCompanyData(companyData,'chg_day7') 
            validateCompanyData(companyData,'chg_day7pct') 
            validateCompanyData(companyData,'chg_day21') 
            validateCompanyData(companyData,'chg_day21pct') 
            validateCompanyData(companyData,'chg_day30') 
            validateCompanyData(companyData,'chg_day30pct') 
            validateCompanyData(companyData,'chg_day90') 
            validateCompanyData(companyData,'chg_day90pct') 
            validateCompanyData(companyData,'chg_day180') 
            validateCompanyData(companyData,'chg_day180pct') 
            validateCompanyData(companyData,'chg_day200') 
            validateCompanyData(companyData,'chg_day200pct') 
            validateCompanyData(companyData,'chg_monthtodate') 
            validateCompanyData(companyData,'chg_monthtodatepct') 
            validateCompanyData(companyData,'chg_quartertodate') 
            validateCompanyData(companyData,'chg_quartertodatepct') 
            validateCompanyData(companyData,'chg_yeartodate') 
            validateCompanyData(companyData,'chg_yeartodatepct') 
            validateCompanyData(companyData,'volume_avgvol10days') 
            validateCompanyData(companyData,'volume_avgvol20days') 
            validateCompanyData(companyData,'volume_avgvol30days') 
            validateCompanyData(companyData,'volume_avgvol50days') 
            validateCompanyData(companyData,'volume_outstanding') 
            validateCompanyData(companyData,'volume_shareclasslevelsharesoutstanding') 
            validateCompanyData(companyData,'volume_totalsharesoutstanding') 
            validateCompanyData(companyData,'volume_= ') 
            validateCompanyData(companyData,'volume_shareturnover1year') 
            validateCompanyData(companyData,'volume_short= ') 
            validateCompanyData(companyData,'volume_short= ratio') 
            validateCompanyData(companyData,'volume_pct= ') 
            validateCompanyData(companyData,'volume_shortdate') 
            validateCompanyData(companyData,'volume_epsvar10year') 
            
            validateCompanyData(companyData,'holding_instituteholdingspct') 
            validateCompanyData(companyData,'holding_instituteholdingsdate') 
            validateCompanyData(companyData,'holding_instituteboughtprev3mos') 
            validateCompanyData(companyData,'holding_institutesoldprev3mos') 
            validateCompanyData(companyData,'holding_totalheld') 
            validateCompanyData(companyData,'holding_insidersharesowned') 
            validateCompanyData(companyData,'holding_institutions') 
            validateCompanyData(companyData,'holding_insiderholdingspct') 
            validateCompanyData(companyData,'holding_insiderholdingsdate') 
            validateCompanyData(companyData,'holding_insiderboughtprev3mos') 
            validateCompanyData(companyData,'holding_insidersoldprev3mos') 
            
            validateCompanyData(companyData,'income_revenue') 
            validateCompanyData(companyData,'income_revenuepershare') 
            validateCompanyData(companyData,'income_revenue3years') 
            validateCompanyData(companyData,'income_revenue5years') 
            validateCompanyData(companyData,'financial_quickratio') 
            validateCompanyData(companyData,'financial_currentratio') 
            validateCompanyData(companyData,'financial_longtermdebttocapital') 
            validateCompanyData(companyData,'financial_totaldebttoequity') 
            validateCompanyData(companyData,'financial_= coverage') 
            validateCompanyData(companyData,'financial_leverageratio') 
            validateCompanyData(companyData,'management_returnonequity') 
            validateCompanyData(companyData,'management_returnoncapital') 
            validateCompanyData(companyData,'management_returnonassets') 
            validateCompanyData(companyData,'valuation_peratio') 
            validateCompanyData(companyData,'valuation_pehighlast5years') 
            validateCompanyData(companyData,'valuation_pelowlast5years') 
            validateCompanyData(companyData,'valuation_pricetosales') 
            validateCompanyData(companyData,'valuation_pricetobook') 
            validateCompanyData(companyData,'valuation_pricetotangiblebook') 
            validateCompanyData(companyData,'valuation_pricetocashflow') 
            validateCompanyData(companyData,'valuation_pricetofreecash') 
            validateCompanyData(companyData,'profitability_grossmargin') 
            validateCompanyData(companyData,'profitability_ebitmargin') 
            
            validateCompanyData(companyData,'profitability_ebitdamargin') 
            validateCompanyData(companyData,'profitability_pretaxprofitmargin') 
            validateCompanyData(companyData,'profitability_profitmargincont') 
            validateCompanyData(companyData,'profitability_profitmarg= ot') 
            validateCompanyData(companyData,'profitability_profitvar10year') 
            validateCompanyData(companyData,'assets_assetsturnover') 
            validateCompanyData(companyData,'assets_invoiceturnover') 
            validateCompanyData(companyData,'assets_receivablesturnover') 
            
            
            
            sql = " exec cycreal.dbo.sp_QuoteMediaSymbols_updateSymbolFundamentals " 
            sql =("%s '%s'")%(sql,symbol)
            sql =("%s ,'%s'")%(sql,companyData['exchange'])
            sql =("%s ,'%s'")%(sql,companyData['exLgName'])
            sql =("%s ,'%s'")%(sql,companyData['exShName'])
            sql =("%s ,'%s'")%(sql,companyData['longname'])
            sql = ("%s, '%s'")%(sql,companyData['shortname'])
            sql = ("%s, '%s'")%(sql,companyData['shortdescription'])
            #sql = ("%s, '%s'")%(sql,companyData['longdescription'])
            
            sql = ("%s, '%s'")%(sql,companyData['address_city'])
            sql = ("%s, '%s'")%(sql,companyData['address_state'])
            sql = ("%s, '%s'")%(sql,companyData['address_country'])
            sql = ("%s, '%s'")%(sql,companyData['address_postcode'])
            sql = ("%s, '%s'")%(sql,companyData['website'])
            sql = ("%s, '%s'")%(sql,companyData['email'])
            sql = ("%s, '%s'")%(sql,companyData['ceo'])
            sql = ("%s, '%s'")%(sql,companyData['employees'])
            sql = ("%s, '%s'")%(sql,companyData['auditor'])
            sql = ("%s, '%s'")%(sql,companyData['marketcap'])
            sql = ("%s, '%s'")%(sql,companyData['sector'])
            sql = ("%s, '%s'")%(sql,companyData['industry'])
        
            sql = ("%s, '%s'")%(sql,companyData['qmid'])
            sql = ("%s, '%s'")%(sql,companyData['qmdescription'])
            sql = ("%s, '%s'")%(sql,companyData['cik'])
            sql = ("%s, '%s'")%(sql,companyData['naics'])
            sql = ("%s, '%s'")%(sql,companyData['sics'])
            sql = ("%s, '%s'")%(sql,companyData['annualinfo_latestfiscaldate'])
            sql = ("%s, '%s'")%(sql,companyData['annualinfo_latestfiscalrevenue'])
            sql = ("%s, '%s'")%(sql,companyData['annualinfo_latestfiscalEPS'])
            sql = ("%s, '%s'")%(sql,companyData['annualinfo_latestfiscaldividendspershare'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_weeks52high'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_weeks52low'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_weeks52change'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_day21movingavg'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_day50movingavg'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_day200movingavg'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_day21ema'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_day50ema'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_day200ema'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_alpha'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_beta1year'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_beta3year'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_beta'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_r2'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_stddev1year'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_stddev3year'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_stddev'])
            sql = ("%s, '%s'")%(sql,companyData['priceinfo_periods'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day7'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day7pct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day21'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day21pct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day30'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day30pct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day90'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day90pct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day180'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day180pct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day200'])
            sql = ("%s, '%s'")%(sql,companyData['chg_day200pct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_monthtodate'])
            sql = ("%s, '%s'")%(sql,companyData['chg_monthtodatepct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_quartertodate'])
            sql = ("%s, '%s'")%(sql,companyData['chg_quartertodatepct'])
            sql = ("%s, '%s'")%(sql,companyData['chg_yeartodate'])
            sql = ("%s, '%s'")%(sql,companyData['chg_yeartodatepct'])
            sql = ("%s, '%s'")%(sql,companyData['volume_avgvol10days'])
            sql = ("%s, '%s'")%(sql,companyData['volume_avgvol20days'])
            sql = ("%s, '%s'")%(sql,companyData['volume_avgvol30days'])
            sql = ("%s, '%s'")%(sql,companyData['volume_avgvol50days'])
            sql = ("%s, '%s'")%(sql,companyData['volume_outstanding'])
            sql = ("%s, '%s'")%(sql,companyData['volume_shareclasslevelsharesoutstanding'])
            sql = ("%s, '%s'")%(sql,companyData['volume_totalsharesoutstanding'])
            sql = ("%s, '%s'")%(sql,companyData['volume_float'])
            sql = ("%s, '%s'")%(sql,companyData['volume_shareturnover1year'])
            sql = ("%s, '%s'")%(sql,companyData['volume_shortint'])
            sql = ("%s, '%s'")%(sql,companyData['volume_shortintratio'])
            sql = ("%s, '%s'")%(sql,companyData['volume_pctfloat'])
            sql = ("%s, '%s'")%(sql,companyData['volume_shortdate'])
            sql = ("%s, '%s'")%(sql,companyData['volume_epsvar10year'])
        
            sql = ("%s, '%s'")%(sql,companyData['holding_instituteholdingspct'])
            sql = ("%s, '%s'")%(sql,companyData['holding_instituteholdingsdate'])
            sql = ("%s, '%s'")%(sql,companyData['holding_instituteboughtprev3mos'])
            sql = ("%s, '%s'")%(sql,companyData['holding_institutesoldprev3mos'])
            sql = ("%s, '%s'")%(sql,companyData['holding_totalheld'])
            sql = ("%s, '%s'")%(sql,companyData['holding_insidersharesowned'])
            sql = ("%s, '%s'")%(sql,companyData['holding_institutions'])
            sql = ("%s, '%s'")%(sql,companyData['holding_insiderholdingspct'])
            sql = ("%s, '%s'")%(sql,companyData['holding_insiderholdingsdate'])
            sql = ("%s, '%s'")%(sql,companyData['holding_insiderboughtprev3mos'])
            sql = ("%s, '%s'")%(sql,companyData['holding_insidersoldprev3mos'])
        
            sql = ("%s, '%s'")%(sql,companyData['income_revenue'])
            sql = ("%s, '%s'")%(sql,companyData['income_revenuepershare'])
            sql = ("%s, '%s'")%(sql,companyData['income_revenue3years'])
            sql = ("%s, '%s'")%(sql,companyData['income_revenue5years'])
            sql = ("%s, '%s'")%(sql,companyData['financial_quickratio'])
            sql = ("%s, '%s'")%(sql,companyData['financial_currentratio'])
            sql = ("%s, '%s'")%(sql,companyData['financial_longtermdebttocapital'])
            sql = ("%s, '%s'")%(sql,companyData['financial_totaldebttoequity'])
            sql = ("%s, '%s'")%(sql,companyData['financial_intcoverage'])
            sql = ("%s, '%s'")%(sql,companyData['financial_leverageratio'])
            sql = ("%s, '%s'")%(sql,companyData['management_returnonequity'])
            sql = ("%s, '%s'")%(sql,companyData['management_returnoncapital'])
            sql = ("%s, '%s'")%(sql,companyData['management_returnonassets'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_peratio'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_pehighlast5years'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_pelowlast5years'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_pricetosales'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_pricetobook'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_pricetotangiblebook'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_pricetocashflow'])
            sql = ("%s, '%s'")%(sql,companyData['valuation_pricetofreecash'])
            sql = ("%s, '%s'")%(sql,companyData['profitability_grossmargin'])
            sql = ("%s, '%s'")%(sql,companyData['profitability_ebitmargin'])
        
            sql = ("%s, '%s'")%(sql,companyData['profitability_ebitdamargin'])
            sql = ("%s, '%s'")%(sql,companyData['profitability_pretaxprofitmargin'])
            sql = ("%s, '%s'")%(sql,companyData['profitability_profitmargincont'])
            sql = ("%s, '%s'")%(sql,companyData['profitability_profitmargintot'])
            sql = ("%s, '%s'")%(sql,companyData['profitability_profitvar10year'])
            sql = ("%s, '%s'")%(sql,companyData['assets_assetsturnover'])
            sql = ("%s, '%s'")%(sql,companyData['assets_invoiceturnover'])
            sql = ("%s, '%s'")%(sql,companyData['assets_receivablesturnover'])
            sql = ("%s, '%s'")%(sql,companyData['longdescription'])

            #REPLACED 'None' to NULL
            sql = sql.replace("'None'","NULL")
            
            if (dbi.executeSQL(sql)):
                print("b.")
            else:
                print(("Sql failed! sql:%s")%(sql))
#                file =open('sqlerror.log',"a",encoding="utf-8")
#                file.write(sql) 
#                file.close() 


    return
        
#save company  data into database
def qm_saveMiniQuotesIntoDatabase(miniQuotes,dbi):
    if (miniQuotes is None or dbi is None):
        return
    
    if ( ('symbol' in miniQuotes.keys()) 
        ):
        for symbol in miniQuotes.keys():
            miniQuote = miniQuotes[symbol]
            if (miniQuote is not None):
                #save quote
                    miniQuote['last']=last
                    miniQuote['open']=open
                    miniQuote['high']=high
                    miniQuote['low']=low
                    miniQuote['bid']=bid
                    miniQuote['ask']=ask
                    miniQuote['bidsize']=bidsize
                    miniQuote['asksize']=asksize
                    miniQuote['sharevolume']=sharevolume
                    miniQuote['lasttradedatetime']=lasttradedatetime
                
        
        
def qm_parseXMLMiniQuotesFromFile(xmlDataFile): 
    if (xmlDataFile is None):
        return None
    if  not (os.path.exists(xmlDataFile)):
        return None
    
    #
    tree = ET.parse(xmlDataFile) #parse from file 
    root = tree.getroot() 
    # create empty list for 
    miniQuotes = {}
 
    # iterate news items 
    for rootChild in root:
        if (rootChild.tag == "symbolcount"):
            symbolcount = rootChild.text
            if (int(symbolcount)<=0):
                return miniQuotes
            
        if (rootChild.tag == "quote"):
            #it  is a quote
            attr=rootChild.attrib
            symbol=attr['symbolstring']
            miniQuote = {}
            for quoteChild in rootChild: 
                #parse the quote 
                if (quoteChild.tag == "pricedata"):
                    #parse price data
                        #<last>95.2</last>
                        #<open>95.94</open>
                        #<high>96.196</high>
                        #<low>94.88</low>
                        #<bid>95.19</bid>
                        #<ask>95.23</ask>
                        #<bidsize>200</bidsize>
                        #<asksize>100</asksize>
                        #<sharevolume>439639</sharevolume>
                        #<lasttradedatetime>2020-07-31T11:44:59-04:00</lasttradedatetime>
                    last=getChildNodeText(quoteChild,"last")
                    open=getChildNodeText(quoteChild,"open")
                    high=getChildNodeText(quoteChild,"high")
                    low=getChildNodeText(quoteChild,"low")
                    bid=getChildNodeText(quoteChild,"bid")
                    ask=getChildNodeText(quoteChild,"ask")
                    bidsize=getChildNodeText(quoteChild,"bidsize")
                    asksize=getChildNodeText(quoteChild,"asksize")
                    sharevolume=getChildNodeText(quoteChild,"sharevolume")
                    lasttradedatetime=getChildNodeText(quoteChild,"lasttradedatetime")
                    
                    miniQuote['last']=last
                    miniQuote['open']=open
                    miniQuote['high']=high
                    miniQuote['low']=low
                    miniQuote['bid']=bid
                    miniQuote['ask']=ask
                    miniQuote['bidsize']=bidsize
                    miniQuote['asksize']=asksize
                    miniQuote['sharevolume']=sharevolume
                    miniQuote['lasttradedatetime']=lasttradedatetime
                    
                    miniQuotes[symbol]=miniQuote
    return miniQuotes 


def qm_saveHistoryDataIntoDatabase_v2(exchange,eodData,dbi):
    if (eodData is None):
        return
    if (dbi is None):
        return 
    
        #dbo.sp_QuoteMedia_History_refreshData 
        #	@symbol varchar(10)
        #,   @tradedate datetime 
        #,   @openPrice float
        #,   @highPrice float
        #,   @lowPrice float
        #,   @lastPrice float
        #,   @volume  float
        #,   @amount float
        #,   @totaltrades float
        #,	@vwap float
        #,   @change float
        #,   @changepercent float
        #,   @bid float
        #,	@ask float
        #,	@bidsize float
        #,	@asksize float

        
    sql = "exec cycreal.dbo.sp_QuoteMedia_History_refreshData_v2 "
    sql = ("%s '%s'")%(sql , exchange)
    sql = ("%s ,'%s'")%(sql , eodData['symbol'])
    sql = ("%s ,'%s'")%(sql , eodData['tradedate'])
    sql = ("%s ,%s")%(sql , eodData['open_price'])
    sql = ("%s ,%s")%(sql , eodData['high_price'])
    sql = ("%s ,%s")%(sql , eodData['low_price'])
    sql = ("%s ,%s")%(sql , eodData['last_price'])
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['sharevolume']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['totalvalue']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['totaltrades']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['vwap']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['change']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['changepercent']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['bid']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['ask']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['bidsize']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['asksize']))

    sql = sql.replace("'None'","NULL")
    sql = sql.replace("None","NULL")
    if (dbi.executeSQL(sql)):
        print("\b.")
    else:
        print(("Sql failed! sql:%s")%(sql))
    return
    

def qm_saveHistoryDataIntoDatabase(eodData,dbi):
    if (eodData is None):
        return
    if (dbi is None):
        return 
    
        #dbo.sp_QuoteMedia_History_refreshData 
        #	@symbol varchar(10)
        #,   @tradedate datetime 
        #,   @openPrice float
        #,   @highPrice float
        #,   @lowPrice float
        #,   @lastPrice float
        #,   @volume  float
        #,   @amount float
        #,   @totaltrades float
        #,	@vwap float
        #,   @change float
        #,   @changepercent float
        #,   @bid float
        #,	@ask float
        #,	@bidsize float
        #,	@asksize float

        
    sql = "exec cycreal.dbo.sp_QuoteMedia_History_refreshData "
    sql = ("%s '%s'")%(sql , eodData['symbol'])
    sql = ("%s ,'%s'")%(sql , eodData['tradedate'])
    sql = ("%s ,%s")%(sql , eodData['open_price'])
    sql = ("%s ,%s")%(sql , eodData['high_price'])
    sql = ("%s ,%s")%(sql , eodData['low_price'])
    sql = ("%s ,%s")%(sql , eodData['last_price'])
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['sharevolume']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['totalvalue']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['totaltrades']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['vwap']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['change']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['changepercent']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['bid']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['ask']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['bidsize']))
    sql = ("%s ,%s")%(sql , replaceEmptyStringAsNone(eodData['asksize']))

    sql = sql.replace("'None'","NULL")
    sql = sql.replace("None","NULL")
    if (dbi.executeSQL(sql)):
        print("\b.")
    else:
        print(("Sql failed! sql:%s")%(sql))
    return
    

def qm_parseXMLHistoryDataFromFile(xmlDataFile):
    if ((xmlDataFile is None) or len(xmlDataFile)<=0):
        return {}
    
     # create element tree object 
    tree = ET.parse(xmlDataFile) #parse from file 
    root = tree.getroot() 
    historyDataLists = {}
 
    # iterate news items 
    for rootChild in root:
        if (rootChild.tag == "history"):
            #got into "history" node parsing "eoddata"
            attr=rootChild.attrib
            symbol=attr['symbolstring']
            
            for historyChild in rootChild: 
                if (historyChild.tag == "eoddata"):
                    #historyData={}
                    eodData={}
                    #symbolinfo node of company
                    attr=historyChild.attrib
                    tradeDate=attr['date']
                    openPrice = getChildNodeText(historyChild,'open')
                    highPrice = getChildNodeText(historyChild,'high')
                    lowPrice = getChildNodeText(historyChild,'low')
                    closePrice = getChildNodeText(historyChild,'close')
                    vwap = getChildNodeText(historyChild,'vwap')
                    change = getChildNodeText(historyChild,'change')
                    changepercent = getChildNodeText(historyChild,'changepercent')
                    sharevolume = getChildNodeText(historyChild,'sharevolume')
                    totalvalue = getChildNodeText(historyChild,'totalvalue')
                    totaltrades = getChildNodeText(historyChild,'totaltrades')
                    bid = getChildNodeText(historyChild,'bid')
                    ask = getChildNodeText(historyChild,'ask')
                    bidsize = getChildNodeText(historyChild,'bidsize')
                    asksize = getChildNodeText(historyChild,'asksize')
                    
                    eodData['symbol']=symbol
                    eodData['tradedate']=tradeDate
                    eodData['open_price']=openPrice
                    eodData['high_price']=highPrice
                    eodData['low_price']=lowPrice
                    eodData['last_price']=closePrice
                    eodData['vwap']=vwap
                    eodData['change']=change
                    eodData['changepercent']=changepercent
                    eodData['sharevolume']=sharevolume
                    eodData['totalvalue']=totalvalue
                    eodData['totaltrades']=totaltrades
                    eodData['bid']=bid
                    eodData['ask']=ask
                    eodData['bidsize']=bidsize
                    eodData['asksize']=asksize
                    
                    historyDataLists[tradeDate]=eodData    
                    
    return historyDataLists 


def qm_parseMultipleXMLHistoryDataFromFile(xmlDataFile):
    if ((xmlDataFile is None) or len(xmlDataFile)<=0):
        return {}
    
     # create element tree object 
    tree = ET.parse(xmlDataFile) #parse from file 
    root = tree.getroot() 
    historyDataLists = {}
 
    # iterate news items 
    for rootChild in root:
        if (rootChild.tag == "history"):
            #got into "history" node parsing "eoddata"
            attr=rootChild.attrib
            symbol=attr['symbolstring']
            eodData={}
            
            for historyChild in rootChild: 
                if (historyChild.tag == "eoddata"):
                    #historyData={}
                    #symbolinfo node of company
                    attr=historyChild.attrib
                    tradeDate=attr['date']
                    openPrice = getChildNodeText(historyChild,'open')
                    highPrice = getChildNodeText(historyChild,'high')
                    lowPrice = getChildNodeText(historyChild,'low')
                    closePrice = getChildNodeText(historyChild,'close')
                    vwap = getChildNodeText(historyChild,'vwap')
                    change = getChildNodeText(historyChild,'change')
                    changepercent = getChildNodeText(historyChild,'changepercent')
                    sharevolume = getChildNodeText(historyChild,'sharevolume')
                    totalvalue = getChildNodeText(historyChild,'totalvalue')
                    totaltrades = getChildNodeText(historyChild,'totaltrades')
                    bid = getChildNodeText(historyChild,'bid')
                    ask = getChildNodeText(historyChild,'ask')
                    bidsize = getChildNodeText(historyChild,'bidsize')
                    asksize = getChildNodeText(historyChild,'asksize')
                    
                    eodData['symbol']=symbol
                    eodData['tradedate']=tradeDate
                    eodData['open_price']=openPrice
                    eodData['high_price']=highPrice
                    eodData['low_price']=lowPrice
                    eodData['last_price']=closePrice
                    eodData['vwap']=vwap
                    eodData['change']=change
                    eodData['changepercent']=changepercent
                    eodData['sharevolume']=sharevolume
                    eodData['totalvalue']=totalvalue
                    eodData['totaltrades']=totaltrades
                    eodData['bid']=bid
                    eodData['ask']=ask
                    eodData['bidsize']=bidsize
                    eodData['asksize']=asksize
                    
                    historyDataLists[tradeDate]=eodData    

            #add EOD data into historyDataLists
            historyDataLists[symbol]=eodData    
                
                    
    return historyDataLists 


def qm_parseXMLCompanyBasicInfoFromFile(xmlDataFile): 
     # create element tree object 
    #root = ET.fromstring(country_data_as_string)  #parse from string 
    tree = ET.parse(xmlDataFile) #parse from file 
      # get root element 
    root = tree.getroot() 
    # create empty list for news items 
    companyDataLists = {}
 
    # iterate news items 
    for rootChild in root:
        if (rootChild.tag == "company"):
            #got into "company" node parsing a company info 
            for companyChild in rootChild: 
                if (companyChild.tag == "symbolinfo"):
                    companyData={}
                    #symbolinfo node of company
                    attr=companyChild.attrib
                    symbol=attr['symbolstring']
                    companyData['symbol']=symbol
                    
                    for symbolInfoChild in companyChild:
                        if (symbolInfoChild.tag == "key"):
                            #key of symbolInfo
                            for keyInfo in symbolInfoChild:
                                companyData[keyInfo.tag] = keyInfo.text
                        if (symbolInfoChild.tag == "equityinfo"):
                            #eqityinfo 
                            for equityInfoChild in symbolInfoChild:
                                companyData[equityInfoChild.tag] = equityInfoChild.text
               
                if (companyChild.tag == "profile"):
                    if (companyData is not None):
                        #symbolinfo node of company
                        for profileChild in companyChild: 
                            if (profileChild.tag == "shortdescription"):
                                companyData["shortdescription"]=profileChild.text
                                
                            if (profileChild.tag == "longdescription"):
                                companyData["longdescription"]=profileChild.text
                                
                            if (profileChild.tag == "info"):
                                #info 
                                for infoChild in profileChild: 
                                    if (infoChild.tag == "address"):
                                        for addressChild in infoChild:
                                            if (addressChild.tag == "city"):
                                                companyData["address_city"] = addressChild.text
                                            if (addressChild.tag == "state"):
                                                companyData["address_state"] = addressChild.text
                                            if (addressChild.tag == "country"):
                                                companyData["address_country"] = addressChild.text
                                            if (addressChild.tag == "postcode"):
                                                companyData["address_postcode"] = addressChild.text
                                    if (infoChild.tag == "website"):
                                        companyData["website"] = infoChild.text
                                    if (infoChild.tag == "email"):
                                        companyData["email"] = infoChild.text
                            if (profileChild.tag == "details"):
                                #detail 
                                for detailChild in profileChild:
                                    if (detailChild.tag == "ceo"):
                                        companyData["ceo"]=detailChild.text
                                    if (detailChild.tag == "employees"):
                                        companyData["employees"]=detailChild.text
                                    if (detailChild.tag == "auditor"):
                                        companyData["auditor"]=detailChild.text
                                    if (detailChild.tag == "marketcap"):
                                        companyData["marketcap"]=detailChild.text
    
                            if (profileChild.tag == "classification"):
                                #classification
                                for classificationChild in profileChild:
                                    if (classificationChild.tag == "sics"):
                                        sic = getChildNodeText(classificationChild,"sic")
                                        print(("sic:%s")%(sic))
                                        companyData[classificationChild.tag]= sic
                                    else:
                                        companyData[classificationChild.tag]=classificationChild.text
                        

                                            
                if (companyChild.tag == "shareinfo"):
                    if (companyData is not None):
                        #share info 
                        for shareInfoChild in companyChild:
                            if (shareInfoChild.tag == "annualinfo"):
                                #annual info 
                                for annualinfoChild in shareInfoChild:
                                    infoname = ("annualinfo_%s")%(annualinfoChild.tag)
                                    companyData[infoname]=annualinfoChild.text
                            if (shareInfoChild.tag == "priceinfo"):
                                #annual info 
                                for priceinfoChild in shareInfoChild:
                                    infoname = ("priceinfo_%s")%(priceinfoChild.tag)
                                    companyData[infoname]=priceinfoChild.text
                            if (shareInfoChild.tag == "pricechange"):
                                #annual info 
                                for pricechangeChild in shareInfoChild:
                                    infoname = ("chg_%s")%(pricechangeChild.tag)
                                    companyData[infoname]=pricechangeChild.text
                            if (shareInfoChild.tag == "shareinformation"):
                                #annual info 
                                for informationChild in shareInfoChild:
                                    infoname = ("volume_%s")%(informationChild.tag)
                                    companyData[infoname]=informationChild.text
                            if (shareInfoChild.tag == "holdings"):
                                #annual info 
                                for holdingChild in shareInfoChild:
                                    infoname = ("holding_%s")%(holdingChild.tag)
                                    companyData[infoname]=holdingChild.text
                    
                if (companyChild.tag == "keyratios"):
                    if (companyData is not None):
                        #keyratio node of company
                        for ratioChild in companyChild: 
                            if (ratioChild.tag == "incomestatements"):
                                #incomestatement info 
                                for incomeChild in ratioChild:
                                    infoname = ("income_%s")%(incomeChild.tag)
                                    companyData[infoname]=incomeChild.text
                                
                            if (ratioChild.tag == "financialstrength"):
                                #incomestatement info 
                                for strengthChild in ratioChild:
                                    infoname = ("financial_%s")%(strengthChild.tag)
                                    companyData[infoname]=strengthChild.text
                    
                            if (ratioChild.tag == "managementeffectiveness"):
                                #incomestatement info 
                                for managementChild in ratioChild:
                                    infoname = ("management_%s")%(managementChild.tag)
                                    companyData[infoname]=managementChild.text
    
                            if (ratioChild.tag == "valuationmeasures"):
                                #incomestatement info 
                                for valuationChild in ratioChild:
                                    infoname = ("valuation_%s")%(valuationChild.tag)
                                    companyData[infoname]=valuationChild.text
                            if (ratioChild.tag == "profitability"):
                                #incomestatement info 
                                for profitChild in ratioChild:
                                    infoname = ("profitability_%s")%(profitChild.tag)
                                    companyData[infoname]=profitChild.text
                            if (ratioChild.tag == "assets"):
                                #incomestatement info 
                                for assetChild in ratioChild:
                                    infoname = ("assets_%s")%(assetChild.tag)
                                    companyData[infoname]=assetChild.text
            
            #end if the company node
            if (len(companyData.keys())>0):
                if (('symbol' in companyData.keys()) and ('shortdescription' in companyData.keys())):
                    print(("Company identified! symbol:%s company name:%s") %(companyData['symbol'],companyData["shortdescription"]))
                    companyDataLists[companyData['symbol']]=companyData

    return companyDataLists 



def qm_parseXMLGICSInfoFromFile(xmlDataFile): 
     # create element tree object 
    tree = ET.parse(xmlDataFile) #parse from file 
    root = tree.getroot() 
    # create empty list for news items 
    gicsData = {}
 
    # iterate news items 
    for rootChild in root:
        if (rootChild.tag == "sector"):
            attr = rootChild.attrib
            
            sector_code = attr['code']
            sector_name = attr['name']
            gicsData[sector_code] = sector_name
            
            #loop through industry groups 
            for igChild in rootChild: 
                if (igChild.tag == "group"):
                    igAttr = igChild.attrib
                    ig_code = igAttr['code']
                    ig_name = igAttr['name']
                    gicsData[ig_code]= ig_name
                    
                    #loop through industry 
                    for iChild in igChild: 
                        if (iChild.tag == "industry" ):
                            iAttr = iChild.attrib
                            i_code = iAttr['code']
                            i_name = iAttr['name']
                            gicsData[i_code] = i_name

    return gicsData 


#save company  data into database
def qm_saveGICSInfoIntoDatabase(gicsData,dbi):
    if (gicsData is None or dbi is None):
        return
    
    for gics_code, gics_name in gicsData.items(): 
        sql = (" exec cycreal.dbo.sp_QuoteMedia_GICS_addGICS %s,'%s'") % ( gics_code, gics_name)
        if (dbi.executeSQL(sql)):
            print("\b.")
        else:
            print(("Sql failed! sql:%s")%(sql))

    return

def qm_parseXMLGICSSectorPeerInfoFromFile(xmlDataFile):
     # create element tree object 
    tree = ET.parse(xmlDataFile) #parse from file 
    root = tree.getroot() 
    # create empty list for news items 
    gicsSectorPeers = {}
 
    # iterate news items 
    for rootChild in root:
        if (rootChild.tag == "peer"):
            
            attr = rootChild.attrib
            peer_symbol = attr['symbolstring']
            
            #loop through peer symbols 
            for peerChild in rootChild: 
                if (peerChild.tag == "symbolinfo"):
                    
                    peerChildAttr = peerChild.attrib
                    symbol = peerChildAttr['symbolstring']
                    peerSymbolInfo={}
            
                    for symbolInfoChild in peerChild:
                        if (symbolInfoChild.tag == "key"):
                            exchangeNode = symbolInfoChild.findall('exchange')
                            if not exchangeNode is None: 
                                exchange = exchangeNode[0].text
                                peerSymbolInfo['exchange']=exchange
                                
                            exLgNameNode = symbolInfoChild.findall('exLgName')
                            if (not exLgNameNode is None):
                                exLgName = exLgNameNode[0].text
                                peerSymbolInfo['exLgName']=exLgName
                                
                            exShNameNode = symbolInfoChild.findall('exShName')
                            if (not exShNameNode is None):
                                exShName = exShNameNode[0].text
                                peerSymbolInfo['exShName']=exShName
                    
                        if (symbolInfoChild.tag == "equityinfo"):
                            longNameNode = symbolInfoChild.findall('longname')
                            if not (longNameNode is None):
                                longName = longNameNode[0].text
                                peerSymbolInfo['longname']=longName
                                
                if (peerChild.tag == "secgroupind"):
                    sectorChild = peerChild.findall('sector')
                    if not (sectorChild is None):
                        sector_code = sectorChild[0].attrib['code']
                        peerSymbolInfo['sector_code']=sector_code
                    groupChild = peerChild.findall('group')
                    if not (groupChild is None):
                        group_code = groupChild[0].attrib['code']
                        peerSymbolInfo['group_code']=group_code
                    industryChild = peerChild.findall('industry')
                    if not (industryChild is None):
                        industry_code = industryChild[0].attrib['code']
                        peerSymbolInfo['industry_code']=industry_code
                        
                if (peerChild.tag == "fundamental"):
                    marketcapChild = peerChild.findall('marketcap')
                    if not (marketcapChild is None):
                        if (len(marketcapChild)>0):
                            marketcap = marketcapChild[0].text
                            peerSymbolInfo['marketcap']=marketcap
                        else:
                            peerSymbolInfo['marketcap']='None'
                    
                    avg30dayvolumeChild = peerChild.findall('avg30dayvolume')
                    if not (avg30dayvolumeChild is None):
                        if (len(avg30dayvolumeChild)>0):
                            avg30dayvolume = avg30dayvolumeChild[0].text
                            peerSymbolInfo['avg30dayvolume']=avg30dayvolume
                        else:
                            peerSymbolInfo['avg30dayvolume']='None'


                    week52highChild = peerChild.findall('week52high')
                    if not (week52highChild is None):
                        if (len(week52highChild)>0):
                            week52high = week52highChild[0].text
                            week52highdate = week52highChild[0].attrib['date']
                            
                            peerSymbolInfo['week52high']=week52high
                            peerSymbolInfo['week52highdate']=week52highdate
                        else:
                            peerSymbolInfo['week52high']='None'
                            peerSymbolInfo['week52highdate']='None'

                    week52lowChild = peerChild.findall('week52low')
                    if not (week52lowChild is None):
                        if (len(week52lowChild)>0):
                            week52low = week52lowChild[0].text
                            week52lowdate = week52lowChild[0].attrib['date']
                            
                            peerSymbolInfo['week52low']=week52low
                            peerSymbolInfo['week52lowdate']=week52lowdate
                        else:
                            peerSymbolInfo['week52low']='None'
                            peerSymbolInfo['week52lowdate']='None'
            #
            if (symbol is not None) and (peerSymbolInfo is not None):
                gicsSectorPeers[symbol]=peerSymbolInfo
                
    return gicsSectorPeers 


def qm_saveGICSSectorPeerInfoIntoDatabase(peer_symbol,gicsSectorPeers,dbi):
    if (gicsSectorPeers is None or dbi is None):
        return
    
    for symbol in gicsSectorPeers.keys():
        peerSymbolInfo = gicsSectorPeers[symbol]
        exchange = peerSymbolInfo['exchange']
        exLgName = peerSymbolInfo['exLgName']
        exShName = peerSymbolInfo['exShName']
        longName = peerSymbolInfo['longname']
        sector_code = peerSymbolInfo['sector_code']
        group_code = peerSymbolInfo['group_code']
        industry_code = peerSymbolInfo['industry_code']
        marketcap = peerSymbolInfo['marketcap']
        avg30dayvolume = peerSymbolInfo['avg30dayvolume']
        week52high= peerSymbolInfo['week52high']
        week52highdate = peerSymbolInfo['week52highdate']
        week52low= peerSymbolInfo['week52low']
        week52lowdate = peerSymbolInfo['week52lowdate']
        
#EXECUTE @RC = [cycreal].[dbo].[sp_QuoteMedia_GICSIndustryPeers_addPeer] 
#   @peerSymbol
#  ,@symbol
#  ,@exchange
#  ,@exLgName
#  ,@exShName
#  ,@longName
#  ,@shortName
        
#  ,@sector_code
#  ,@industrygroup_code
#  ,@industry_code
#  ,@lastPrice

#  ,@marketcap
#  ,@week52high
#  ,@week52high_date
#  ,@week52low
#  ,@week52low_date
#  ,@avg30dayvolume
        
        sql = "exec cycreal.dbo.sp_QuoteMedia_GICSIndustryPeers_addPeer "
        sql = ("%s '%s'")%(sql , peer_symbol)
        sql = ("%s ,'%s'")%(sql , symbol)
        sql = ("%s ,'%s'")%(sql , validateDataItem(exchange))
        sql = ("%s ,'%s'")%(sql , validateDataItem(exLgName))
        sql = ("%s ,'%s'")%(sql , validateDataItem(exShName))
        
        sql = ("%s ,'%s'")%(sql , validateDataItem(longName))
        sql = ("%s ,'%s'")%(sql , symbol)
        sql = ("%s ,%s")%(sql , sector_code)
        sql = ("%s ,%s")%(sql , group_code)
        sql = ("%s ,%s")%(sql , industry_code)
        sql = ("%s ,%s")%(sql , 0)  #lastprice
        
        sql = ("%s ,%s")%(sql , marketcap)
        sql = ("%s ,%s")%(sql , week52high)
        sql = ("%s ,'%s'")%(sql , week52highdate)
        sql = ("%s ,%s")%(sql , week52low)
        sql = ("%s ,'%s'")%(sql , week52lowdate)
        sql = ("%s ,%s")%(sql , avg30dayvolume)

        sql = sql.replace("'None'","NULL")
        sql = sql.replace("None","NULL")
        if (dbi.executeSQL(sql)):
            print("\b.")
        else:
            print(("Sql failed! sql:%s")%(sql))

    return

#sp_QuoteMedia_GICS_addGICS

def qm_parseXMLBrokerRatingFromFile(xmlDataFile):
     # create element tree object 
    tree = ET.parse(xmlDataFile) #parse from file 
    root = tree.getroot() 
    # create empty list for news items 
    brokerRatings = {}
 
    # iterate news items 
    for rootChild in root:
        if (rootChild.tag == "brokerratings"):
            
            attr = rootChild.attrib
            symbol = attr['symbolstring']
            
            #loop through peer symbols 
            for ratingChild in rootChild: 
                broker = None
                brokerRatingInfo=None
                
                if (ratingChild.tag == "brokerrating"):
                    ratingType = ratingChild.attrib['type']
                    ratingDate = ratingChild.attrib['date']   

                    broker = ""
                    rating = ""
                    prev_rating = ""
                    lowtarget = ""
                    target = ""
                    
                    broker = getChildNodeText(ratingChild,'broker')
                    
                    if (broker != ""):
                        rating = getChildNodeText(ratingChild,'rating')
                        prev_rating = getChildNodeText(ratingChild,'prevrating')
                        target = getChildNodeText(ratingChild,'target')
                        lowtarget = getChildNodeText(ratingChild,'lowtarget')

                      
                    if ((broker != "") and (rating != "")):
                        brokerRatingInfo={}
                        brokerRatingInfo['symbol']=symbol
                        brokerRatingInfo['type']=ratingType
                        brokerRatingInfo['date']=ratingDate
                        brokerRatingInfo['broker']=broker
                        brokerRatingInfo['rating']=rating
                        brokerRatingInfo['prev_rating']=prev_rating
                        brokerRatingInfo['target']=target
                        brokerRatingInfo['lowtarget']=lowtarget
                        
                #finished parsing the current rating         
                if (broker is not None) and (brokerRatingInfo is not None):
                    brokerRatings[broker]=brokerRatingInfo
                
    return brokerRatings 

def qm_saveBrokerRatingsIntoDatabase(brokerRatings,dbi):
    if (brokerRatings is None or dbi is None):
        return
    
    for broker in brokerRatings.keys():
        brokerRatingInfo = brokerRatings[broker]
        symbol= brokerRatingInfo['symbol']
        ratingDate = brokerRatingInfo['date']
        ratingType= brokerRatingInfo['type']
        rating = brokerRatingInfo['rating']
        prev_rating = brokerRatingInfo['prev_rating']
        target = brokerRatingInfo['target']
        if (target == ""):
            target = "0"
        lowtarget = brokerRatingInfo['lowtarget']
        if (lowtarget == ""):
            lowtarget = "0"
        
        sql = "exec cycreal.dbo.sp_QuoteMedia_BrokerRatings_addRating "
        sql = ("%s '%s'")%(sql , broker)
        sql = ("%s ,'%s'")%(sql , symbol)
        sql = ("%s ,'%s'")%(sql , ratingDate)
        sql = ("%s ,'%s'")%(sql , ratingType)
        sql = ("%s ,'%s'")%(sql , rating)
        sql = ("%s ,'%s'")%(sql , prev_rating)
        sql = ("%s ,%s")%(sql , target)
        sql = ("%s ,%s")%(sql , lowtarget)

        sql = sql.replace("'None'","NULL")
        sql = sql.replace("None","NULL")
        if (dbi.executeSQL(sql)):
            print("\b.")
        else:
            print(("Sql failed! sql:%s")%(sql))

    return


def qm_parseXMLEarningsEstimatesFromFile(xmlDataFile):
     # create element tree object 
    tree = ET.parse(xmlDataFile) #parse from file 
    root = tree.getroot() 
    # create empty list for news items 
    earningsEstimates = {}
    brokerEstimates=[]
    
 
    # iterate news items 
    for rootChild in root:
        if (str(rootChild.tag).lower() == "earningsestimates"):
            
            attr = rootChild.attrib
            symbol = attr['symbolstring']
            earningsEstimates['symbol']=symbol
            
#<earningsestimate date="2020-02-04">
#<broker>
#<![CDATA[
#G.Research, Llc
#]]>
#</broker>
#<financialyear>2023.0</financialyear>
#<prevestimate>1.08</prevestimate>
#<revestimate>1.06</revestimate>
#<percentchange>-0.019</percentchange>
            
            #loop estimateChild
            for estimateChild in rootChild: 
                if (str(estimateChild.tag).lower() == "earningsestimate"):
                    #
                    if ("date" in estimateChild.attrib):
                        estimateDate = estimateChild.attrib['date']
                        broker = getChildNodeText(estimateChild,'broker')   
                        financialyear= getChildNodeText(estimateChild,'financialyear')   
                        revestimate = getChildNodeText(estimateChild,'revestimate')   
                        prevestimate = getChildNodeText(estimateChild,'prevestimate') 
                        percentchange = getChildNodeText(estimateChild,'percentchange') 
                        brokerEstimate={}
                        brokerEstimate['broker_estimate_broker']=broker
                        brokerEstimate['broker_estimate_date']=estimateDate
                        brokerEstimate['broker_estimate_financialyear']=financialyear
                        brokerEstimate['broker_estimate_revestimate']=revestimate
                        brokerEstimate['broker_estimate_prevestimate']=prevestimate
                        brokerEstimate['broker_estimate_percentchange']=percentchange
                        
                        brokerEstimates.append(brokerEstimate)
                        
                        
                         
                            
                if (str(estimateChild.tag).lower() == "targetpricehistory"):
                    for targetChild in estimateChild:
                        if (str(targetChild.tag).lower() == "hightargetpriceestimate"):
                            # high target estimates
                            hightarget_current = getChildNodeText(targetChild,'currentTarget')                            
                            hightarget_weeks1ago = getChildNodeText(targetChild,'weeks1Ago')
                            hightarget_weeks2ago = getChildNodeText(targetChild,'weeks2Ago')
                            hightarget_weeks3ago = getChildNodeText(targetChild,'weeks3Ago')
                            
                            earningsEstimates['target_current']=hightarget_current
                            earningsEstimates['target_weeks1ago']=hightarget_weeks1ago
                            earningsEstimates['target_weeks2ago']=hightarget_weeks2ago
                            earningsEstimates['target_weeks3ago']=hightarget_weeks3ago
                            

                        if (str(targetChild.tag).lower() == "lowtargetpriceestimate"):
                            # high target estimates
                            lowtarget_current = getChildNodeText(targetChild,'currentTarget')
                            lowtarget_weeks1ago = getChildNodeText(targetChild,'weeks1Ago')
                            lowtarget_weeks2ago = getChildNodeText(targetChild,'weeks2Ago')
                            lowtarget_weeks3ago = getChildNodeText(targetChild,'weeks3Ago')

                            earningsEstimates['lowtarget_current']=lowtarget_current
                            earningsEstimates['lowtarget_weeks1ago']=lowtarget_weeks1ago
                            earningsEstimates['lowtarget_weeks2ago']=lowtarget_weeks2ago
                            earningsEstimates['lowtarget_weeks3ago']=lowtarget_weeks3ago

                        if (str(targetChild.tag).lower() == "meantargetpriceestimate"):
                            # high target estimates
                            meantarget_current = getChildNodeText(targetChild,'currentTarget')
                            meantarget_weeks1ago = getChildNodeText(targetChild,'weeks1Ago')
                            meantarget_weeks2ago = getChildNodeText(targetChild,'weeks2Ago')
                            meantarget_weeks3ago = getChildNodeText(targetChild,'weeks3Ago')

                            earningsEstimates['meantarget_current']=meantarget_current
                            earningsEstimates['meantarget_weeks1ago']=meantarget_weeks1ago
                            earningsEstimates['meantarget_weeks2ago']=meantarget_weeks2ago
                            earningsEstimates['meantarget_weeks3ago']=meantarget_weeks3ago

                        if (str(targetChild.tag).lower() == "standarddeviation"):
                            # high target estimates
                            stdtarget_current = getChildNodeText(targetChild,'currentTarget')
                            stdtarget_weeks1ago = getChildNodeText(targetChild,'weeks1Ago')
                            stdtarget_weeks2ago = getChildNodeText(targetChild,'weeks2Ago')
                            stdtarget_weeks3ago = getChildNodeText(targetChild,'weeks3Ago')

                            earningsEstimates['stdtarget_current']=stdtarget_current
                            earningsEstimates['stdtarget_weeks1ago']=stdtarget_weeks1ago
                            earningsEstimates['stdtarget_weeks2ago']=stdtarget_weeks2ago
                            earningsEstimates['stdtarget_weeks3ago']=stdtarget_weeks3ago


                        if (str(targetChild.tag).lower() == "datemostrecentestimate"):
                            # high target estimates
                            date_current = getChildNodeText(targetChild,'currentTarget')
                            date_weeks1ago = getChildNodeText(targetChild,'weeks1Ago')
                            date_weeks2ago = getChildNodeText(targetChild,'weeks2Ago')
                            date_weeks3ago = getChildNodeText(targetChild,'weeks3Ago')

                            earningsEstimates['date_current']=date_current
                            earningsEstimates['date_weeks1ago']=date_weeks1ago
                            earningsEstimates['date_weeks2ago']=date_weeks2ago
                            earningsEstimates['date_weeks3ago']=date_weeks3ago

                            
                if (str(estimateChild.tag).lower() == "consensusepsestimatestrends"):
                    for targetChild in estimateChild:
                        if (str(targetChild.tag).lower() == "currentest"):
                            # high target estimates
                            (est_quarter1, est_quarter1_monthyear) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_quarter2, est_quarter2_monthyear) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_year1, est_year1_monthyear) = getChildNodeTextAndAttr(targetChild,'fr1','monthYear')
                            (est_year2, est_year2_monthyear) = getChildNodeTextAndAttr(targetChild,'fr2','monthYear')

                            earningsEstimates['est_quarter1']=est_quarter1
                            earningsEstimates['est_quarter1_monthyear']=est_quarter1_monthyear
                            earningsEstimates['est_quarter2']=est_quarter2
                            earningsEstimates['est_quarter2_monthyear']=est_quarter2_monthyear
                            earningsEstimates['est_year1']=est_year1
                            earningsEstimates['est_year1_monthyear']=est_year1_monthyear
                            earningsEstimates['est_year2']=est_year1
                            earningsEstimates['est_year2_monthyear']=est_year2_monthyear

                        if (str(targetChild.tag).lower() == "days7ago"):
                            # high target estimates
                            (est_quarter1_days7Ago, est_quarter1_monthyear_days7Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_quarter2_days7Ago, est_quarter2_monthyear_days7Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_year1_days7Ago, est_year1_monthyear_days7Ago) = getChildNodeTextAndAttr(targetChild,'fr1','monthYear')
                            (est_year2_days7Ago, est_year2_monthyear_days7Ago) = getChildNodeTextAndAttr(targetChild,'fr2','monthYear')


                            earningsEstimates['est_quarter1_days7Ago']=est_quarter1_days7Ago
                            earningsEstimates['est_quarter1_monthyear_days7Ago']=est_quarter1_monthyear_days7Ago
                            earningsEstimates['est_quarter2_days7Ago']=est_quarter2_days7Ago
                            earningsEstimates['est_quarter2_monthyear_days7Ago']=est_quarter2_monthyear_days7Ago
                            earningsEstimates['est_year1_days7Ago']=est_year1_days7Ago
                            earningsEstimates['est_year1_monthyear_days7Ago']=est_year1_monthyear_days7Ago
                            earningsEstimates['est_year2_days7Ago']=est_year2_days7Ago
                            earningsEstimates['est_year2_monthyear_days7Ago']=est_year2_monthyear_days7Ago


                        if (str(targetChild.tag).lower() == "days30ago"):
                            # high target estimates
                            (est_quarter1_days30Ago, est_quarter1_monthyear_days30Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_quarter2_days30Ago, est_quarter2_monthyear_days30Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_year1_days30Ago, est_year1_monthyear_days30Ago) = getChildNodeTextAndAttr(targetChild,'fr1','monthYear')
                            (est_year2_days30Ago, est_year2_monthyear_days30Ago) = getChildNodeTextAndAttr(targetChild,'fr2','monthYear')


                            earningsEstimates['est_quarter1_days30Ago']=est_quarter1_days30Ago
                            earningsEstimates['est_quarter1_monthyear_days30Ago']=est_quarter1_monthyear_days30Ago
                            earningsEstimates['est_quarter2_days30Ago']=est_quarter2_days30Ago
                            earningsEstimates['est_quarter2_monthyear_days30Ago']=est_quarter2_monthyear_days30Ago
                            earningsEstimates['est_year1_days30Ago']=est_year1_days30Ago
                            earningsEstimates['est_year1_monthyear_days30Ago']=est_year1_monthyear_days30Ago
                            earningsEstimates['est_year2_days30Ago']=est_year2_days30Ago
                            earningsEstimates['est_year2_monthyear_days30Ago']=est_year2_monthyear_days30Ago


                        if (str(targetChild.tag).lower() == "days60ago"):
                            # high target estimates
                            (est_quarter1_days60Ago, est_quarter1_monthyear_days60Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_quarter2_days60Ago, est_quarter2_monthyear_days60Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_year1_days60Ago, est_year1_monthyear_days60Ago) = getChildNodeTextAndAttr(targetChild,'fr1','monthYear')
                            (est_year2_days60Ago, est_year2_monthyear_days60Ago) = getChildNodeTextAndAttr(targetChild,'fr2','monthYear')


                            earningsEstimates['est_quarter1_days60Ago']=est_quarter1_days60Ago
                            earningsEstimates['est_quarter1_monthyear_days60Ago']=est_quarter1_monthyear_days60Ago
                            earningsEstimates['est_quarter2_days60Ago']=est_quarter2_days60Ago
                            earningsEstimates['est_quarter2_monthyear_days60Ago']=est_quarter2_monthyear_days60Ago
                            earningsEstimates['est_year1_days60Ago']=est_year1_days60Ago
                            earningsEstimates['est_year1_monthyear_days60Ago']=est_year1_monthyear_days60Ago
                            earningsEstimates['est_year2_days60Ago']=est_year2_days60Ago
                            earningsEstimates['est_year2_monthyear_days60Ago']=est_year2_monthyear_days60Ago


                        if (str(targetChild.tag).lower() == "days90ago"):
                            # high target estimates
                            (est_quarter1_days90Ago, est_quarter1_monthyear_days90Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_quarter2_days90Ago, est_quarter2_monthyear_days90Ago) = getChildNodeTextAndAttr(targetChild,'qr1','monthYear')
                            (est_year1_days90Ago, est_year1_monthyear_days90Ago) = getChildNodeTextAndAttr(targetChild,'fr1','monthYear')
                            (est_year2_days90Ago, est_year2_monthyear_days90Ago) = getChildNodeTextAndAttr(targetChild,'fr2','monthYear')


                            earningsEstimates['est_quarter1_days90Ago']=est_quarter1_days90Ago
                            earningsEstimates['est_quarter1_monthyear_days90Ago']=est_quarter1_monthyear_days90Ago
                            earningsEstimates['est_quarter2_days90Ago']=est_quarter2_days90Ago
                            earningsEstimates['est_quarter2_monthyear_days90Ago']=est_quarter2_monthyear_days90Ago
                            earningsEstimates['est_year1_days90Ago']=est_year1_days90Ago
                            earningsEstimates['est_year1_monthyear_days90Ago']=est_year1_monthyear_days90Ago
                            earningsEstimates['est_year2_days90Ago']=est_year2_days90Ago
                            earningsEstimates['est_year2_monthyear_days90Ago']=est_year2_monthyear_days90Ago


                if (str(estimateChild.tag).lower() == "earningssurprise"):
                    for targetChild in estimateChild:
                        if (str(targetChild.tag).lower() == "epsestimate"):
                            (eps_est_current, eps_est_current_monthyear) = getChildNodeTextAndAttr(targetChild,'current','monthYear')
                            earningsEstimates['eps_est_current']=eps_est_current
                            earningsEstimates['eps_est_current_monthyear']=eps_est_current_monthyear
                        if (str(targetChild.tag).lower() == "epsactual"):
                            (eps_actual_current, eps_actual_monthyear) = getChildNodeTextAndAttr(targetChild,'current','monthYear')
                            earningsEstimates['eps_actual_current']=eps_actual_current
                            earningsEstimates['eps_actual_monthyear']=eps_actual_monthyear
                            
                        if (str(targetChild.tag).lower() == "epsdifference"):
                            (eps_diff_current, eps_diff_monthyear) = getChildNodeTextAndAttr(targetChild,'current','monthYear')
                            earningsEstimates['eps_diff_current']=eps_diff_current
                            earningsEstimates['eps_diff_monthyear']=eps_diff_monthyear


                        if (str(targetChild.tag).lower() == "percentepssurprise"):
                            (eps_pctsurprice_current, eps_pctsurprice_monthyear) = getChildNodeTextAndAttr(targetChild,'current','monthYear')
                            earningsEstimates['eps_pctsurprice_current']=eps_pctsurprice_current
                            earningsEstimates['eps_pctsurprice_monthyear']=eps_pctsurprice_monthyear


                if (str(estimateChild.tag).lower() == "consensusepsgrowthrate"):
                    growth_next5year = getChildNodeText(estimateChild,'next5year')
                    earningsEstimates['growth_next5year']=growth_next5year
                    
                if (str(estimateChild.tag).lower() == "numberofestimates"):
                    numInConsensus = getChildNodeText(estimateChild,'NumEstimatesInConsensus')
                    NumEstimatesRaised = getChildNodeText(estimateChild,'NumEstimatesRaised')
                    earningsEstimates['numInConsensus']=numInConsensus
                    earningsEstimates['NumEstimatesRaised']=NumEstimatesRaised

                if (str(estimateChild.tag).lower() == "datemostrecentestimate"):
                    date_current = getChildNodeText(estimateChild,'currentTarget')
                    date_weeks1Ago = getChildNodeText(estimateChild,'weeks1Ago')
                    date_weeks2Ago = getChildNodeText(estimateChild,'weeks2Ago')
                    date_weeks3Ago = getChildNodeText(estimateChild,'weeks3Ago')

                    if (date_current != ""):
                        earningsEstimates['estimate_date_current']=date_current
                        earningsEstimates['estimate_date_weeks1Ago']=date_weeks1Ago
                        earningsEstimates['estimate_date_weeks2Ago']=date_weeks2Ago
                        earningsEstimates['estimate_date_weeks3Ago']=date_weeks3Ago

                if (str(estimateChild.tag).lower() == "numberofestimateshistory"):
                    numOfEstimate_current = getChildNodeText(estimateChild,'currentTarget')
                    numOfEstimate_weeks1Ago = getChildNodeText(estimateChild,'weeks1Ago')
                    numOfEstimate_weeks2Ago = getChildNodeText(estimateChild,'weeks2Ago')
                    numOfEstimate_weeks3Ago = getChildNodeText(estimateChild,'weeks3Ago')

                    earningsEstimates['numOfEstimate_current']=numOfEstimate_current
                    earningsEstimates['numOfEstimate_weeks1Ago']=numOfEstimate_weeks1Ago
                    earningsEstimates['numOfEstimate_weeks2Ago']=numOfEstimate_weeks2Ago
                    earningsEstimates['numOfEstimate_weeks3Ago']=numOfEstimate_weeks3Ago
    
    earningsEstimates['broker_estimates']=brokerEstimates
    return earningsEstimates     

def qm_saveEstimatesIntoDatabase_raw(symbol,updatedate, qr1_monthyear, qr1_est, qr2_monthyear, qr2_est 
                                            ,fr1_monthyear, fr1_est, fr2_monthyear, fr2_est
                                            , epsgrowth_5year
                                            ,dbi):
        #dbo.sp_QuoteMedia_EPSEstimates_addEstimate
        #	@symbol varchar(20) 
        #	,@updatedate datetime 
        #	,@qr1_monthyear varchar(20)
        #	,@qr1_est float
        #	,@qr2_monthyear varchar(20)
        #	,@qr2_est float
        #	,@fr1_monthyear varchar(20)
        #	,@fr1_est float
        #	,@fr2_monthyear varchar(20)
        #	,@fr2_est float
        #	,@epsgrowth_5year float
        sql = "exec cycreal.dbo.sp_QuoteMedia_EPSEstimates_addEstimate "
        sql = ("%s '%s'")%(sql , symbol)
        sql = ("%s ,'%s'")%(sql , updatedate)
        sql = ("%s ,'%s'")%(sql , qr1_monthyear)
        sql = ("%s ,%s")%(sql , qr1_est)
        sql = ("%s ,'%s'")%(sql , qr2_monthyear)
        sql = ("%s ,%s")%(sql , qr2_est)
        sql = ("%s ,'%s'")%(sql , fr1_monthyear)
        sql = ("%s ,%s")%(sql , fr1_est)
        sql = ("%s ,'%s'")%(sql , fr2_monthyear)
        sql = ("%s ,%s")%(sql , fr2_est)
        sql = ("%s ,%s")%(sql , epsgrowth_5year)

        sql = sql.replace("'None'","NULL")
        sql = sql.replace("None","NULL")
        if (dbi.executeSQL(sql)):
            print("\b.")
        else:
            print(("Sql failed! sql:%s")%(sql))
        return
    

def qm_saveBrokerEstimates_raw(earningsEstimates
                     ,dbi):
#                        earningsEstimates['broker_estimate_broker']=broker
#                        earningsEstimates['broker_estimate_date']=estimateDate
#                        earningsEstimates['broker_estimate_financialyear']=financialyear
#                        earningsEstimates['broker_estimate_revestimate']=revestimate
#                        earningsEstimates['broker_estimate_prevestimate']=prevestimate
#                        earningsEstimates['broker_estimate_percentchange']=percentchange
#exec dbo.sp_QuoteMedia_BrokerEstimates_addBrokerEstimate
#	@symbol varchar(20)
#	,@broker varchar(200)
#	,@financialyear int
#	,@estimate_date datetime
#	,@estimate_revestimate float
#	,@estimate_prevestimate float
#	,@estimate_pctchange float
    
#    earningsEstimates['broker_estimates']=brokerEstimates
    if ('broker_estimates' in earningsEstimates.keys()):
        symbol = earningsEstimates['symbol']
        brokerEstimates=earningsEstimates['broker_estimates'] 
        
        if ( brokerEstimates is not None):
            for brokerEstimate in brokerEstimates:
                if (brokerEstimate is not None and 'broker_estimate_broker' in brokerEstimate.keys()):
                    broker = brokerEstimate['broker_estimate_broker']
                    estimate_date = brokerEstimate['broker_estimate_date'] 
                    financialyear = brokerEstimate['broker_estimate_financialyear']
                    revestimate = brokerEstimate['broker_estimate_revestimate']
                    prevestimate = brokerEstimate['broker_estimate_prevestimate']
                    pctchange = brokerEstimate['broker_estimate_percentchange']
                    
                    sql = "exec cycreal.dbo.sp_QuoteMedia_BrokerEstimates_addBrokerEstimate "
                    sql = ("%s '%s'")%(sql , symbol)
                    sql = ("%s ,'%s'")%(sql , broker)
                    sql = ("%s ,%s")%(sql , financialyear)
                    sql = ("%s ,'%s'")%(sql , estimate_date)
                    sql = ("%s ,%s")%(sql , revestimate)
                    sql = ("%s ,%s")%(sql , prevestimate)
                    sql = ("%s ,%s")%(sql , pctchange)
                    
                    sql = sql.replace("'None'","NULL")
                    sql = sql.replace("None","NULL")
                    if (dbi.executeSQL(sql)):
                        print("\b.")
                    else:
                        print(("Sql failed! sql:%s")%(sql))
    return
    
def qm_saveEarningSurprises_raw(symbol
                                ,earning_monthyear
                                ,epsEstimate
                                ,epsActual
                                ,epsDifference
                                ,epsPctDifference
                                ,dbi
                                ):
#create procedure dbo.sp_QuoteMedia_EarningSurprises_addSurpriseReport
#	@symbol varchar(20) 
#	, @earning_monthyear varchar(20)
#	, @epsEstimate float
#	, @epsActual float
#	, @epsDifference float
#	, @epsPctDifference float
    sql = "exec cycreal.dbo.sp_QuoteMedia_EarningSurprises_addSurpriseReport "
    sql = ("%s '%s'")%(sql , symbol)
    sql = ("%s ,'%s'")%(sql , earning_monthyear)
    sql = ("%s ,%s")%(sql , epsEstimate)
    sql = ("%s ,%s")%(sql , epsActual)
    sql = ("%s ,%s")%(sql , epsDifference)
    sql = ("%s ,%s")%(sql , epsPctDifference)
    
    sql = sql.replace("'None'","NULL")
    sql = sql.replace("None","NULL")
    if (dbi.executeSQL(sql)):
        print("\b.")
    else:
        print(("Sql failed! sql:%s")%(sql))
    return
    
def qm_saveTargetsIntoDatabase_raw(symbol,updatedate,target_high, target_low,target_mean,target_std
                                            ,NumerOfEstimates
                                            ,dbi
                                            ,bUpdateSymbol=False):

#create procedure dbo.sp_QuoteMedia_Targets_addTarget
#	@symbol varchar(20)
#	,@updatedate datetime
#	,@target float
#	,@target_low float
#	,@target_mean float
#	,@target_std float
#	,@numberOfEstimate int

        sql = "exec cycreal.dbo.sp_QuoteMedia_Targets_addTarget "
        sql = ("%s '%s'")%(sql , symbol)
        sql = ("%s ,'%s'")%(sql , updatedate)
        sql = ("%s ,%s")%(sql , target_high)
        sql = ("%s ,%s")%(sql , target_low)
        sql = ("%s ,%s")%(sql , target_mean)
        sql = ("%s ,%s")%(sql , target_std)
        sql = ("%s ,%s")%(sql , NumerOfEstimates)
        if (bUpdateSymbol):
            sql = ("%s ,1")%(sql)
        else:
            sql = ("%s ,0")%(sql)
            
        sql = sql.replace("'None'","NULL")
        sql = sql.replace("None","NULL")
        if (dbi.executeSQL(sql)):
            print("\b.")
        else:
            print(("Sql failed! sql:%s")%(sql))
            
        return
    
    
def qm_saveEarningsEstimateIntoDatabase(earningsEstimates,dbi):
    if (earningsEstimates is None or dbi is None):
        return
    
    if ((earningsEstimates is not None) and ("symbol" in earningsEstimates.keys())):
        symbol = earningsEstimates['symbol']
        
        qm_saveBrokerEstimates_raw(earningsEstimates,dbi)
        
        #current estimates: 
        currentDate = None        
        updatedate = getDictDataKeyValue(earningsEstimates,"date_current")
        #save targets 
        if (updatedate is not None):
            currentDate = datetime.datetime.strptime(updatedate,'%m/%d/%y')
            target_high = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"target_current"))
            target_low = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"lowtarget_current"))
            target_mean = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"meantarget_current"))
            target_std = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"stdtarget_current"))
            NumerOfEstimates= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"numOfEstimate_current"))
            
            
            epsgrowth_5year= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"growth_next5year"))
            qm_saveTargetsIntoDatabase_raw(symbol, updatedate, target_high,target_low, target_mean, target_std
                                                    ,NumerOfEstimates
                                                    ,dbi)
                                                
            #weeks1Ago estimates
            updatedate = getDictDataKeyValue(earningsEstimates,"date_weeks1ago")
            if (updatedate == "" or updatedate is None) and (currentDate is not None):
                #date_week1ago = date_weeks2ago - 7 days 
                d = datetime.timedelta(days=7)
                currentDate_7= currentDate - d
                updatedate = currentDate_7.strftime('%Y-%m-%d')
            if (updatedate is not None):
                target_high = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"target_weeks1ago"))
                target_low = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"lowtarget_weeks1ago"))
                target_mean = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"meantarget_weeks1ago"))
                target_std = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"stdtarget_weeks1ago"))
                NumerOfEstimates= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"numOfEstimate_weeks1Ago"))
                
                epsgrowth_5year= getDictDataKeyValue(earningsEstimates,"growth_next5year")
                qm_saveTargetsIntoDatabase_raw(symbol, updatedate, target_high,target_low, target_mean, target_std
                                                        ,NumerOfEstimates
                                                        ,dbi)
            #weeks2Ago estimates
            updatedate = getDictDataKeyValue(earningsEstimates,"date_weeks2ago")
            if (updatedate == "" or updatedate is None) and (currentDate is not None):
                d = datetime.timedelta(days=14)
                currentDate_14= currentDate - d
                updatedate = currentDate_14.strftime('%Y-%m-%d')
                
            if (updatedate is not None):
                target_high = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"target_weeks2ago"))
                target_low = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"lowtarget_weeks2ago"))
                target_mean = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"meantarget_weeks2ago"))
                target_std = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"stdtarget_weeks2ago"))
                NumerOfEstimates= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"numOfEstimate_weeks2Ago"))
                qm_saveTargetsIntoDatabase_raw(symbol, updatedate, target_high,target_low, target_mean, target_std
                                                        ,NumerOfEstimates
                                                        ,dbi)
            #weeks3Ago estimates
            updatedate = getDictDataKeyValue(earningsEstimates,"date_weeks3ago")
            if (updatedate == "" or updatedate is None) and (currentDate is not None):
                d = datetime.timedelta(days=21)
                currentDate_21= currentDate - d
                updatedate = currentDate_21.strftime('%Y-%m-%d')
                
            if (updatedate is not None):
                target_high = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"target_weeks3ago"))
                target_low = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"lowtarget_weeks3ago"))
                target_mean = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"meantarget_weeks3ago"))
                target_std = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"stdtarget_weeks3ago"))
                NumerOfEstimates= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"numOfEstimate_weeks3Ago"))
                
                qm_saveTargetsIntoDatabase_raw(symbol, updatedate, target_high,target_low, target_mean, target_std
                                                        ,NumerOfEstimates
                                                        ,dbi)

        #save estimates
        #qm_saveEstimatesIntoDatabase_raw
        if (updatedate is not None and currentDate is not None):
            #current estimates 
            qr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_monthyear"))
            qr1_est =   replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1"))
            qr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_monthyear"))
            qr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2"))
            
            fr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_monthyear"))
            fr1_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1"))
            fr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_monthyear"))
            fr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2"))
            epsgrowth_5year= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"growth_next5year"))
            qm_saveEstimatesIntoDatabase_raw(symbol,updatedate, qr1_monthyear, qr1_est, qr2_monthyear, qr2_est 
                                            ,fr1_monthyear, fr1_est, fr2_monthyear, fr2_est
                                            ,epsgrowth_5year
                                            ,dbi)
            
            #7-days ago estimates 
            d = datetime.timedelta(days=7)
            currentDate_7= currentDate - d
            updatedate = currentDate_7.strftime('%Y-%m-%d')
            qr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_monthyear_days7Ago"))
            qr1_est =   replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_days7Ago"))
            qr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_monthyear_days7Ago"))
            qr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_days7Ago"))
            fr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_monthyear_days7Ago"))
            fr1_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_days7Ago"))
            fr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_monthyear_days7Ago"))
            fr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_days7Ago"))
            epsgrowth_5year= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"growth_next5year_days7Ago"))
            qm_saveEstimatesIntoDatabase_raw(symbol,updatedate, qr1_monthyear, qr1_est, qr2_monthyear, qr2_est 
                                            ,fr1_monthyear, fr1_est, fr2_monthyear, fr2_est
                                            ,epsgrowth_5year
                                            ,dbi)
            
            
            #30-days ago estimates 
            d = datetime.timedelta(days=30)
            currentDate_30= currentDate - d
            updatedate = currentDate_30.strftime('%Y-%m-%d')
            qr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_monthyear_days30Ago"))
            qr1_est =   replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_days30Ago"))
            qr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_monthyear_days30Ago"))
            qr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_days30Ago"))
            fr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_monthyear_days30Ago"))
            fr1_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_days30Ago"))
            fr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_monthyear_days30Ago"))
            fr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_days30Ago"))
            epsgrowth_5year= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"growth_next5year_days30Ago"))
            qm_saveEstimatesIntoDatabase_raw(symbol,updatedate, qr1_monthyear, qr1_est, qr2_monthyear, qr2_est 
                                            ,fr1_monthyear, fr1_est, fr2_monthyear, fr2_est
                                            ,epsgrowth_5year
                                            ,dbi)

            #60-days ago estimates 
            d = datetime.timedelta(days=60)
            currentDate_60= currentDate - d
            updatedate = currentDate_60.strftime('%Y-%m-%d')
            qr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_monthyear_days60Ago"))
            qr1_est =   replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_days60Ago"))
            qr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_monthyear_days60Ago"))
            qr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_days60Ago"))
            fr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_monthyear_days60Ago"))
            fr1_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_days60Ago"))
            fr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_monthyear_days60Ago"))
            fr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_days60Ago"))
            epsgrowth_5year= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"growth_next5year_days60Ago"))
            qm_saveEstimatesIntoDatabase_raw(symbol,updatedate, qr1_monthyear, qr1_est, qr2_monthyear, qr2_est 
                                            ,fr1_monthyear, fr1_est, fr2_monthyear, fr2_est
                                            ,epsgrowth_5year
                                            ,dbi)

            #90-days ago estimates 
            d = datetime.timedelta(days=90)
            currentDate_90= currentDate - d
            updatedate = currentDate_90.strftime('%Y-%m-%d')
            qr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_monthyear_days90Ago"))
            qr1_est =   replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter1_days90Ago"))
            qr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_monthyear_days90Ago"))
            qr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_quarter2_days90Ago"))
            fr1_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_monthyear_days90Ago"))
            fr1_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year1_days90Ago"))
            fr2_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_monthyear_days90Ago"))
            fr2_est = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"est_year2_days90Ago"))
            epsgrowth_5year= replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"growth_next5year_days90Ago"))
            qm_saveEstimatesIntoDatabase_raw(symbol,updatedate, qr1_monthyear, qr1_est, qr2_monthyear, qr2_est 
                                            ,fr1_monthyear, fr1_est, fr2_monthyear, fr2_est
                                            ,epsgrowth_5year
                                            ,dbi)


        #save earning surprises 
        if (updatedate is not None and currentDate is not None):
            earning_monthyear = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"eps_est_current_monthyear"))
            epsEstimate = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"eps_est_current"))
            epsActual = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"eps_actual_current"))
            epsDifference = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"eps_diff_current"))
            epsPctDifference = replaceEmptyStringAsNone(getDictDataKeyValue(earningsEstimates,"eps_pctsurprice_current"))
            qm_saveEarningSurprises_raw(symbol
                                ,earning_monthyear
                                ,epsEstimate
                                ,epsActual
                                ,epsDifference
                                ,epsPctDifference
                                ,dbi)

    return



def qm_MakeQuoteMediaRequest(requestType, requestURL, token, dataFileName,dbi=None,bReueseExistingFile=False):
    #check in 
    if (        (requestType is None or len(requestType)<=0) 
            or (requestURL is None or len(requestURL )<=0) 
            or (token is None or len(token )<=0) 
            or (dataFileName is None or len(dataFileName )<=0)):
        print ("qmMakeQuoteMediaRequest() invalid arguments!")
        return 

    if ((not os.path.exists(dataFileName)) or (bReueseExistingFile)):
        strRequestTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');    
        print(("qmMakeQuoteMediaRequest() requestType:%s requestURL:%s dataFile:%s requestTime:%s")%(requestType, requestURL, dataFileName,strRequestTime))
        
        if (dbi is not None):
            #log the request into database
            sql = ("exec cycreal.dbo.sp_QuoteMedia_RequestLog_addRequest '%s','%s','%s'")%(requestType,strRequestTime, requestURL)
            if not (dbi.executeSQL(sql)):
                print(("Failed to save request Log! sql:%s")%(sql))
    

        #url = "http://app.quotemedia.com/data/getCompanyByExchange.xml?webmasterId=103529&excode=NGS"
        #url = ("http://app.quotemedia.com/data/getCompanyByExchange.xml?webmasterId=103529&excode=%s")%(exchange)
        savedDataFileName = ''
        textLength = 0 
        status_code = -1 
        
        url = requestURL
        bearStr = ("Bearer %s")%(token)
        headerObj={"Authorization":bearStr}
        response = requests.get(url, headers=headerObj)
        status_code = response.status_code
        strResponseTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');    
        if (status_code == 200):            
            print(("Succeeded to make a Quotemedia data request! requestURL:%s")%(url))
            #start to save the results into file 
            textLength = len (response.text)
            f = open(dataFileName,'w', encoding="utf-8")
            f.write(response.text)
            f.close()
            
            savedDataFileName =dataFileName
            print(('Company basic info saved into file:%s')%(dataFileName))
        else:
            savedDataFileName =''
            
        #log the request results
        if (dbi is not None):
            sql = ("exec cycreal.dbo.sp_QuoteMedia_RequestLog_addRequestResponse '%s','%s','%s',%d,%d,'%s'")%(requestType,strRequestTime, strResponseTime,status_code,textLength,savedDataFileName)
            if not (dbi.executeSQL(sql)):
                print(("Failed to save request Log! sql:%s")%(sql))
    else:
        savedDataFileName =dataFileName
        
    return savedDataFileName


