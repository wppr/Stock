
import tushare as ts
import sqlite3
import pickle
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine 
import time
from DataProcess import * 
from StockTrack import * 
from StockUtil import * 
import threading
import os
import sys

#定义全局变量
class g:
    track=1
    df=1
    StockBasic=1
    tradedays=1
    Sepdfs=1
    AdjFactor=1
    CurTradeDay=1
    ma=1
today=GetToday()
startDay='20200101'

#启动参数
bAppMode=False
#StockBasic=GetStockBasic()

#def GetStockHistory(df,code):
#    q=df[df.ts_code==code].sort_values(by=['trade_date'],ascending=True )
#    return q

def CalcAllMean2(dfs):
    dfList=[]
    i=0
    for key,stockInfo in dfs.items():
        i=i+1
        if i%100==0:
            print('process mean ', i)  
        stockInfo=stockInfo[['ts_code','trade_date','close']].sort_values(by=['trade_date'],ascending=True)      
        stockInfo['Ma5']=stockInfo['close'].rolling(5,min_periods=1).mean().round(2)
        stockInfo['Ma14']=stockInfo['close'].rolling(14,min_periods=1).mean().round(2)
        dfList.append(stockInfo)
    print('process mean done')
    return pd.concat(dfList)

def TryGetStock(x,startDay,endDay):
    for _ in range(10):
        try:
            df=ts.pro_bar(ts_code=x, adj='qfq', start_date=startDay, end_date=endDay,ma=[5,14])
        except:
            time.sleep(4)
        else:
            return df
def UpdateStocks(stocks,df,startDay,endDay):
    df1=df.drop(df[df.ts_code.isin(stocks)].index)
    print('drop',df.shape[0])
    dfs=[0]*len(stocks)
    i=0
    for x in stocks:
        a=TryGetStock(x,startDay,endDay)
        dfs[i]=a
        i=i+1
        print('GetStockData ',i,x)
    b=pd.concat(dfs)
    df=pd.concat([df1,b])
    print(df.shape[0])
    return df
def DailyUpdate(df):
    print("Daily Update")
    days=GetTradeDays(startDay,today)
    grp=df.groupby('trade_date')
    bUpdated=False
    for day in days:
        if not day in grp.groups:
            res=WriteDailyTradeData(day)
            if not res:
                break
            bUpdated=True    
    
    
    if bUpdated:
        df=LoadDailyData()
        print("Update Trade Data")
        UpdateStockBasic()
        UpdateTradeDays()
        #os._exit(0)
    else:
        print('Already Update')



def DoReport(track,df,day,StockBasic):
    a=track[track.trade_date==day][(track.state==2)|(track.state==1)|(track.state==3)]
    if a.empty:
        print(day,"空日期")
        return
    a=a.sort_values(by=['countZF'],ascending=False)
    b=df[df.trade_date==day]
    stocks=a['ts_code'].tolist()
    c=b[b.ts_code.isin(stocks)]
    d=pd.merge(a,c,on=['trade_date','ts_code','close'])
    e=JoinBasicInfo(d,StockBasic)
    e.fillna(0)
    e['Weight'] = (e['countZF'].mul(e['amount']).mul(e['close'])/100000000).round(3)  
    e['amount'] = (e['amount']/100000).round(3)  
    e['pct_alert']=e.apply(lambda x: x['alertCount'] / x['countDay'] if x['countDay']>0 else 999, axis=1)
    e['state2']='上涨'
    e['state2']=e.apply(lambda x:GetState(x['state']),axis=1)
    e=e.sort_values(by=['Weight'],ascending=False)
    order=[ 'name','countZF','avgZF','Weight','countDay','alertCount','pct_alert','state2','startDay','startPrice','pct_chg','amount','trade_date','close','Ma5','Ma14',
          'vol','symbol','area','industry']
    e=e[order]
    #e['state'].replace(2,'上涨',inplace=True)
    e.rename(columns = {'name':'名字',"countZF": "累计涨幅", "avgZF":"平均每日涨幅",'Weight':'权重','trade_date':'日期','close':'收盘价','pct_chg':'今日涨幅'
                        ,'Ma5':'5日均线','Ma14':'14日均线','state':'状态','countDay':'上涨天数','startDay':'启动日期','startPrice':'启动价格',
                        'alertCount':'跌破5日线次数','pct_alert':'跌破5日线概率'
                        ,'amount':'成交额(亿)','state2':'状态','state':'状态','state':'状态','state':'状态'},  inplace=True)
    e.to_csv('track_'+day+'.csv')
    print("生成报告 ",day)

def LoadDBData():
    print("从数据库加载基本数据")
    g.StockBasic=LoadStockBasicData()
    g.tradedays=GetTradeDaysFromDB()
    
    g.df=LoadDailyData()
    g.ma=LoadMa()
    g.CurTradeDay=GetLastTradeDay(today,g.tradedays)
    g.trackData=LoadTable('Track')
    print("End")

def CalcMeans(df):
    DfsByCode=SeperateAllDf(df,g.StockBasic)
    ma=CalcAllMean2(DfsByCode)
    SaveMa(ma)
    print("保存均线完毕")
def UpdateTrack(df,StockBasic,db):
    endDay=g.CurTradeDay
    stocks=df[df.trade_date==endDay][df.amount*1000>250000000]['ts_code'].tolist()
    print("活跃股数",endDay,len(stocks))
    ma=LoadMa()
    masByCode=SeperateAllDf(ma,StockBasic)
    DfsByCode=SeperateAllDf(df,StockBasic)
    RunTrack(g.track,stocks,DfsByCode,masByCode,db)

def UpdateFuQuan(df,startDay,endDay):
    adj=GetAdjRange(startDay,endDay)
    #adj=LoadTable("AdjFactor")
    stocks=GetAdjChangeList(adj)
    print("更新复权 ",len(stocks))
    df=UpdateStocks(stocks,df,'20200101',today)
    SaveTradeData(df)

        

if __name__ == '__main__':
    print('main')
    #LoadDBData()
    #adj=LoadTable("AdjFactor")
    #s=GetAdjChangeList(adj)
    if not bAppMode:
    #     app = QApplication(sys.argv)
    #     ex = Example()
    #     sys.exit(app.exec_())
    #else:
        LoadDBData()
        g.track=Track1()
        g.track.Init()
        trackData=LoadTable('Track')
        df=g.df
        endDay=g.CurTradeDay
        ActiveStocks=df[df.trade_date==endDay][df.amount*1000>300000000]['ts_code'].tolist()
        print("活跃股数",endDay,len(ActiveStocks))
        ma=LoadMa()
        masByCode=SeperateAllDf(ma,g.StockBasic)
        DfsByCode=SeperateAllDf(g.df,g.StockBasic)
        RunTrack(g.track,ActiveStocks,DfsByCode,masByCode,db)
    #
    #stockList=StockBasic['ts_code'].tolist()
    #code='002475.SZ'
    
    #b.to_sql('Trade', db, index=False, if_exists='replace')
    #df['ts_code'].notin(StockBasic['ts_code'].tolist())

    #DoReport(track,g.df,g.CurTradeDay,g.StockBasic)
    #DoReport(g.trackData,g.df,g.StockBasic,today)