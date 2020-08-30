
import tushare as ts
import sqlite3
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine 
import time
import datetime

db =create_engine('sqlite:///stock.db')
ts.set_token('f605af9e97cbe8661289b9c975a23d48b0d8ac09d8d9059d4cb39204')
pro = ts.pro_api()

def GetTradeDaysFromDB():
    tradedays=pd.read_sql("tradeDays",db)
    return tradedays


def LoadStockBasicData():
    df=pd.read_sql("stock_basic",db)
    return df

#def GetStockBasic():
#    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
#    return data

def UpdateTradeDays():
    tradedays=pro.query('trade_cal', start_date='20170101', end_date='20201230')
    tradedays.to_sql('tradeDays', db, index=False, if_exists='replace', chunksize=1000)
    print("UpdateTradeDays ok")
    return tradedays


def UpdateStockBasic():
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,list_status')
    res = df.to_sql('stock_basic', db, index=False, if_exists='replace', chunksize=1000)

    print("UpdateStockBasic",res)

def WriteDailyTradeData(date):
    df = pro.daily(trade_date=date)
    if df.shape[0]==0:
        return False
    res = df.to_sql('Trade', db, index=False, if_exists='append')
    print("WriteDailyTradeData OK",date)
    return True

def GetTradeDays(start,end):
    days = pro.query('trade_cal', start_date=start, end_date=end)
    tradedays=[]
    for index, row in days.iterrows():
        if row['is_open']==1:
            tradedays.append(row['cal_date'])
    return tradedays
def GetAdjRange(start,end):
    days=GetTradeDays(start,end)
    dfs=[]
    for d in days:
        df = pro.adj_factor(ts_code='', trade_date=d)
        dfs.append(df)
    q=pd.concat(dfs)
    return q

def GetAdjChangeList(df):
    grp=df.groupby('ts_code')
    stocks=[]
    for x in grp.groups:
        d=grp.get_group(x)
        factor=0
        result=True
        i=0
        for index,row in d.iterrows():
            if i==0:
                factor=row['adj_factor']
            if i>0 and row['adj_factor']!=factor:
                result=False
                print("发生复权变化 ",x,row['trade_date'])
                stocks.append(x)
                break
            i=i+1
    return stocks

def GetRangeDailyAndSave(start,end):
    days = pro.query('trade_cal', start_date=start, end_date=end)
    for index, row in days.iterrows():
        print(row['cal_date'],row['is_open'])
        if row['is_open']==1:
            WriteDailyTradeData(row['cal_date'])

def SaveMa(df):
    df.to_sql('MA', db, index=False, if_exists='replace', chunksize=1000)
    print("SaveMA End")
def SaveAdjFactor(df):
    df.to_sql('AdjFactor', db, index=False, if_exists='replace', chunksize=1000)
    print("SaveAdjFactor")
def LoadTable(name):
     return pd.read_sql(name,db)
def LoadMa():
    return pd.read_sql("MA",db)
#def read_data():
#    sql = """SELECT * FROM stock_basic LIMIT 20"""
#    df = pd.read_sql_query(sql, db)
#    return df


def LoadDailyData():
    print('Load Daily Data')
    df=pd.read_sql("Trade",db)
    return df

def SaveTradeData(df):
    df.to_sql('Trade', db, index=False, if_exists='replace', chunksize=1000)
    print("SaveTradeData")

