import time
import datetime
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine 

def GetToday():
    today=time.strftime("%Y%m%d", time.localtime())
    print("今日",today)
    now=datetime.datetime.now()
    if int(now.hour)<=15:
        curDay=now-datetime.timedelta(days=1)
        today=curDay.strftime("%Y%m%d")
   
    print("当前交易日",today)
    return today
def GetNTradeDays(curDay,N,tradedays):
    date=datetime.datetime.strptime(curDay,"%Y%m%d")#字符串转化为date形式
    daytime=datetime.timedelta(days=1)
    days=[] 
    count=0
    while(count<N):
       
        dayStr=date.strftime("%Y%m%d")
        d=tradedays[tradedays.cal_date==dayStr]
        if d.shape[0]==0:
            print("error calc day")
            break
        if d.iloc[0]['is_open']==1:
            days.append(dayStr)
            count=count+1
        date=date-daytime
    return days        
def GetLastTradeDay(today,tradedays):
    a=GetNTradeDays(today,1,tradedays)
    print(today,"获取 最近交易日",a[-1])
    return a[-1]


def IsZhangTing(row):
    if row['pct_chg']>9.8 and row['high']==row['close']:
        return True
    else:
        return False

def GetMean(df,code,day,N):#获取平均价格
    days=GetNTradeDays(day,N,tradedays)
    q=df[df.ts_code==code]
    q=q[q.trade_date.isin(days)]
    m=q['close'].mean()
    return m
#def DailyAnalyze():
#    ztg=df[df.pct_chg>9.8][ df.trade_date==today]
#    b=JoinBasicInfo(ztg)
#    f=open('1.txt','w')
#    f.write(b.to_csv())


def GetDivideRanges(start,end,step):
    r=[]
    while start<end:
        newstart=start+step
        if newstart>=end:
            r.append([start,end])
        else:
            r.append([start,newstart])
        start=newstart+1
    return r

def SubUpdateStocks(stocks,start,end,dfs,startDay,endDay):
    for i in range(start,end+1):
        x=stocks[i]
        a=ts.pro_bar(ts_code=x, adj='qfq', start_date=startDay, end_date=endDay)
        print('GetStockData ',i,x)
        dfs[i]=a
def UpdateStocksMT(stocks,df,startDay,endDay):
    r=GetDivideRanges(0,len(stocks)-1,2000)
    dfs=[0]*len(stocks)
    ths=[]
    for x in r:
        t = threading.Thread(target=SubUpdateStocks,args=(stocks,x[0],x[1],dfs,startDay,endDay,))
        ths.append(t)
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    df=pd.concat(dfs)
    return df

def SeperateAllDf(df,StockBasic):
    print('Seperate Begin')
    dfs={}
    grp=df.groupby('ts_code')
    for i,row in StockBasic.iterrows():     
        key=row['ts_code']
        if key in grp.groups:
            dfs[key]=(grp.get_group(key).copy())   
    print('Seperate End')
    return dfs
def JoinBasicInfo(df,StockBasic):
    b=df.set_index('ts_code').join(StockBasic.set_index('ts_code'))
    return  b