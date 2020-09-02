import tushare as ts
import sqlite3
import pandas as pd
import tushare as ts
import time
import datetime
import threading

dayState=1 #当前 新进入， 退出  0 undefine 1 上穿  2 上涨趋势 3 下穿 4.没上涨
# 进入 每天 信息 
#StartDay
#Days
#total zf
#avg zf
#zf
#amount
bIgonreNewStocks=True
def GetState(n):
    if n==2:
        return '上涨'
    elif n==1:
        return '突破'
    elif n==3:
        return '跌破'
    else:
        return '未知'

#趋势股追踪器
class Track1():
    def Init(self):
        self.dfList=[]
        self.stockToIndex={}
    def SetStocks(self,stocks):
        self.stockToIndex={}
        i=0
        for x in stocks:
            self.stockToIndex[x]=i
            i=i+1
        self.dfList=[0]*i
        self.dfFlag=[False]*i

    def ProcessRange(self,startDay,endDay):
        self.a=1
    def Clear(self):
        1
    def GetState(day):
        1
    def AlertCond(self,row,ma):
        if row['close']<ma['Ma5']:
            return True
        else:
            return False
    def InPoolCond(self,row,row0,ma,ma0):
        factor=1
        if row['amount']<200000:#2亿
            factor=1.02
        if ma0['Ma5']<ma0['Ma14']*factor and ma['Ma5']>ma['Ma14']*factor:
            return True
        return False
    def OutPoolCond(self,row,ma):
        if ma['Ma5']<ma['Ma14']:
            return True
        else:
            return False

    def Process(self,code,df,maT):
        #输入一个股的历史记录。计算出其进入上升日期，持续日期，离开日期
        a=df.sort_values(by=['trade_date'],ascending=True )
        b=maT.sort_values(by=['trade_date'],ascending=True ).copy()
        print('Process Start ',code,'线程',threading.current_thread().ident)
        #c=pd.merge(a,b,on=['trade_date','ts_code','close'])
        #print(code,a.shape[0])
        assert a.shape[0]==b.shape[0]
        b['state']=0
        b['startDay']=''
        b['startPrice']=0.0
        b['countDay']=0
        b['countZF']=0.0
        b['avgZF']=0.0
        b['alertCount']=0
        state=0
        startDay=''
        startPrice=0 
        startIndex=0
        alertCount=0
       
        for i in range(a.shape[0]):
            ma=b.iloc[i]
            row=a.iloc[i]
            if i>=1:
                row0=a.iloc[i-1]
                ma0=b.iloc[i-1]
                if (state==0 or state==4):
                    if self.InPoolCond(row,row0,ma,ma0):
                        state=1
                        startDay=row['trade_date']
                        startPrice=row['close']
                        startIndex=i
                        alertCount=0
                elif(state==1 or state==2):
                    if self.OutPoolCond(row,ma):
                        state=3
                        startDay=0
                        startPrice=0
                        startIndex=0
                    else:
                        state=2
                    if self.AlertCond(row,ma):
                        alertCount=alertCount+1
                elif (state==3 or state==4):
                    if state==3 and (not self.InPoolCond(row,row0,ma,ma0)):
                        state=4

      #  Index(['ts_code', 'trade_date', 'close', 'Ma5', 'Ma14', 'state', 'startDay',
      # 'startPrice', 'countDay', 'countZF', 'avgZF', 'alertCount'],
      #dtype='object')
                b.iloc[i,5]=state
                if(state==2 or state==1 or state==3):
                    b.iloc[i,6]=startDay
                if(state==2):
                    b.iloc[i,7]=startPrice
                    b.iloc[i,8]=i-startIndex #countZF
                    countzf=round(row['close']/startPrice-1,3)*100
                    b.iloc[i,9]=countzf
                    b.iloc[i,10]=round(countzf/(i-startIndex),2)
                    b.iloc[i,11]=alertCount
                #print(row['trade_date'],state,round(ma['Ma5'],2),round(ma['Ma14'],2),b.iloc[i,9])
                #print(b.iloc[i])
        if code in self.stockToIndex:
            index=self.stockToIndex[code]
            self.dfList[index]=b
            self.dfFlag[index]=True
        print('Process end   ',code,index,"/",len(self.dfList)-1,'线程',threading.current_thread().ident)

def RemoveListValue(list_i,value):
    j=0
    for i in range(len(list_i)):
        if list_i[j] == value:
            list_i.pop(j)
        else:
            j += 1

def RunTrack(t,stocks,DfsByCode,masByCode,db):
    t.SetStocks(stocks)
    for key in stocks:
        if (key in DfsByCode) and( key in masByCode):
            if bIgonreNewStocks and DfsByCode[key].shape[0]<30:
                print('新股剔除 ',key)
                continue
            if DfsByCode[key].shape[0]==masByCode[key].shape[0]:
                t.Process(key,DfsByCode[key],masByCode[key])
            else:
                print('error not equal',key)
        else:
            print('error',key,key in DfsByCode,key in masByCode)
    #RemoveListValue(t.dfList,0)
    newList=[]
    for x in t.dfList:
        if not type(x)==type(0):
            newList.append(x)
        
    newMa=pd.concat(newList)
    newMa.to_sql('Track', db, index=False, if_exists='replace', chunksize=1000)
    print("Write Track End")
def GetRanges(start,end,step):
    r=[]
    while start<end:
        if start+step-1>=end:
            r.append([start,end])
            break
        else:
            r.append([start,start+step-1])
            start=start+step
    return r
def SubRunTrack(t,stocks,start,end,DfsByCode,masByCode):
    for i in range(start,end+1):
        key=stocks[i]
        t.Process(key,DfsByCode[key],masByCode[key])

def RunTrackMT(t,stocks,DfsByCode,masByCode,db):
    t.SetStocks(stocks)
    r=GetRanges(0,len(stocks)-1,300)
    ths=[]
    for x in r:
        th = threading.Thread(target=SubRunTrack,args=(t,stocks,x[0],x[1],DfsByCode,masByCode))
        ths.append(th)
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    print("run track mt end")
    #Mas=pd.concat(t.dfList)
    #Mas.to_sql('Track', db, index=False, if_exists='replace', chunksize=1000)