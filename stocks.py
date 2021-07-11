from gspread.urls import DRIVE_FILES_API_V3_URL
from nsepy import get_history
from datetime import date
import numpy as np
from oauth2client import client
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import gspread_dataframe as gd
from gspread_dataframe import set_with_dataframe
from statistics import stdev

start_date = date(2019, 1, 1)
end_date = date(2021,6, 10)
symb1 = 'JINDALPOLY'
symb2 = 'POLYPLEX'
symb3 = 'Basket'
df1 = get_history (symbol = symb1, start = start_date, end = end_date)
df1=df1.drop(['Series','Symbol','Prev Close','Open','High','Low','Last','Close','Volume','Turnover','Trades','Deliverable Volume','%Deliverble'],axis=1)
df1.rename(columns = {'Date':'Date',
                      'VWAP':symb1+'_VWAP',   
                     },inplace = True)

df2 = get_history (symbol = symb2, start = start_date, end = end_date)
df2=df2.drop(['Series','Symbol','Prev Close','Open','High','Low','Last','Close','Volume','Turnover','Trades','Deliverable Volume','%Deliverble'],axis=1)
df2.rename(columns = {'Date':'Date',
                      'VWAP':symb2+'_VWAP',                   
                     },inplace = True)
df = pd.merge(df1,df2, how='outer',on='Date')

# fn for ratio column adder 
def Ratio_column_adder(a,b):
    data=[]
    df3=pd.DataFrame(data,columns=["x"])
    df3["x"]=df[a+'_VWAP']/df[b+'_VWAP']
    return df3["x"]

df["Ratio"] = Ratio_column_adder(symb1,symb2)

# Basket column added
df["Basket"]=df["Ratio"]
for i in range(0,len(df["Basket"])-1):
    if(df["Basket"][i]>=1):
        df["Basket"][i]=(df["Basket"][i]*df[symb2+'_VWAP'][i])+df[symb1+'_VWAP'][i]
    else:
        df["Basket"][i]=(df["Basket"][i]*df[symb1+'_VWAP'][i])+df[symb2+'_VWAP'][i]


def Moving_Average(a,b):
    df["test"]=""
    df.rename(columns={"test":a+"_MA"+str(b)},inplace=True)
    for i in range(b,len(df[a+"_MA"+str(b)])-1):
        df[a+"_MA"+str(b)][i-1]=sum(df[a+"_VWAP"][i-b:i])/b
    return None

def Std_Dev(a):
    df["test"]=""
    df.rename(columns={"test":a+"_STDEV"},inplace=True)
    for k in range(20,len(df[a+"_STDEV"])-1):
        df[a+"_STDEV"][k-1]=stdev(df[a+"_VWAP"][k-20:k])
    return None

def Z_Score(a):
    df["test"]=""
    df.rename(columns={"test":a+"_Zscore"},inplace=True)
    for l in range(20,len(df[a+"_Zscore"])-1):
        df[a+"_Zscore"][l-1]=(df[a+"_MA5"][l-1]-df[a+"_MA20"][l-1])/df[a+"_STDEV"][l-1]
    return None

# To initiate buy for basket 
def Basket_buy_initiate():
    pos=0
    for score in df["Basket_Zscore"].values:
        if(score==''):
            pos=pos+1
            continue
        if(float(score)<=-1):
            if(float(df[symb1+"_Zscore"][pos])!=''):
                if(float(df[symb1+"_Zscore"][pos])<=-1):
                    if(float(df[symb2+"_Zscore"][pos+1])<=-1):
                        if(float(df[symb1+"_Zscore"][pos+1])<=float(df[symb2+"_Zscore"][pos])):
                            df["Buy_initiate"][pos+1]=-float(df[symb1+"_VWAP"][pos+1])
                            pos=pos+1
                            continue
                        else:
                            df["Buy_initiate"][pos+1]=-float(df[symb2+"_VWAP"][pos+1])
                            pos=pos+1
                            continue  
                    else:
                        pos=pos+1
                        continue
                else:
                    pos=pos+1
                    continue
            else:
                break
        else:
            pos=pos+1
            continue
    return None

# To initiate buy for single stock
def Single_buy_initiate(a):
    pos=0
    for score in df[a+"_Zscore"].values:
        if(score==''):
            pos=pos+1
            continue
        if(score<=-1):
            if(df[a+"_Zscore"][pos+1]!=''):
                if(float(df[a+"_Zscore"][pos+1])<=-1):
                    df[a+"_Buy_initiate"][pos+1]=-float(df[a+"_VWAP"][pos+1])
                    pos=pos+1
                    continue
                else:
                    pos=pos+1
                    continue
            else:
                break
        else:
            pos=pos+1
            continue
    return None
"""
# Sell value estimation
def sell_absolute(a,b):
    lis=[]

    return lis

# To initiate sell for basket
def Sell_initiate(a):
    pos1=0
    pos2=0
    sum=0
    lis=[]
    for score in df[a+"_Buy_initiate"].values:
        if(score==''):
            pos2=pos2+1
            continue
        if(score>=1):  
            lis=sell_absolute(pos1,pos2)         

    return None

"""

# Basket MA5 column 
df["Basket_MA5"]=""
for i in range(5,len(df["Basket_MA5"])-1):
    df["Basket_MA5"][i-1]=sum(df["Basket"][i-5:i])/5

# Basket MA20 column 
df["Basket_MA20"]=""
for j in range(20,len(df["Basket_MA20"])-1):
    df["Basket_MA20"][j-1]=sum(df["Basket"][j-20:j])/20

# STDEV of basket
df["Basket_STDEV"]=""
for k in range(20,len(df["Basket_STDEV"])-1):
    df["Basket_STDEV"][k-1]=stdev(df["Basket"][k-20:k])

# Z_Score of the basket
Z_Score(symb3)
df["Buy_initiate"]=""

# Parameters of Symb1
Moving_Average(symb1,5)
Moving_Average(symb1,20)
Std_Dev(symb1)
Z_Score(symb1)
df[symb1+"_Buy_initiate"]=""

# Parameters of Symb2
Moving_Average(symb2,5)
Moving_Average(symb2,20)
Std_Dev(symb2)
Z_Score(symb2)
df[symb2+"_Buy_initiate"]=""

Basket_buy_initiate()
Single_buy_initiate(symb1)
Single_buy_initiate(symb2)
# To create a google sheet and write the content
gc = gspread.service_account(filename='json file key.json') 
# json file availabe in my branch

gsheet = gc.open_by_key("1HsERTNlhYY48Ex5hIz8knyKUDQCVzdhc4DxrnxKhunI") 
# Sheets key

# Name for new worksheet
NwWkSht = symb1+symb2
gsheet.add_worksheet(rows=617,cols=31,title=NwWkSht)

#df=pd.DataFrame()
wks = gsheet.get_worksheet(1)
set_with_dataframe(wks, df,include_index=True)



        



