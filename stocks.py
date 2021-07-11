from nsepy import get_history
from datetime import date
import numpy as np
#from oauth2client import client
import pandas as pd
#from oauth2client.service_account import ServiceAccountCredentials
#import gspread
#import gspread_dataframe as gd
#from gspread_dataframe import set_with_dataframe
from statistics import stdev

start_date = date(2019, 1, 1)
end_date = date(2021,6, 10)
symb1 = 'JINDALPOLY'
symb2 = 'POLYPLEX'
df1 = get_history (symbol = symb1, start = start_date, end = end_date)
df1.rename(columns = {'Date':'Date',
                      'Symbol':symb1+'_Symbol',
                      'Series':symb1+'_Series',
                      'Prev Close':symb1+'_Prev Close',
                      'Open':symb1+'_Open',
                      'High':symb1+'_High',
                      'Low':symb1+'__Low',
                      'Last':symb1+'_Last',
                      'Close':symb1+'_Close',
                      'VWAP':symb1+'_VWAP',
                      'Volume':symb1+'_Vol',
                      'Turnover':symb1+'_TO',
                      'Trades':symb1+'_Trades',
                      'Deliverable Volume':symb1+'_DelVol',
                      '%Deliverble':symb1+'_%Del',                     
                     },inplace = True)
remove_list = [symb1+'_Symbol',
                symb1+'_Series',
                symb1+'_Prev Close',
                symb1+'_Open',
                symb1+'_High',
                symb1+'__Low',
                symb1+'_Last',
                symb1+'_Close',
                symb1+'_Vol',
                symb1+'_TO',
                symb1+'_Trades',
                symb1+'_DelVol',
                symb1+'_%Del',
                symb2+'_Symbol',
                symb2+'_Series',
                symb2+'_Prev Close',
                symb2+'_Open',
                symb2+'_High',
                symb2+'__Low',
                symb2+'_Last',
                symb2+'_Close',
                symb2+'_VWAP',
                symb2+'_Vol',
                symb2+'_TO',
                symb2+'_Trades',
                symb2+'_DelVol',
                symb2+'_%Del']
df2 = get_history (symbol = symb2, start = start_date, end = end_date)
df2.rename(columns = {'Date':'Date',
                      'Symbol':symb2+'_Symbol',
                      'Series':symb2+'_Series',
                      'Prev Close':symb2+'_Prev Close',
                      'Open':symb2+'_Open',
                      'High':symb2+'_High',
                      'Low':symb2+'__Low',
                      'Last':symb2+'_Last',
                      'Close':symb2+'_Close',
                      'VWAP':symb2+'_VWAP',
                      'Volume':symb2+'_Vol',
                      'Turnover':symb2+'_TO',
                      'Trades':symb2+'_Trades',
                      'Deliverable Volume':symb2+'_DelVol',
                      '%Deliverble':symb2+'_%Del',                     
                     },inplace = True)
df = pd.merge(df1,df2, how='outer',on='Date')
# fn for ratio column adder 
def Ratio_column_adder(a,b):
    df["x"]=df[a+'_VWAP']/df[b+'_VWAP']
    return df["x"]

df["Ratio"]=Ratio_column_adder(symb1,symb2)
# Ratio column added
#df["Ratio"]=df[symb1+'_VWAP']/df[symb2+'_VWAP']

    
# Basket column added
df["Basket"]=df["Ratio"]
for i in range(0,len(df["Basket"])-1):
    if(df["Basket"][i]>=1):
        df["Basket"][i]=(df["Basket"][i]*df[symb2+'_VWAP'][i])+df[symb1+'_VWAP'][i]
    else:
        df["Basket"][i]=(df["Basket"][i]*df[symb1+'_VWAP'][i])+df[symb2+'_VWAP'][i]

# MA5 column 
df["MA5"]=""
for i in range(5,len(df["MA5"])-1):
    df["MA5"][i-1]=sum(df["Basket"][i-5:i])/5 

# MA20 column 
df["MA20"]=""
for j in range(20,len(df["MA20"])-1):
    df["MA20"][j-1]=sum(df["Basket"][j-20:j])/20

# STDEV of basket
df["STDEV"]=""
for k in range(20,len(df["STDEV"])-1):
    df["STDEV"][k-1]=stdev(df["Basket"][k-20:k])

# Z-Score of the pair
df["Zscore"]=""
for l in range(20,len(df["Zscore"])-1):
    df["Zscore"][l-1]=(df["MA5"][l-1]-df["MA20"][l-1])/df["STDEV"][l-1]

# print on screen to see output
# print(df)  
for i in remove_list:
    df = df.drop(columns=[i],axis=1)
print(df)
# To make into an excel sheet
writer = pd.ExcelWriter(symb1+"_"+symb2+".xlsx")
df.to_excel(writer)
writer.save()

'''To create a google sheet and write the content
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
'''

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

