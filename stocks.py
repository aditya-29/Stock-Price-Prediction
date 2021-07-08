from nsepy import get_history
from datetime import date
import numpy as np
from oauth2client import client
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import gspread_dataframe as gd
from gspread_dataframe import set_with_dataframe

start_date = date(2019, 1, 1)
end_date = date(2021,1, 10)
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

# Ratio column added
df["Ratio"]=df[symb1+'_VWAP']/df[symb2+'_VWAP']

# Basket column added
df["Basket"]=df["Ratio"]
for i in range(0,len(df["Basket"])-1):
    if(df["Basket"][i]>=1):
        df["Basket"][i]=(df["Basket"][i]*df[symb2+'_VWAP'][i])+df[symb1+'_VWAP'][i]
    else:
        df["Basket"][i]=(df["Basket"][i]*df[symb1+'_VWAP'][i])+df[symb2+'_VWAP'][i]

# MA5 column 
df["MA5"]=""
for i in range(4,len(df["MA5"])-1):
    df["MA5"][i]=(df["Basket"][i-4]+df["Basket"][i-3]+df["Basket"][i-2]+df["Basket"][i-1]+df["Basket"][i])/5 

# MA5 column 
df["MA20"]=""
for i in range(19,len(df["MA20"])-1):
    rf=df.loc[i-19:i,30]
    df["MA20"]=rf.sum(axis=0)
   

# print on screen to see output
# print(df)  

# To make into an excel sheet
"""writer = pd.ExcelWriter(symb1+"_"+symb2+".xlsx")
df.to_excel(writer)
writer.save()"""

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


