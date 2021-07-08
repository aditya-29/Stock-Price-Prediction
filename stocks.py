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
# print on screen to see output
# print(df)  
# writer = pd.ExcelWriter(symb1+"_"+symb2+".xlsx")
# df.to_excel(writer)
# writer.save()

#scope = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/drive']
#creds = ServiceAccountCredentials.from_json_keyfile_name('nseproject-319207-61852d50ecf0', scope)
#client = gspread.authorize(creds)

gc = gspread.service_account(filename='json file key.json')
gsheet = gc.open_by_key("1HsERTNlhYY48Ex5hIz8knyKUDQCVzdhc4DxrnxKhunI")
# Name for new worksheet
NwWkSht = symb1+symb2
gsheet.add_worksheet(rows=617,cols=28,title=NwWkSht)

#df=pd.DataFrame()
wks = gsheet.get_worksheet(1)
set_with_dataframe(wks, df,include_index=True)


