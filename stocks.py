from nsepy import get_history
from datetime import date
import numpy as np
import pandas as pd

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
writer = pd.ExcelWriter(symb1+"_"+symb2+".xlsx")
df.to_excel(writer)
writer.save()