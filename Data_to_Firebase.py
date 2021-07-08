import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from nsepy import get_history
from datetime import date
import numpy as np
import datetime
import pandas as pd
cred = credentials.Certificate('./stock-prediction-804e2-firebase-adminsdk-4ny8o-89c56b89b9.json')
firebase_admin.initialize_app(cred, 
{
'databaseURL': 'https://Stock prediction.firebaseio.com/'
})
db = firestore.client()
doc_ref = db.collection(u'JINDALPOLY')# Import data


start_date = date(2019, 1, 1)
end_date = date(2021,6, 10)
symb = 'JINDALPOLY'
#symb= input('Enter the company symbol code you want a history of')
df= get_history (symbol=symb,start = start_date, end = end_date)
print(len(df.columns))
df.rename(columns = { 'Date': symb+'Date', 
                      'Symbol':symb+'_Symbol',
                      'Series':symb+'_Series',
                      'Prev Close':symb+'_Prev Close',
                      'Open':symb+'_Open',
                      'High':symb+'_High',
                      'Low':symb+'__Low',
                      'Last':symb+'_Last',
                      'Close':symb+'_Close',
                      'VWAP':symb+'_VWAP',
                      'Volume':symb+'_Vol',
                      'Turnover':symb+'_TO',
                      'Trades':symb+'_Trades',
                      'Deliverable Volume':symb+'_DelVol',
                      '%Deliverble':symb+'_%Del',                     
                     },inplace = True)
print(df)
tmp = df.to_dict(orient='records')
print(tmp)
list(map(lambda x: doc_ref.add(x), tmp))