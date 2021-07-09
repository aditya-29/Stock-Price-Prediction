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
Zdf= get_history (symbol=symb,start = start_date, end = end_date)
print(len(df.columns))
df.rename(columns = { 'Date'                :'Date', 
                      'Symbol'              :'Symbol',
                      'Series'              :'Series',
                      'Prev Close'          :'Prev Close',
                      'Open'                :'Open',
                      'High'                :'High',
                      'Low'                 :'Low',
                      'Last'                :'Last',
                      'Close'               :'Close',
                      'VWAP'                :'VWAP',
                      'Volume'              :'Vol',
                      'Turnover'            :'TO',
                      'Trades'              :'Trades',
                      'Deliverable Volume'  :'DelVol',
                      '%Deliverble'         :'%Del',                     
                     },inplace = True)
df = df.reset_index()
df["Date"] = df.Date.apply(lambda x: x.strftime('%Y-%m-%d'))
tmp = df.to_dict(orient='records')
list(map(lambda x: doc_ref.add(x), tmp))