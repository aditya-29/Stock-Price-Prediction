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
from statistics import mean, median, stdev

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

# Fn for moving averages, standard deviation and Z Score
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

# Z_Score of the basket using fn call
Z_Score(symb3)

df["Zscore_ratio"]=""

# Adding columns for buy initiate and company indicator
df["Buy_initiate"]=""
df["S_company"]=""

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
                            df["S_company"][pos+1]=str(symb1)
                            pos=pos+1
                            continue
                        else:
                            df["Buy_initiate"][pos+1]=-float(df[symb2+"_VWAP"][pos+1])
                            df["S_company"][pos+1]=str(symb2)
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
                    df[a+"_Qty"][pos+1]=1
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

# Basket Sell value estimation
def sell_absolute():
    pos1=0
    pos2=0
    s1=0
    s2=0
    for pos in range(0,len(df["Basket_Zscore"])):    
        if(df["Basket_Zscore"][pos]==''):
            continue
        elif(float(df["Basket_Zscore"][pos])>=1):
            if(float(df["Zscore_ratio"][pos])>=0.8):
                pos2=pos
            else: 
                continue
            for comp in df["S_company"][pos1:pos2]:
                if(comp==symb1):
                    s1=s1+1
                    continue
                elif(comp==symb2):
                    s2=s2+1
                    continue
                else:
                    continue
            df["Buy_initiate"][pos2]=(s1*float(df[symb1+"_VWAP"][pos2]))+(s2*float(df[symb2+"_VWAP"][pos2]))
            s1=0
            s2=0
            pos1=pos2
            pos2=pos2+1
        else:
            pos2=pos2+1
    return None


# individual sell initiate
def ind_sell(a):
    pos1=0
    pos2=0
    s=0
    for score in df[a+"_Zscore"]:
        if(score==''):
            pos2=pos2+1
            continue
        elif(float(score)>=1):
            for cel in df[a+"_Qty"][pos1:pos2]:
                if(cel!=''):
                    if(int(cel)==1):
                        s=s+1
                else:
                    continue
            df[a+"_Buy_initiate"][pos2]=s*df[a+"_VWAP"][pos2]
            s=0
            pos1=pos2
            pos2=pos2+1
        else:
            pos2=pos2+1
    return None

# To remove the stocks from the column without a sell date
def column_cleanser(a):
    for i in range(len(df[a+"_Buy_initiate"]),0,-1):
        if(df[a+"_Buy_initiate"][i-1]==''):
            df[a+"_Buy_initiate"][i-1]=0
            df[a+"_Qty"][i-1]=0
        elif(float(df[a+"_Buy_initiate"][i-1])>=0):
            break
        else:
            df[a+"_Buy_initiate"][i-1]=0    
            df[a+"_Qty"][i-1]=0        
    return None

def basket_cleanser():
    for i in range(len(df["Buy_initiate"]),0,-1):
        if(df["Buy_initiate"][i-1]==''):
            df["Buy_initiate"][i-1]=0
        elif(float(df["Buy_initiate"][i-1])>=0):
            break
        else:
            df["Buy_initiate"][i-1]=0            
    return None

# Parameters of Symb1
Moving_Average(symb1,5)
Moving_Average(symb1,20)
Std_Dev(symb1)
Z_Score(symb1)
df[symb1+"_Buy_initiate"]=""
df[symb1+"_Qty"]=""

# Parameters of Symb2
Moving_Average(symb2,5)
Moving_Average(symb2,20)
Std_Dev(symb2)
Z_Score(symb2)
df[symb2+"_Buy_initiate"]=""
df[symb2+"_Qty"]=""

for scores in range(19,len(df[symb3+"_Zscore"])):
    if(df[symb1+"_Zscore"][scores]!=''):
        df["Zscore_ratio"][scores]=float(df[symb1+"_Zscore"][scores])/float(df[symb2+"_Zscore"][scores])
    else:
        continue

# Buying and selling process
Basket_buy_initiate()
Single_buy_initiate(symb1)
Single_buy_initiate(symb2)
sell_absolute()
ind_sell(symb1)
ind_sell(symb2)

# Columns added to calculate profits
df[symb3+"_Profit"]=''
df[symb1+"_Profit"]=''
df[symb2+"_Profit"]=''

df[symb3+"_Buy_date"]=''
df[symb3+"_Sell_date"]=''

# To calculate the profit of the basket
def Basket_profit_calc():
    sell=0
    buy=0
    sellcount=0
    for val in df["Buy_initiate"].values:
        if(val!=''):
            if(float(val)>0):
                sell=sell+val
                sellcount=sellcount+1
            elif(float(val)<0):
                buy=buy+val
            else:
                continue
        else:
            continue
    if(buy<0 and sell>0):
        profit=buy+sell
        df[symb3+"_Profit"][1]=sellcount
        df[symb3+"_Profit"][2]=buy
        df[symb3+"_Profit"][3]=sell
        df[symb3+"_Profit"][4]=profit
        df[symb3+"_Profit"][5]=(profit/-buy)*100
        profit=buy+sell
    elif(buy==0):
        profit=buy+sell
        df[symb3+"_Profit"][1]="Did not buy"
        df[symb3+"_Profit"][2]="No buy to sell"
        df[symb3+"_Profit"][3]=profit
        df[symb3+"_Profit"][4]=0
    elif(sell==0):
        profit=buy+sell
        df[symb3+"_Profit"][1]=buy
        df[symb3+"_Profit"][2]="No sell condition"
        df[symb3+"_Profit"][3]="No sell for profit"
        df[symb3+"_Profit"][4]=0
    return None

# To calculate single company zscore profit 
def Profit_calc(a):
    sell=0
    buy=0
    sellcount=0
    for val in df[a+"_Buy_initiate"].values:
        if(val!=''):
            if(float(val)>0):
                sell=sell+val
                sellcount=sellcount+1
            elif(float(val)<0):
                buy=buy+val
            else:
                continue
        else:
            continue
    if(buy<0 and sell>0):
        profit=buy+sell
        df[a+"_Profit"][1]=sellcount
        df[a+"_Profit"][2]=buy
        df[a+"_Profit"][3]=sell
        df[a+"_Profit"][4]=profit
        df[a+"_Profit"][5]=(profit/-buy)*100
        profit=buy+sell
    elif(buy==0):
        profit=buy+sell
        df[a+"_Profit"][1]="Did not buy"
        df[a+"_Profit"][2]="No buy to sell"
        df[a+"_Profit"][3]=profit
        df[a+"_Profit"][4]=0
    elif(sell==0):
        profit=buy+sell
        df[a+"_Profit"][1]=buy
        df[a+"_Profit"][2]="No sell condition"
        df[a+"_Profit"][3]="No sell for profit"
        df[a+"_Profit"][4]=0
    return None

# Cleansing the buy initiate columns
column_cleanser(symb1)
column_cleanser(symb2)
basket_cleanser()

# Calculating the buy profits
Basket_profit_calc()
Profit_calc(symb1)
Profit_calc(symb2)

def date_entry():
    i=0
    c=0
    if(len(df1.index)>len(df2.index)):
        p=df2.index
    else:
        p=df1.index

    for it in range(1,len(df["Buy_initiate"])):
        if(df["Buy_initiate"][it]==''):
            continue
        elif(float(df["Buy_initiate"][it])>0):
            df[symb3+"_Sell_date"][i]=p[it]
            i=i+1
        elif(float(df["Buy_initiate"][it])<0):
            df[symb3+"_Buy_date"][i]=p[it]
            i=i+1
        else:
            continue

    return None

date_entry()

# To create a google sheet and write the content
gc = gspread.service_account(filename='json file key.json') 
# json file availabe in my branch

gsheet = gc.open_by_key("1HsERTNlhYY48Ex5hIz8knyKUDQCVzdhc4DxrnxKhunI") 
# Sheets key

# Name for new worksheet
NwWkSht = symb1+symb2
wks=gsheet.add_worksheet(rows=617,cols=31,title=NwWkSht)

#df=pd.DataFrame()
#wks = gsheet.get_worksheet(1)
set_with_dataframe(wks, df,include_index=True)

# Creating summary dataframe
newdf={'Analysis_Period_Begin':[start_date],'Analysis_Period_End':[end_date],'Script_Name_1':[symb1],'Script_Name_2':[symb2]}

newdf=pd.DataFrame()

# Adding columns of mean and median of scripts to the dataframe
newdf[symb1+'_Mean']=''
newdf[symb1+'_Median']=''

newdf[symb2+'_Mean']=''
newdf[symb2+'_Median']=''

# Noting the dates of the trade cycle
newdf['Buy_Begin_Date']=''
newdf['Buy_End_Date']=''
newdf['Sell_Date']=''

# Finding the selling dates of each trade cycle
def Sell_date_est():
    newlist = [x for x in df["Basket_Sell_date"] if x != '']
    newdf['Sell_Date']=newlist
    return None

Sell_date_est()

# Adding the mean median calculated values to the dataframe
newdf[symb1+'_Mean'][0]=mean(x for x in df[symb1+"_VWAP"] if pd.isnull(x) == False)
newdf[symb1+'_Median'][0]=median(x for x in df[symb1+"_VWAP"] if pd.isnull(x) == False)

newdf[symb2+'_Mean'][0]=mean(x for x in df[symb2+"_VWAP"] if pd.isnull(x) == False)
newdf[symb2+'_Median'][0]=median(x for x in df[symb2+"_VWAP"] if pd.isnull(x) == False)

# Finding the trade cycle buy beginning and end dates
def Buy_date_est():
    k=0
    for i in range(0,len(df[symb3+"_Buy_date"])):
        if df[symb3+"_Buy_date"][i]=='':
            newdf['Buy_End_Date'][k]=df[symb3+"_Buy_date"][i-1]
            k=k+1
        else:
            continue
    t=0
    k=0
    for i in range(0,len(df[symb3+"_Buy_date"])):
        if t==0:
            newdf['Buy_Begin_Date'][k]=df[symb3+"_Buy_date"][i]
            k=k+1
            t=1
        elif(df[symb3+"_Buy_date"][i]==''):
            t=0
        else:
            continue
    return None

Buy_date_est()

# Counting the number of scripts traded in each cycle 
newdf[symb1+'_Count']=''
newdf[symb2+'_Count']=''
newdf[symb1+'_Buy_cost']=''
newdf[symb2+'_Buy_cost']=''
newdf['Sell_cost']=''

def Buy_counter():
    s1=0
    s2=0
    sum1=0
    sum2=0
    k=0
    for i in range(0,len(df['Buy_initiate'])):
        if(df['Buy_initiate'][i]==''):
            continue
        elif(float(df['Buy_initiate'][i])<0):
            if(df['S_company'][i]==symb1):
                s1=s1+1
                sum1=float(df['Buy_initiate'][i])+sum1
            else:
                s2=s2+1
                sum2=float(df['Buy_initiate'][i])+sum2
        elif(float(df['Buy_initiate'][i])>0):
            newdf[symb1+'_Count'][k]=s1
            newdf[symb2+'_Count'][k]=s2
            newdf[symb1+'_Buy_cost'][k]=sum1
            newdf[symb2+'_Buy_cost'][k]=sum2
            newdf['Sell_cost'][k]=float(df['Buy_initiate'][i])
            s1=0
            s2=0
            sum1=0
            sum2=0
            k=k+1
        else:
            continue
    return None
            
Buy_counter()
        
# Calculating the profits of each trade cycle
newdf['Profit']=''
newdf['Profit']=newdf['Sell_cost']+newdf[symb1+'_Buy_cost']+newdf[symb2+'_Buy_cost']

newdf['Profit_%']=''
newdf['Profit_%']=100*newdf['Profit']/-(newdf[symb1+"_Buy_cost"]+newdf[symb2+"_Buy_cost"])

# Adding the stats datasheet to google sheets
NwWkSht = symb1+symb2+'_Stats'
wks=gsheet.add_worksheet(rows=617,cols=31,title=NwWkSht)

#df=pd.DataFrame()
#wks = gsheet.get_worksheet(1)
set_with_dataframe(wks, newdf,include_index=True)
        



