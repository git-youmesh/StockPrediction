import re
import mysql.connector
from mysql.connector import errorcode
import sys 
import json
import pandas as pd
import datetime as dt
import requests
import arrow
import numpy as np
import scipy as sp
from sklearn.preprocessing import MinMaxScaler
import yfinance as yt
import matplotlib.pyplot as plt 
 
import math 
class DataStoreManager:
    def __init__(self):
            """config = dotenv_values(".env")
            self.Envt = "Prod"
            f = open('./config.json',)
            # returns JSON object as 
            # a dictionary
            data = json.load(f)
            # Iterating through the json
            # list
            i = data['producton']
            self.smtp_server= i['smtp_server']
            self.Smpt_Port= i['port']
            self.prisender_email= i['sender_email']
            self.Smpt_server_pwd = i['smtppassword']
            self.DB_username = i["db_user"]
            self.DB_password = i["db_password"]
            self.host =i["host"]
            self.Smpt_Server_user_name= i["sender_email"]
            f.close()"""
    
    def GetDBConfig(self):
                try:
                    if ("Prod") == "Prod" :
                        #print("Connecting to production")
                        Db_config = {
                                'user': "root", #self.DB_username,
                                'password': "Password001#", #self.DB_password,
                                'host': "localhost" , # "self.host,
                                'database': 'Maya',
                                'raise_on_warnings': True
                        }
                    else:
                       # print("Connecting to UAT")
                        Db_config = {
                        'user':'dbusrerguat',
                        'password': '435234d1dbb89273ebbaad9005d3837a',
                        'host': 'ergos-uat.caaquijxdja0.ap-south-1.rds.amazonaws.com',
                        'database': 'ergosuat',
                        'raise_on_warnings': True
                        }       
                   
                    #Establish DB connection 
                    Conn = self.connection(Db_config)
                    # Closing fil
                    return Conn
                except Exception as err : 

                    raise err
    def connection(self,ConnectionStr):
                try :
                    #Create connection db context 
                    
                    cnx = mysql.connector.connect(**ConnectionStr )
                    
                    return cnx
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                      print("Something is wrong with your user name or password")
                    elif err.errno == errorcode.ER_BAD_DB_ERROR:
                        print("Database does not exist")
                    else:
                                                raise(err)
    def ExecuteSelect(self,SQL):
              
                Conn = self.GetDBConfig()
                mycursor = Conn.cursor()
                try:
                # Execute the SQL command
                    
                    mycursor.execute(SQL)
                     
                    # Fetch all the rows in a li
                    # st of lists.
                    return( mycursor.fetchall())
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                      print("Something is wrong with your user name or password")
                    elif err.errno == errorcode.ER_BAD_DB_ERROR:
                        print("Database does not exist")
                    else:
                        print("Something went wrong: {}".format(err))
    def ExecuteSelectDF(self,SQL):
                df = pd.DataFrame()
                Conn = self.GetDBConfig()
                mycursor = Conn.cursor()
                try:
                # Execute the SQL command
                    
                    mycursor.execute(SQL)
                    
                    row = mycursor.fetchall()
                    
                    df = pd.DataFrame.from_records(row, columns=[x[0] for x in mycursor.description])
                    
                    return (df)
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                      print("Something is wrong with your user name or password")
                    elif err.errno == errorcode.ER_BAD_DB_ERROR:
                        print("Database does not exist")
                    else:
                        print("Something went wrong: {}".format(err))
                         
                    return df
    
    def ExecuteSQL(self,SQL):
        try:
                Conn = self.GetDBConfig()
                cursor = Conn.cursor()
                cursor.execute(SQL)
                Conn.commit()
                cursor.close()

        except mysql.connector.Error as error:
            print("Failed to insert record into Laptop table {}".format(error))
        finally:
            if Conn.is_connected():
                Conn.close()

                   # print("MySQL connection is closed")
    def UpdateyAnalyticDataForDate(self,tablename,dic ):
            try:
                SQL= columns= values =''
                Conn = self.GetDBConfig()
                cursor = Conn.cursor()
                Seperator = ''
                SQL = " Insert INTO " + str(tablename) + " ( "
                for key in dic:
                    columns +=  Seperator +  str(key) + " " 
                    values  +=  Seperator  + "'"  + str(dic[key]) + "'" 
                    Seperator = ','
                SQL += columns + ") "  + "Values ( " +  values + " ) "
                 
                cursor.execute(SQL)
                Conn.commit()
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            finally:
                if Conn.is_connected():
                    Conn.close()
    def InsetNewRow(self,tablename,dic ):
            try:
                SQL= columns= values =''
                Conn = self.GetDBConfig()
                cursor = Conn.cursor()
                Seperator = ''
                SQL = " Insert INTO " + str(tablename) + " ( "
                for key in dic:
                    columns +=  Seperator +  str(key) + " " 
                    values  +=  Seperator  + "'"  + str(dic[key]) + "'" 
                    Seperator = ','
                SQL += columns + ") "  + "Values ( " +  values + " ) "
                 
                cursor.execute(SQL)
                Conn.commit()
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            finally:
                if Conn.is_connected():
                    Conn.close()
    def GetData(self,x,type=0,Sq=1):
                 


                if type ==1 :
                    
                    preStr ="chartPreviousClose"
                    if x == '^NSEI':
                        url = 'http://query1.finance.yahoo.com/v8/finance/chart/^NSEI?range=1y&interval=1d'
                    else:
                        url = "https://query1.finance.yahoo.com/v8/finance/chart/" + x + ".NS?range=2y&interval=1d"
                else : 
                    if x == '^NSEI':
                        url = "http://query1.finance.yahoo.com/v8/finance/chart/^NSEI?range=1d&interval="+ str(Sq) + "m"
                    else :
                         url = "https://query1.finance.yahoo.com/v8/finance/chart/" + x + ".NS?range=1d&interval=" + str(Sq) + "m"
                    preStr ="chartPreviousClose"
                df = pd.DataFrame() 
                myheaders = {"User-Agent" : "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2)"}
               
                try : 
                        res = requests.get(url,headers=myheaders)
                        data = res.json()
                        body = data['chart']['result'][0]
                        Quote = body['indicators']['quote']
                        if res.status_code == 200  :
                                            if (data['chart']['result'] != None):
                                                #body = data['chart']['result'][0]
                                                #Quote = str(body['indicators']['quote'])
                                                preclose = data['chart']['result'][0]['meta'][preStr]
                                                dt_1 = pd.Series(
                                                    map(lambda x: arrow.get(x).to('Asia/Calcutta').datetime.replace(tzinfo=None),
                                                        body['timestamp']), name='Datetime')
                                                #dt_1 = pd.to_datetime(dt_1).dt.date
                                                df = pd.DataFrame(body['indicators']['quote'][0], index=dt_1)
                                                df.replace(["NaN", 'NaT'], np.nan, inplace=True)
                                                df = df.dropna()
                                                df['date'] = df.index.date
                                                

                except Exception as error:
                        i =0 
                        i + 1 
                        #print(Exception)  
                if len(df) > 0  :
                    df['Close'] = df['close']  
                    if type == 0 :
                        df['Change'] = (( df['close']-preclose)/preclose) * 100
                    else : 
                        df['Change'] = df['close'].pct_change() * 100
                    mms = MinMaxScaler()
                    df['pre']=  preclose
                    df['Scale'] = mms.fit_transform(df[['close']])  
                    df = df.fillna(0)
        
                return df
    def GetSymbol(self,context):
        Sym ='NA'
        URL = "https://www.google.com/search?q=XXX+nse+symbol".replace('XXX',context)
        myheaders = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        res = requests.get(URL,headers=myheaders)
        strcontent = str(res.content)
        index = strcontent.find("NSE:")
        
        if index == -1 : 
               index = strcontent.find("NSE :")
        index1 = strcontent.find("</",index)
        txt = strcontent[index : index1]
        if len(txt) > 0 : 
            Sym = str(res.content)[index: index+ index1].split(":")[1].strip().split(" ")[0]
        print(Sym)
        if Sym == 'NA':
          print(URL) 
        return Sym
    def UpdateLR(self,Sym,LR):
        try:
            SQL =""
            SQL = "Update nse set LearningRate= 'YYY' where Symbol ='XXX'".replace('XXX',Sym).replace('YYY','.0001')
            Conn = self.GetDBConfig()
            cursor = Conn.cursor()
            cursor.execute(SQL)
            Conn.commit()
            cursor.close()
        except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
        finally:
                if Conn.is_connected():
                    Conn.close()
    def GetUSData(self,symbol='AMZN'):

                preStr ="chartPreviousClose"
                url = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol + "?range=1y&interval=1d"
                preStr ="chartPreviousClose"
                df = pd.DataFrame() 
                myheaders = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                try : 
                        res = requests.get(url,headers=myheaders)
                        data = res.json()
                        body = data['chart']['result'][0]
                        Quote = body['indicators']['quote']
                        if res.status_code == 200  :
                                            if (data['chart']['result'] != None):
                                                #body = data['chart']['result'][0]
                                                #Quote = str(body['indicators']['quote'])
                                                preclose = data['chart']['result'][0]['meta'][preStr]
                                                dt_1 = pd.Series(
                                                    map(lambda x: arrow.get(x).to('Asia/Calcutta').datetime.replace(tzinfo=None),
                                                        body['timestamp']), name='Datetime')
                                                #dt_1 = pd.to_datetime(dt_1).dt.date
                                                df = pd.DataFrame(body['indicators']['quote'][0], index=dt_1)
                                                df.replace(["NaN", 'NaT'], np.nan, inplace=True)
                                                df = df.dropna()
                                                df['T'] = df.index.date
                                                

                except Exception as error:
                    print("Test")
                        #print(Exception)  
                if len(df) > 0  :
                    
                    if type == 0 :
                        df['Change'] = (( df['close']-preclose)/preclose) * 100
                    else : 
                        df['Change'] = df['close'].pct_change() * 100
                    mms = MinMaxScaler()
                    df['pre']=  preclose
                    df['Scale'] = mms.fit_transform(df[['close']])  
                    df['volume'] = mms.fit_transform(df[['volume']]) 
                    df['high'] = mms.fit_transform(df[['high']])  
                    df['low'] = mms.fit_transform(df[['low']]) 
                    #df['Change'] = mms.fit_transform(df[['Change']])  
                    df =df[['Scale','close','volume','high','low','open','Change']]
                    df['Scale'] = mms.fit_transform(df[['close']])  
                    df['volume'] = mms.fit_transform(df[['volume']])  
                    df =df[['Scale','close','volume','high','low','open','Change']]
                    df.rename(columns = {'Close':'close', 'Volume':'volume','High':'high','Low':'low','Open':'open','Change' : 'change'}, inplace = True)
                    df = df.dropna()
                    return df
    def GetDataYahoo(self,Sym):
        if Sym != '^NSEI':
              Sym = Sym +'.NS'
        GetFacebookInformation = yt.Ticker(Sym )
        df = GetFacebookInformation.history(period="1Y")
        if len(df) > 0  : 
                   df.rename(columns={'Open': 'open','Close': 'close', 'High': 'high', 'Low': 'low','Volume': 'volume'}, inplace=True)
                   
        return df
    def FindResistance(self,bars):
                            strong_peak_distance = 60

                            # Define the prominence (how high the peaks are compared to their surroundings).
                            strong_peak_prominence = 20

                            # Find the strong peaks in the 'high' price data
                            strong_peaks, _ = sp.signal.find_peaks(
                            bars['high'],
                            distance=strong_peak_distance,
                            prominence=strong_peak_prominence
                            )

                            # Extract the corresponding high values of the strong peaks
                            strong_peaks_values = bars.iloc[strong_peaks]["high"].values.tolist()

                            # Include the yearly high as an additional strong peak
                            yearly_high = bars["high"].iloc[-252:].max()
                            strong_peaks_values.append(yearly_high)
                            # Create a list of horizontal lines to plot as resistance levels
                            
                            # Define the shorter distance between general peaks (in days)
                            # This controls how far apart peaks need to be to be considered separate.
                            peak_distance = 5

                            # Define the width (vertical distance) where peaks within this range will be grouped together.
                            # If the high prices of two peaks are closer than this value, they will be merged into a single resistance level.
                            peak_rank_width = 2

                            # Define the threshold for how many times the stock has to reject a level
                            # Before it becomes a resistance level
                            resistance_min_pivot_rank = 3

                            # Find general peaks in the stock's 'high' prices based on the defined distance between them.
                            # The peaks variable will store the indices of the high points in the 'high' price data.
                            peaks, _ = sp.signal.find_peaks(bars['high'], distance=peak_distance)

                            # Initialize a dictionary to track the rank of each peak
                            peak_to_rank = {peak: 0 for peak in peaks}

                            # Loop through all general peaks to compare their proximity and rank them
                            for i, current_peak in enumerate(peaks):
                                # Get the current peak's high price
                                current_high = bars.iloc[current_peak]["high"]
                                
                                # Compare the current peak with previous peaks to calculate rank based on proximity
                                for previous_peak in peaks[:i]:
                                    if abs(current_high - bars.iloc[previous_peak]["high"]) <= peak_rank_width:
                                        # Increase rank if the current peak is close to a previous peak
                                        peak_to_rank[current_peak] += 1
                                        print(strong_peaks_values)
                            # Initialize the list of resistance levels with the strong peaks already identified.
                            resistances = strong_peaks_values

                            # Now, go through each general peak and add it to the resistance list if its rank meets the minimum threshold.
                            for peak, rank in peak_to_rank.items():
                                # If the peak's rank is greater than or equal to the resistance_min_pivot_rank, 
                                # it means this peak level has been rejected enough times to be considered a resistance level.
                                if rank >= resistance_min_pivot_rank:
                                    # Append the peak's high price to the resistances list, adding a small offset (1e-3) 
                                    # to avoid floating-point precision issues during the comparison.
                                    resistances.append(bars.iloc[peak]["high"] + 1e-3)

# Sort the list of resistance levels so that they are in ascending order.
                                resistances.sort()
                            resistance_bins = []

                            # Start the first bin with the first resistance level.
                            current_bin = [resistances[0]]

                            # Loop through the sorted resistance levels.
                            for r in resistances:
                                # If the difference between the current resistance level and the last one in the current bin 
                                # is smaller than a certain threshold (defined by peak_rank_w_pct), add it to the current bin.
                                if r - current_bin[-1] < peak_rank_width:
                                    current_bin.append(r)
                                else:
                                    # If the current resistance level is far enough from the last one, close the current bin
                                    # and start a new one.
                                    resistance_bins.append(current_bin)
                                    current_bin = [r]

                            # Append the last bin.
                            resistance_bins.append(current_bin)

                            # For each bin, calculate the average of the resistances within that bin.
                            # This will produce a clean list of resistance levels where nearby peaks have been merged.
                            resistances = [np.mean(bin) for bin in resistance_bins]
                            return resistances
    def FindSupport(self,bars):
                            strong_peak_distance = 60

                            # Define the prominence (how high the peaks are compared to their surroundings).
                            strong_peak_prominence = 20

                            # Find the strong peaks in the 'high' price data
                            strong_peaks, _ = sp.signal.find_peaks(
                            -bars['low'],
                            distance=strong_peak_distance,
                            prominence=strong_peak_prominence
                            )

                            # Extract the corresponding high values of the strong peaks
                            strong_peaks_values = bars.iloc[strong_peaks]["high"].values.tolist()

                            # Include the yearly high as an additional strong peak
                            yearly_high = bars["high"].iloc[-252:].max()
                            strong_peaks_values.append(yearly_high)
                            # Create a list of horizontal lines to plot as resistance levels
                            
                            # Define the shorter distance between general peaks (in days)
                            # This controls how far apart peaks need to be to be considered separate.
                            peak_distance = 5

                            # Define the width (vertical distance) where peaks within this range will be grouped together.
                            # If the high prices of two peaks are closer than this value, they will be merged into a single resistance level.
                            peak_rank_width = 2

                            # Define the threshold for how many times the stock has to reject a level
                            # Before it becomes a resistance level
                            resistance_min_pivot_rank = 3

                            # Find general peaks in the stock's 'high' prices based on the defined distance between them.
                            # The peaks variable will store the indices of the high points in the 'high' price data.
                            peaks, _ = sp.signal.find_peaks(bars['high'], distance=peak_distance)

                            # Initialize a dictionary to track the rank of each peak
                            peak_to_rank = {peak: 0 for peak in peaks}

                            # Loop through all general peaks to compare their proximity and rank them
                            for i, current_peak in enumerate(peaks):
                                # Get the current peak's high price
                                current_high = bars.iloc[current_peak]["high"]
                                
                                # Compare the current peak with previous peaks to calculate rank based on proximity
                                for previous_peak in peaks[:i]:
                                    if abs(current_high - bars.iloc[previous_peak]["high"]) <= peak_rank_width:
                                        # Increase rank if the current peak is close to a previous peak
                                        peak_to_rank[current_peak] += 1
                                        print(strong_peaks_values)
                            # Initialize the list of resistance levels with the strong peaks already identified.
                            resistances = strong_peaks_values

                            # Now, go through each general peak and add it to the resistance list if its rank meets the minimum threshold.
                            for peak, rank in peak_to_rank.items():
                                # If the peak's rank is greater than or equal to the resistance_min_pivot_rank, 
                                # it means this peak level has been rejected enough times to be considered a resistance level.
                                if rank >= resistance_min_pivot_rank:
                                    # Append the peak's high price to the resistances list, adding a small offset (1e-3) 
                                    # to avoid floating-point precision issues during the comparison.
                                    resistances.append(bars.iloc[peak]["high"] + 1e-3)

# Sort the list of resistance levels so that they are in ascending order.
                                resistances.sort()
                            resistance_bins = []

                            # Start the first bin with the first resistance level.
                            current_bin = [resistances[0]]

                            # Loop through the sorted resistance levels.
                            for r in resistances:
                                # If the difference between the current resistance level and the last one in the current bin 
                                # is smaller than a certain threshold (defined by peak_rank_w_pct), add it to the current bin.
                                if r - current_bin[-1] < peak_rank_width:
                                    current_bin.append(r)
                                else:
                                    # If the current resistance level is far enough from the last one, close the current bin
                                    # and start a new one.
                                    resistance_bins.append(current_bin)
                                    current_bin = [r]

                            # Append the last bin.
                            resistance_bins.append(current_bin)

                            # For each bin, calculate the average of the resistances within that bin.
                            # This will produce a clean list of resistance levels where nearby peaks have been merged.
                            resistances = [np.mean(bin) for bin in resistance_bins]

                            return resistances
    def get_stochastic_oscillator(self,df, period=14):
            for i in range(len(df)):
                low = df.iloc[i]['close']
                high = df.iloc[i]['close']
                if i >= period:
                    n = 0
                    while n < period:
                        if df.iloc[i-n]['close'] >= high:
                            high = df.iloc[i-n]['close']
                        elif df.iloc[i-n]['close'] < low:
                            low = df.iloc[i-n]['close']
                        n += 1
                    df.at[i, 'best_low'] = low
                    df.at[i, 'best_high'] = high
                    df.at[i, 'fast_k'] = 100*((df.iloc[i]['close']-df.iloc[i]['best_low'])/(df.iloc[i]['best_high']-df.iloc[i]['best_low']))

            df['fast_d'] = df['fast_k'].rolling(3).mean().round(2)
            df['slow_k'] = df['fast_d']
            df['slow_d'] = df['slow_k'].rolling(3).mean().round(2)

            return df
    def Chart(self,ticker, df,Info,path):
           

            plt.figure(figsize=[16, 8])
            plt.style.use('default')
            fig, ax = plt.subplots(1)
            fig.suptitle(Info)
            plt.subplots_adjust(hspace=0.02)
            ax[0].grid(True)
            ax[0].axes.get_xaxis().set_visible(False)  # Remove X labels
            ax[0].set_ylabel(r'Price [\$]')
            ax[0].plot(df['close'], color='black', linewidth=1)
            ax[0].plot(df['Garvi'], color='green', linewidth=1)
        
            ax[1].plot(df['T'], df['Slow'], color='orange', linewidth=1)
            ax[1].plot(df['T'], df['Fast'], color='red', linewidth=1)
            ax[1].grid(True)
            ax[2].plot(df['T'], df['macd'], color='orange', linewidth=1)
            ax[2].plot(df['T'], df['macd_s'], color='red', linewidth=1)
            ax[2].grid(True)
            
            ax[2].set_ylim(-40, 40)
            #plt.xticks(rotation=30, ha='right')
            plt.savefig(path)
          
            plt.close()
            
if __name__ == "__main__":
      Ds = DataStoreManager()
      df= Ds.GetData("INFY",1)
      print(df)