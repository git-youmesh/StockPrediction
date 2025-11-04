import warnings
import os
import pandas_ta as ta 
warnings.simplefilter(action='ignore', category=FutureWarning)
from scipy.stats import multinomial
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import DataStore as DM
from sklearn.cluster import KMeans
import requests
import numpy as np
requests.packages.urllib3.disable_warnings()
from scipy.stats import mode
import pandas as pd
import warnings
import matplotlib.pyplot as plot 
import datetime as dt
from nltk.tokenize import word_tokenize
import math
import pandas as pd
warnings.filterwarnings("ignore")
#clf = SVC(kernel='linear') 
os.chdir(os.path.dirname(__file__))

def tfidf(word, id): #A
        tf = tokenized_docs[id].count(word) #B
        df = len(index[word]) if word in index else 0 #C
        idf = math.log(len(examples) / (df+1)) #D
        return tf * idf
  
if __name__ =="__main__":

        Ds = DM.DataStoreManager()
        sym = pd.read_csv("./Futures.csv").values[:,0]
        All =[]
        for X in sym:
                df = Ds.GetData(X,1) 
                if len(df) > 0 :
                        df['close'] = df['close'].astype(int)
                        examples = df['close'].values 
                        tokenized_docs = [word_tokenize(str(d)) for d in examples] #C
                        index = {} #D
                        for i, doc in enumerate(tokenized_docs):
                                for word in doc:
                                        if word not in index:
                                                index[word] = []
                                                index[word].append(i)
                        days_14 = df['close'].tail(30).values
                        query_words =  [word_tokenize(str(d)) for d in days_14]
                        scores = {id:0 for id in range(len(examples))} #B
                        for q in query_words:
                                if q[0] in index:
                                        for id in index[q[0]]:
                                                scores[id] += tfidf(q[0], id)
                        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
                        latest = df['close'].tail(1).values[0]
                        All.append([X, examples[ranked[0][0]] ,latest] )
        df = pd.DataFrame(All)
        
        df.columns =['Sym','Predicted','Current']
        df['Diff'] = df['Predicted'] - df['Current']
        df.to_csv("Predicted.csv")
        print("test")
                        
        
