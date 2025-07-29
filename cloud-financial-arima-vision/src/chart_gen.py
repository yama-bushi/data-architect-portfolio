import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from google.cloud import storage
from google.cloud import bigquery
import time
import matplotlib.pyplot as plt
import numpy as np
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'your creds.json'
ticker = ""
resolution = ""
PROJECT_ID = ''
client = bigquery.Client(project=PROJECT_ID)
query = "select distinct window_ends,direction from `dset._"f"{ticker}_USD.{resolution}_windows` where direction <> 'SAME' limit 1000"
query_job = client.query(query)
results = query_job.result()
rows =[dict(row) for row in results]
df_i = pd.DataFrame(rows) 
#print(df)
for index,row in df_i.iterrows():
    # Plot
    i = row['window_ends']
    d = row['direction']
    print(i)
    query_2 = f"select date_time,{ticker}_close chart_close,{ticker}_rsi chart_rsi,minima,maxima from `_"f"{ticker}_USD.{resolution}_graph_physical` where window_ends= '"f"{i}' order by date_time"
    query_job2 = client.query(query_2)
    results2 = query_job2.result()
    rows2 =[dict(row) for row in results2]
    df = pd.DataFrame(rows2) 
    fig, ax1 = plt.subplots()
    #plt.figure(figsize=(12, 6))  # Adjusted figsize for better visibility
    ax1.plot(df['date_time'], df['chart_close'], label='Chart Close', color='black')
    ax1.plot(df['date_time'], df['minima'], label='support', color='blue')
    ax1.plot(df['date_time'], df['maxima'], label='resistance', color='green')

    
    # Add labels and title
    #ax1.set_xlabel('Date')
    #ax1.set_ylabel('Measures')
    #ax1.tick_params(axis='y',labelcolor='b')

    #ax2 = ax1.twinx()
    #ax2.plot(df['date_time'], df['chart_rsi'], label='Chart RSI', color='purple')
    #ax2.plot(df['date_time'], df['btc_rsi'], label='BTC RSI', color='orange')
    #ax2.plot(df['date_time'], df['eth_rsi'], label='ETH RSI', color='blue')
    #ax2.set_ylabel('RSI', color='r')
    #ax2.set_ylim(0,2)
    #ax2.tick_params(axis='y', labelcolor='r')
    #ax2.set_ylim(0,200)
                    
    # Show legend
    #plt.legend()
    
    # Rotate date labels for clarity
    #plt.xticks(rotation=45)
    
    # Show plot with tight layout
    #fig.tight_layout()
    #plt.show()
    fig.savefig(f"../../MODEL/{d}/"f"{str(i).replace('-','_').replace(' ','_').replace('+','_').replace(':','_')}_{ticker}_chart.png")
    plt.close(fig)
