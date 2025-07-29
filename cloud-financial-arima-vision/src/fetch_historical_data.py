import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from google.cloud import storage
from google.cloud import bigquery
import time
#C:/Users/jmarc/Documents/gcloud/
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'your creds.json'


def calculate_rsi(df, period=14):
    delta = df['close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    # Calculate StochRSI
    min_rsi = rsi.rolling(window=period).min()
    max_rsi = rsi.rolling(window=period).max()
    stoch_rsi = (rsi - min_rsi) / (max_rsi - min_rsi)
    df['RSI'] = stoch_rsi*100
    return df

def calculate_bollinger_bands(df, period=20, num_of_std=2):
    rolling_mean = df['close'].rolling(window=period).mean()
    rolling_std = df['close'].rolling(window=period).std()
    
    df['upper_band'] = rolling_mean + (rolling_std * num_of_std)
    df['lower_band'] = rolling_mean - (rolling_std * num_of_std)
    return df


def fetch_historical_data(symbol, resolution):
    symbol_integ = symbol.replace('.','_').replace('/','_').upper()

    start_time = datetime.now(timezone.utc)
    end_time = datetime.now(timezone.utc)
    PROJECT_ID = 'your project'
    try:
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        #client.set_project(PROJECT_ID)
        #if resolution == 'D' or resolution == 'W':
        #    query = "SELECT max(DATETIME_ADD(datetime, INTERVAL 1 DAY) ) dt FROM `"f"{PROJECT_ID}."f"{symbol_integ}."f"{resolution}`"
        #else:
        query = "SELECT max(DATETIME_ADD(cast(datetime as datetime), INTERVAL 1 MINUTE)) dt, max(DATETIME_ADD(current_datetime(), INTERVAL -1 DAY) ) end_date,DATETIME_ADD(DATE_TRUNC(max(current_datetime()), WEEK),INTERVAL 1 DAY) week_end,DATETIME_SUB(current_datetime(),INTERVAL 60 MINUTE) minute_end ,DATETIME_SUB(current_datetime(),INTERVAL 120 MINUTE) hour_end ,DATETIME_SUB(current_datetime(),INTERVAL 480 MINUTE) fhour_end ,DATETIME_SUB(current_datetime(),INTERVAL 1440 MINUTE) thour_end FROM `"f"{PROJECT_ID}."f"{symbol_integ}."f"{resolution}`"
        #print(query)
        query_job = bigquery_client.query(query)
        results = query_job.result()
        rows =[dict(row) for row in results]
        start_time = rows[0]['dt']
        
        if resolution == 'D':
            end_time = rows[0]['end_date']
        elif resolution == 'W':
            end_time = rows[0]['week_end']
        elif resolution == '30':
            end_time = rows[0]['minute_end']
        elif resolution == '60':
            end_time = rows[0]['hour_end']
        elif resolution == '240':
            end_time = rows[0]['fhour_end']
        elif resolution == '720':
            end_time = rows[0]['thour_end']
        else:
            end_time = rows[0]['dt']
    except:
        if resolution == 'D' or resolution == 'W':
            start_time = datetime(2025,1,1,0,0,0,0)
            end_time = datetime.now(timezone.utc) - timedelta(hours=168)
        else:
            start_time = datetime(2025,5,1,0,0,0,0)
            end_time = datetime.now(timezone.utc) - timedelta(hours=13)
    #if resolution == 'D':
    #    resolution_hours = 24
    #    end_time = rows[0]['end_date']
    #elif resolution == 'W':
    #    resolution_hours = 168
    #    end_time = rows[0]['week_end']
    #else:
    #    resolution_hours = int(resolution) / 60  # Convert minutes to hours


    #if start_time > datetime.now(timezone.utc) - timedelta(hours=resolution_hours):
    #    print("Data is up to date. No need to refetch.")
    #else:
    print(f"Start time is {start_time}.")
    print(f"End time is {end_time}.")
    start_time=start_time.replace(tzinfo=timezone.utc)
    end_time=end_time.replace(tzinfo=timezone.utc)
    from_timestamp = int(start_time.timestamp())
    to_timestamp = int(end_time.timestamp())
    csv_file = f"{symbol_integ}/"f"{resolution}/"f"{str(end_time)[0:19].replace('/','_').replace(':','_').replace(' ','_').replace('-','_')}_"f"{symbol.replace('/', '_')}"f"{from_timestamp}_historical_data.csv"
    url = "https://benchmarks.pyth.network/v1/shims/tradingview/history"
    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": from_timestamp,
        "to": to_timestamp
    }
    
    headers = {'accept': 'application/json'}
    response = requests.get(url, params=params, headers=headers)
    #print(response.headers)
    
    if response.status_code == 200:
        data = response.json()
        if data['s'] == 'ok' and data['t']:
            df = pd.DataFrame({
                'datetime': pd.to_datetime(data['t'], unit='s'),
                'open': data['o'],
                'high': data['h'],
                'low': data['l'],
                'close': data['c'],
                'volume': data['v']
            })
            #calculate_rsi(df)
            #calculate_bollinger_bands(df)
            df.to_csv(csv_file, index=False)
            print(f"Data saved to {csv_file}")
            # Initialize a client
            storage_client = storage.Client(project=PROJECT_ID)
            
            # Specify the bucket name and file path
            bucket_name = 'bucket'
            file_path = csv_file
            blob_file = f"{symbol_integ}/"f"{resolution}/DATA/"f"{str(end_time)[0:19].replace('/','_').replace(':','_').replace(' ','_').replace('-','_')}_"f"{symbol.replace('/', '_')}"f"{to_timestamp}_historical_data.csv"
            destination_blob_name = blob_file
            
            # Get the bucket
            bucket = storage_client.get_bucket(bucket_name)
            
            # Create a blob and upload the file's content
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(file_path, content_type="text/csv")
            
            print(f'Successfully uploaded {file_path} to {bucket_name}/{destination_blob_name}')
            
            os.remove(csv_file)
            print('Local File Removed.')

            #Now we create the bigquery table sources from said file
            #query = "DROP TABLE IF EXISTS `"f"{PROJECT_ID}."f"{symbol_integ}."f"{resolution}`"
            #print(query)
            #query_job = bigquery_client.query(query)
            #results = query_job.result()
            #print(results)
            uri = f"{symbol_integ}/"f"{resolution}/DATA/*.csv"
            dataset_name = symbol_integ
            table_name = resolution
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE"
            #allow_quoted_newlines=True,
                #schema=[
                #    bigquery.SchemaField('datetime', 'TIMESTAMP'),
                #    bigquery.SchemaField('open', 'float'),
                #    bigquery.SchemaField('high', 'float'),
                #    bigquery.SchemaField('low', 'float'),
                #    bigquery.SchemaField('close', 'float'),
                #    bigquery.SchemaField('volume', 'float')
                #        ]   
            )
            load_job = bigquery_client.load_table_from_uri(
                uri, f"{dataset_name}.{table_name}", job_config=job_config
            )

        # 
            #result_loader = load_job.result()
            #print(result_loader)
            time.sleep(10)
            #return df

        else:
            print("Data fetch was successful but returned an empty dataset.")
    else:
        print(f"Failed to fetch data: {response.status_code}, Response: {response.text}")
        #return pd.DataFrame()
