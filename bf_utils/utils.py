import os
import pandas as pd
import csv
from datetime import datetime



def save_new_rows(file_path: str, data: pd.DataFrame):
    
    # if csv doesn't exist save down what we have then load saved data
    if not os.path.exists(file_path):
        data.to_csv(file_path, index=False)
    df_csv = pd.read_csv(file_path, dtype=str)

    # get a df of only the new events since the last save
    is_new_data = ~data['id'].isin(df_csv['id'])  # ~ negates the boolean df
    new_data = data[is_new_data]

    # create the complete list of all events
    all_data = pd.concat([df_csv, new_data], ignore_index=True)

    # save the new events
    all_data.to_csv(file_path, index=False)


def get_non_started_market_ids(market_catalogue: pd.DataFrame) -> list[str]:
    # given a market_catalogue containing the market_start_time, return a list of 
    # market_ids where the market hasn't started yet
    
    # a Bool list to filter on rows we want to keep
    is_keep_rows = []
    current_time = datetime.now()
    
    # check each market individually
    for n in market_catalogue.index:
        row = market_catalogue.iloc[n]
        start_time = datetime.strptime(str(row.market_start_time), '%Y-%m-%d %H:%M:%S')
        
        # if start_time is in the future then we check the prices of that market
        is_keep_rows.append(start_time > current_time)
    
    # filter the df and convert to a list
    market_ids = market_catalogue[is_keep_rows].id.tolist()
    return market_ids