import os
import pandas as pd
from datetime import datetime
from statsapi import get



def save_rows(file_path: str, data: pd.DataFrame, id_col: str='id', is_save_only_new_data: bool=True):
    
    # if csv doesn't exist save down what we have then load saved data
    if not os.path.exists(file_path):
        data.to_csv(file_path, index=False)
        # we have just saved all data in an empty csv so can break out of loop
        return
    df_csv = pd.read_csv(file_path, dtype=str)

    # if saving only new data, create a df of data that isn't already in the csv
    if is_save_only_new_data:
        # get a df of only the new events since the last save
        is_new_data = ~data[id_col].isin(df_csv[id_col])  # ~ negates the boolean df
        data_to_save = data[is_new_data]   
    # else we are saving all the data given
    else:
        data_to_save = data
    
    # create the complete list of all events
    all_data = pd.concat([df_csv, data_to_save], ignore_index=True)

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

def player_stat_data_season(personId, group="[hitting,pitching,fielding]", type="season", sportId=1, season='2023'):
    """Altered the statsapi.player_stat_data function to be able to select the season of stats data to download"""
    params = {
        "personId": personId,
        "hydrate": "stats(group="
        + group
        + ",type="
        + type
        + "season="
        + season
        + ",sportId="
        + str(sportId)
        + "),currentTeam",
    }
    r = get("person", params)

    stat_groups = []

    player = {
        "id": r["people"][0]["id"],
        "first_name": r["people"][0]["useName"],
        "last_name": r["people"][0]["lastName"],
        "active": r["people"][0]["active"],
        "current_team": r["people"][0]["currentTeam"]["name"],
        "position": r["people"][0]["primaryPosition"]["abbreviation"],
        "nickname": r["people"][0].get("nickName"),
        "last_played": r["people"][0].get("lastPlayedDate"),
        "mlb_debut": r["people"][0].get("mlbDebutDate"),
        "bat_side": r["people"][0]["batSide"]["description"],
        "pitch_hand": r["people"][0]["pitchHand"]["description"],
    }

    for s in r["people"][0].get("stats", []):
        for i in range(0, len(s["splits"])):
            stat_group = {
                "type": s["type"]["displayName"],
                "group": s["group"]["displayName"],
                "season": s["splits"][i].get("season"),
                "stats": s["splits"][i]["stat"],
            }
            stat_groups.append(stat_group)

    player.update({"stats": stat_groups})

    return player