import statsapi
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import argparse
from bf_utils import utils




def get_game_pitcher_info(start_date: str, end_date: str, pitcher_season: str):
    """
    Given dates, download all historical game information during that time, and get info on pitchers played
    """
    
    print('downloading game data...')
    # get game info
    # TODO should this select what tournament data is downloaded from?
    stats_games = pd.DataFrame(statsapi.schedule(date=None, start_date=start_date, end_date=end_date, team="", opponent="", sportId=1, game_id=None))
    
    print('downloading pitcher data...')
    pitchers = pd.concat([stats_games.home_probable_pitcher, stats_games.away_probable_pitcher], ignore_index=True).unique()
    
    # iterate through every pitcher in historical games, and search for their stats
    pitchers = pitchers[pd.notna(pitchers)]  # remove NaN values
    stats_pitchers = []
    skipped = []
    for pitcher_name in tqdm(pitchers):
        pitcher_id = None
    # TODO figure out how to download pitchers stats from previous season
        pitcher_infos = statsapi.lookup_player(pitcher_name)  # gets a list of pitchers matching that name, could be none
        if len(pitcher_infos) == 0:
            continue  # if didn't find a pitcher then skip to next pitcher
        elif len(pitcher_infos) == 1: # if there is only one pitcher match use it
            pitcher_id = pitcher_infos[0]['id']
        else:
            for pitcher in pitcher_infos:
                if pitcher['primaryPosition']['abbreviation'] == 'P':  # only take the pitcher that is primarily a pitcher
                    pitcher_id = pitcher['id']
                    break  # if found pitcher then break from for loop

        if pitcher_id is None:  # if we didn't find a pitcher skip to the next one
            skipped.append(pitcher_name)
        else:  # if we did find pitcher then get stats for pitcher
            stats = utils.player_stat_data_season(pitcher_id, group="[pitching]", type="season", sportId=1, season=pitcher_season)

            try:
                stats_pitchers.append({
                    'pitcher_id': pitcher_id,
                    'pitcher_name': pitcher_name,
                    'current_team': stats['current_team'],
                    'position': stats['position'],
                    'pitch_hand': stats['pitch_hand'],
                    'games_started': (stats['stats'][0]['stats']['gamesStarted'] if len(stats['stats']) > 0 else -1)        
                })
            except e:
                print(f'on player {pitcher_name}, id: {pitcher_id} had error: {e}')
    
    stats_pitchers = pd.DataFrame(stats_pitchers)
    print(f'skipped {len(skipped)}\n{skipped}')
    
    return stats_games, stats_pitchers


def main(args):
    # get data
    stats_games, stats_pitchers = get_game_pitcher_info(args.start_date, args.end_date, args.pitcher_season)
    
    # save data
    data_dir = Path(args.data_dir)
    stats_games.to_csv(data_dir / 'stats_games.csv', index=False)
    stats_pitchers.to_csv(data_dir / 'stats_pitchers.csv', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download historic game and pitcher info between given dates')
    
    parser.add_argument('--data_dir', default='data', help='Path to csv folder, default=data')
    parser.add_argument('--start_date', default='02/01/2023', help='mm/dd/yyy - date to download game info from, default=02/01/2023')
    parser.add_argument('--end_date', default='11/30/2023',help='mm/dd/yyy - date to download game info upto, default=11/31/2023')
    parser.add_argument('--pitcher_season', default='2023',help='select what season to download stat data for pitchers, default=2023')
    
    args = parser.parse_args()
    main(args)