import argparse
from bf_utils import client, utils
import pandas as pd
import time


def record_odds(bf_client: client.BetfairClient, my_event_type: str, data_dir: str):
    # record all odds available for event_type given, for runners that haven't started yet
    # append data to relevant csvs 
    
    # create dict of csv locations
    file_paths = {
        'events': data_dir+'/events.csv',
        'marketCatalogue': data_dir+'/marketCatalogue.csv',
        'runnerCatalogue': data_dir+'/runnerCatalogue.csv',
        'runnerPrice': data_dir+'/runnerPrice.csv'
    }
    
    # get baseball's event id
    bf_client.set_my_event_type('Baseball')

    # get baseball events
    events = bf_client.get_events()

    # get market and runner data
    market_catalogue, runner_catalogue = bf_client.get_market_runner_catalogues(
        event_ids=events['id'].tolist(),
        market_type_codes=['COMBINED_TOTAL', 'MATCH_ODDS'],
        market_betting_types=['ODDS']
    )

    # get market_ids that haven't started yet
    market_ids = utils.get_non_started_market_ids(market_catalogue=market_catalogue)

    # get the odds for each runner
    runner_prices = bf_client.get_runner_odds(market_ids=market_ids)

    # save the new data since last refresh
    utils.save_rows(file_path=file_paths['events'], data=events)
    utils.save_rows(file_path=file_paths['marketCatalogue'], data=market_catalogue)
    utils.save_rows(file_path=file_paths['runnerCatalogue'], data=runner_catalogue, id_col='market_id')
    utils.save_rows(file_path=file_paths['runnerPrice'], data=runner_prices, is_save_only_new_data=False)


def main(args):
    # print summary to terminal:
    print(f'\nRecording odds from all games of {args.event_type}')
    if args.is_run_once == 'False':
        print(f'Completed every {args.refresh_rate} hours\n')    
    
    # create client to interact with Betfair
    bf_client = client.BetfairClient(path_creds=args.creds_dir+'/credentials.json')
    
    # every refresh_rate hours -> login, get data, then logout
    sleep_time = float(args.refresh_rate) * 60 * 60  # sleep time in seconds
    while True:
        # record data
        bf_client.login()
        record_odds(bf_client=bf_client, my_event_type=args.event_type, data_dir=args.data_dir)
        bf_client.logout()
        print('data recorded')
        
        # if loop only needs to run once then break
        if args.is_run_once == 'True':
            break
        
        # sleep until another refresh is due
        print('sleeping...safe to exit')  # print when sleeping so user can cancel program when not saving data
        time.sleep(sleep_time)
        print('awake...do not exit...', end='')
        time.sleep(3)  # gives a buffer to exit safely even after waking
    
    print('Program exit')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Record the odds regularly for every event for the given eventType')
    
    parser.add_argument('--data_dir', default='data', help='Path to csv folder, default=data')
    parser.add_argument('--is_run_once', default='True', help='Bool to set if program runs once or continuously, default=True')
    parser.add_argument('--refresh_rate', default='4', help='Frequency of data refresh in hours, default=4')
    parser.add_argument('--event_type', default='Baseball', help='Event to be tracked, default=Baseball')
    parser.add_argument('--creds_dir', default='certs', help='Path to folder containing credentials.json, default=certs')
    
    args = parser.parse_args()
    main(args)