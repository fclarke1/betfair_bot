import pandas as pd
import betfairlightweight
from betfairlightweight import filters
import json
from datetime import datetime

class BetfairClient:
    """Handles login and API requests into readable formats
    """
    def __init__(self, path_creds: str):
        self._path_creds = path_creds

    def login(self) -> betfairlightweight.APIClient:
        with open(self._path_creds) as f:
            cred = json.load(f)
            my_username = cred['username']
            my_password = cred['password']
            my_app_key = cred['app_key']
        trading = betfairlightweight.APIClient(username=my_username,
                                            password=my_password,
                                            app_key=my_app_key)
        trading.login()
        self.trading = trading
    
    
    def logout(self):
        self.trading.logout()
    
    
    def set_my_event_type(self, event_type_name: str):
        event_types = self.get_event_types()
        self.my_event_type_id = event_types.loc[event_types['name'] == event_type_name, 'id'].values[0]
        self.my_event_type_name = event_type_name
        if self.my_event_type_id is None:
            print('\n*****\nERROR: event_type_name not recognised\n*******\n')


    def get_event_types(self) -> pd.DataFrame:    
        # Grab all event type ids. This will return a list which we will iterate over to print out the id and the name of the sport
        event_type_results = self.trading.betting.list_event_types()

        event_types = pd.DataFrame({
            'id': [event_type_object.event_type.id for event_type_object in event_type_results],
            'name': [event_type_object.event_type.name for event_type_object in event_type_results]
        })
        return event_types
    
    
    def get_events(self) -> pd.DataFrame:
        try:
            event_type_id = self.my_event_type_id
        except NameError:
            print('Error: Need to run .set_my_event_type before getting events')
        
        # get data on basketball games 'bb'
        event_filter = filters.market_filter(event_type_ids=[event_type_id])
        event_results = self.trading.betting.list_events(
            filter=event_filter
        )
        
        events = pd.DataFrame({
            'id':   [event_object.event.id for event_object in event_results],
            'name': [event_object.event.name for event_object in event_results],
            'open_date': [event_object.event.open_date for event_object in event_results],
            'event_type_id': [event_type_id for _ in event_results]
        })
        
        return events
    
    
    def get_market_runner_catalogues(self,
                                     event_ids: list,
                                     market_type_codes: list=None,
                                     market_betting_types: list=None):
        
        # NOTE: to find the market_type_codes get the market_ids using this function
        # then use get_market_types() to find the codes
        
        # create filter for markets of given event_ids
        market_catalogue_filter = filters.market_filter(
            event_ids=event_ids,  # filter on given events
            market_betting_types=market_betting_types,
            market_type_codes=market_type_codes  # filter for some markets, eg. leave out handicap
        )

        # Get market catelogue which contains details about different betting options called a market (but no odds)
        market_results = self.trading.betting.list_market_catalogue(
            market_projection=[
                "EVENT", 
                "EVENT_TYPE", 
                "MARKET_START_TIME",
                "RUNNER_DESCRIPTION",
                "MARKET_DESCRIPTION"
            ],
            filter=market_catalogue_filter,
            max_results=100,
        )
        
        # create df of market data
        market_catalogue = pd.DataFrame({
            'id': [market_obj.market_id for market_obj in market_results],
            'market_name': [market_obj.market_name for market_obj in market_results],
            'market_type': [market_obj.description.market_type for market_obj in market_results],
            'market_start_time': [market_obj.market_start_time for market_obj in market_results],
            'event_id': [market_obj.event.id for market_obj in market_results],
            'event_type_id': [market_obj.event_type.id for market_obj in market_results]
        })
        
        # create df for runner data
        runner_data = {
            'id':[],
            'runner_name':[],
            'handicap':[],
            'market_id':[]
        }
        for market_obj in market_results:
            for runner_obj in market_obj.runners:
                runner_data['id'].append(runner_obj.selection_id)
                runner_data['runner_name'].append(runner_obj.runner_name)
                runner_data['handicap'].append(runner_obj.handicap)
                runner_data['market_id'].append(market_obj.market_id)
        runner_catalogue = pd.DataFrame(runner_data)
        
        return market_catalogue, runner_catalogue

    
    def get_market_types(self, market_ids: list) -> list:
        # given a list of markets return the market types (to help with filtering)
        
        # create filter for only given markets
        market_type_filter = filters.market_filter(
            market_ids=market_ids
        )
        
        # get market types
        market_types = self.trading.betting.list_market_types(
            filter=market_type_filter
        )
        
        # return just the list of MarketTypeResult objects
        return market_types

    
    def get_runner_odds(self, market_ids: list):
        
        # get betting odds details from a single market (aka. a single betting option) - this includes odds
        market_book = self.trading.betting.list_market_book(
            market_ids=market_ids,
            price_projection=filters.price_projection(
                price_data=filters.price_data(ex_all_offers=True)
            )
        )

        # create df for runner data
        runner_data = {
            'id':[],
            'status':[],
            'last_price_traded':[],
            'total_matched':[],
            'handicap':[],
            'market_id':[],
            'record_date':[]
        }
        current_time = datetime.now()
        for market_obj in market_book:
            for runner_obj in market_obj.runners:
                runner_data['id'].append(runner_obj.selection_id)
                runner_data['status'].append(runner_obj.status)
                runner_data['last_price_traded'].append(runner_obj.last_price_traded)
                runner_data['total_matched'].append(runner_obj.total_matched)
                runner_data['handicap'].append(runner_obj.handicap)
                runner_data['market_id'].append(market_obj.market_id)
                runner_data['record_date'].append(current_time.strftime("%Y-%m-%d %H:%M"))
        runner_prices = pd.DataFrame(runner_data)
        
        return runner_prices