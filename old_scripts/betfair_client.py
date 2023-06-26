import json
import datetime
import urllib
import urllib.request
import urllib.error
import requests
import pandas as pd



class BetfairClient:
    def __init__(self, paths):
        self.path_creds = paths[0]
        self.path_cert = paths[1]
        self.path_key = paths[2]
        self.path_data = paths[3]
        
        self.app_key = self.get_app_key()  # app_key needed for all requests
        self.SSOID = self.get_SSOID()  # require SSOID for all requests
        self.headers = self.get_common_headers()  # used for all requests
        self.bet_url = "https://api.betfair.com/exchange/betting/json-rpc/v1"
        
        self.event_ids = self.get_event_ids()
    
    
    def get_app_key(self):
        # app_key is stored in creds.json
        # follow this guide to get it:
        # https://towardsdatascience.com/an-introduction-to-betfair-api-and-how-to-use-it-e3cdbd79c94b
        with open(self.path_creds) as f:
            cred = json.load(f)
            return cred['app_key']
    
    
    def get_SSOID(self):
        # SSOID is unique to each logon session
        with open(self.path_creds) as f:
            cred = json.load(f)
            my_username = cred['username']
            my_password = cred['password']

        # a different header to all other requests is used to get the SSOID
        headers = {'X-Application': self.app_key, 'Content-Type': 'application/x-www-form-urlencoded'}
        payload = 'username=' + my_username + '&password=' + my_password
        resp = requests.post('https://identitysso-cert.betfair.com/api/certlogin', data=payload, cert=(self.path_cert, self.path_key), headers=headers)
        json_resp=resp.json()
        return json_resp['sessionToken']

    
    def get_common_headers(self):
        # header is used for most url requests
        headers = {'X-Application': self.app_key, 'X-Authentication': self.SSOID, 'content-type': 'application/json'}
        return headers
    
    
    def get_event_ids(self):
        event_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEventTypes", "params": {"filter":{ }}, "id": 1}'
        req = requests.post(self.bet_url, data=event_req.encode('utf-8'), headers=self.headers) 
        eventTypes = req.json()
        
        # create dataframe with all eventypes {marketCount:[], eventType:{id, name}}
        df_eventypes = pd.DataFrame(eventTypes['result'])
        # split the single column of dictionaries eventType:{id, name} into 2 columns in the df
        df_name_id = df_eventypes['eventType'].apply(pd.Series)
        # concat back into one df {id, name, marketCount}
        df_event_ids = pd.concat([df_name_id, df_eventypes.drop(['eventType'], axis=1)], axis=1)
        
        return df_event_ids
    
    
    def get_market_types(self, event_id):
        assert isinstance(event_id, int), 'ERROR: event_id needs to be an int'
        
        Inplay_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketTypes", "params": {"filter":{"eventTypeIds":["' + str(event_id) + '"],\
                "inPlayOnly":false}}, "id": 1}'
        req = requests.post(self.bet_url, data=Inplay_req.encode('utf-8'), headers=self.headers) 
        MarketTypes = req.json()
        MarketTypes