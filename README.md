# Betfair Bot

## Install
Create environment with python=3.10

then run:
'''bash
pip install -r requirements.txt
'''

## What Script to run?

Use the demo.ipynb notebook which goes through step by step getting event_id, event, market, runners, and prices using betfairlightweight
playground.ipynb walks step by step through the .py file recording odds
Follow the instructions at the top to create app_key, and certificates for logging in on demo.ipynb

For info recording odds run:
python record_odds.py -h

To download historic games and pitcher stats run:
python record_mlb_stats.py