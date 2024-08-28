import sys


import pandas as pd
from datetime import datetime as dt, timedelta
from dateutil import parser
import requests
from constants.definitions import API_KEY, ACCOUNT_ID, OANDA_URL
from infrastructure.exceptions import NullCandles, RequestError, InvalidFunctionArguments
from data_collector import DataCollector
sys.path.append("..")


if "__main__" == __name__:
    data_collector = DataCollector(api_key=API_KEY, account_id=ACCOUNT_ID, oanda_url=OANDA_URL)
    data_collector.run()
