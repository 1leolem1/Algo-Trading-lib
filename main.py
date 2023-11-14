import threading
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pytz


def get_sp500_tickers():
    # Wikipedia URL for the S&P 500 companies
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    res = requests.get(url)
    soup = BeautifulSoup(res.content, "html.parser")

    # Find the table containing the S&P 500 tickers
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))
    return list(df[0].Symbol)


tickers = get_sp500_tickers()

per_start = datetime(2014, 1, 1, tzinfo=pytz.utc)
per_stop = datetime(2023, 11, 13, tzinfo=pytz.utc)


def get_history(ticker, start, end, interval="1d"):
    df = yf.Ticker(ticker).history(start=start,
                                   end=end,
                                   interval=interval,
                                   auto_adjust=True
                                   )
    return df
