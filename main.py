from utils import Alpha
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


def get_history(ticker, start, end, interval="1d", tries=0):
    try:
        df = yf.Ticker(ticker).history(start=start,
                                       end=end,
                                       interval=interval,
                                       auto_adjust=True
                                       ).reset_index()
    except Exception as err:

        if tries < 5:
            return get_history(ticker, start, end, interval, tries+1)
        return pd.DataFrame()
    try:
        df = df.rename(columns={
            "Date": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })
        df = df.drop(columns=["Dividends", "Stock Splits"])
    except:
        pass

    if df.empty:
        return pd.DataFrame()
    df["datetime"] = df["datetime"].dt.tz_localize(
        None).dt.tz_localize(pytz.utc)
    df = df.set_index("datetime", drop=True)
    print(ticker)
    return pd.DataFrame(df)


def get_histories(tickers, period_start, period_end):
    dfs = [None] * len(tickers)
    filterd_tickers = []  # gets correct ticker list incl in dfs

    def _helper(i):
        df = get_history(tickers[i],
                         period_start[i],
                         period_end[i])
        dfs[i] = df
        if not df.empty:
            filterd_tickers.append(tickers[i])

    threads = [threading.Thread(target=_helper, args=(i,))
               for i in range(len(tickers))]
    [thread.start() for thread in threads]
    [thread.join()for thread in threads]
    return dfs, filterd_tickers


def get_ticker_df(start, end):
    from utils import load_pickle, save_pickle
    try:
        tickers, tickers_dfs = load_pickle("dataset.obj")
    except Exception as err:
        # Gets a dictionary that maps ticker to df
        tickers = get_sp500_tickers()
        starts, ends = [start]*len(tickers), [end]*len(tickers)
        dfs, tickers = get_histories(
            tickers=tickers,
            period_start=starts,
            period_end=ends
        )
        tickers_dfs = {ticker: df for ticker, df in zip(tickers, dfs)}
        save_pickle("dataset.obj", obj=(tickers, tickers_dfs))
    return tickers, tickers_dfs


"""Main code"""


tickers = get_sp500_tickers()

tz_utc = pytz.utc
per_start = datetime(2016, 1, 1, tzinfo=tz_utc)
per_stop = datetime.now(tz_utc)
tickers, dict_tickers = get_ticker_df(start=per_start, end=per_stop)

test_no = 50
tickers = tickers[:test_no]


alpha = Alpha(insts=tickers,
              start=per_start,
              end=per_stop,
              dfs=dict_tickers)
alpha.run_backtest()
