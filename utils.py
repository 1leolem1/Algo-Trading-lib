import lzma
import dill as pickle
import pandas as pd
import numpy as np
import random as r


def load_pickle(path):
    with lzma.open(path, "rb") as fp:
        file = pickle.load(fp)
    return file


def save_pickle(path, obj):
    with lzma.open(path, "wb") as fp:
        file = pickle.dump(obj, fp)
    return file


def get_pnl_stats(date, prev_date, portfolio_df, insts, idx, dfs):
    daily_pnl = 0
    nominal_return = 0

    for inst in insts:
        units = portfolio_df.loc[idx - 1, "{} units".format(inst)]
        if units != 0:
            delta = dfs[inst].loc[date, "close"] - \
                dfs[inst].loc[prev_date, "close"]
            inst_pnl = delta * units
            daily_pnl += inst_pnl
            nominal_return += portfolio_df.loc[idx - 1,
                                               "{} w".format(inst)] * dfs[inst].loc[date, "return"]
    capital_return = nominal_return * portfolio_df.loc[idx-1, "leverage"]
    portfolio_df.loc[idx, "capital"] = portfolio_df.loc[idx -
                                                        1, "capital"] + daily_pnl
    portfolio_df.loc[idx, "day_pnl"] = daily_pnl
    portfolio_df.loc[idx, "nominal_return"] = nominal_return
    portfolio_df.loc[idx, "capital_return"] = capital_return
    return daily_pnl, capital_return


class Alpha():
    def __init__(self, insts, dfs, start, end):
        self.insts = insts
        self.start = start
        self.dfs = dfs
        self.end = end

    def init_portfolio_settings(self, trade_range):
        portfolio_df = pd.DataFrame(index=trade_range)\
            .reset_index()\
            .rename(columns={"index": "datetime"})
        portfolio_df.loc[0, "capital"] = 1000000  # Init capital
        return portfolio_df

    def compute_meta_informations(self, trade_range):
        for inst in self.insts:
            df = pd.DataFrame(index=trade_range)
            self.dfs[inst] = df.join(self.dfs[inst])\
                .fillna(method="ffill")\
                .fillna(method="bfill")
            self.dfs[inst]["return"] = -1 + self.dfs[inst]["close"] / \
                self.dfs[inst]["close"].shift(1)  # linear not log returns
            sampled_bool = self.dfs[inst]["close"] != self.dfs[inst]["close"].shift(
                1).fillna(method="bfill")
            eligible = sampled_bool.rolling(5).apply(
                lambda x: int(np.any(x))).fillna(0)

            self.dfs[inst]["eligible"] = eligible.astype(
                int) & (self.dfs[inst]["close"] > 0).astype(int)
        return 0

    def run_backtest(self):
        print("Running BT...")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        portfolio_df = self.init_portfolio_settings(trade_range=date_range)

        self.compute_meta_informations(date_range)

        for i in portfolio_df.index[:1000]:
            date = portfolio_df.loc[i, "datetime"]

            eligibles = [
                inst for inst in self.insts if self.dfs[inst].loc[date, "eligible"]]
            non_eligibles = [
                inst for inst in self.insts if inst not in eligibles]

            if i != 0:
                date_prev = portfolio_df.loc[i-1, "datetime"]
                daily_pnl, capital_return = get_pnl_stats(date=date,
                                                          prev_date=date_prev,
                                                          portfolio_df=portfolio_df,
                                                          insts=self.insts,
                                                          idx=i,
                                                          dfs=self.dfs
                                                          )

            alpha_scores = {}

            for inst in eligibles:
                alpha_scores[inst] = r.uniform(0, 1)
            alpha_scores = {k: v for k, v in sorted(
                alpha_scores.items(), key=lambda pair: pair[1])}
            tickers_short = list(alpha_scores.keys())[
                :int(len(alpha_scores)/4)]
            tickers_long = list(alpha_scores.keys()
                                )[-int(len(alpha_scores)/4):]

            for inst in non_eligibles:
                portfolio_df.loc[i, "{} w".format(inst)] = 0
                portfolio_df.loc[i, "{} units".format(inst)] = 0

            nominal_expo_tot = 0  # How much money in markets basically
            for inst in eligibles:
                forecast = 1 if inst in tickers_long else (
                    -1 if inst in tickers_short else 0)

                dollar_alloc = portfolio_df.loc[i, "capital"] / \
                    (len(tickers_long)+len(tickers_short))
                position = forecast * dollar_alloc / \
                    self.dfs[inst].loc[date, "close"]
                portfolio_df.loc[i, inst + " units"] = position
                nominal_expo_tot += abs(position *
                                        self.dfs[inst].loc[date, "close"])

            for inst in eligibles:
                units = portfolio_df.loc[i, inst + " units"]
                nominal_expo_inst = units*self.dfs[inst].loc[date, "close"]
                inst_w = nominal_expo_inst / nominal_expo_tot
                portfolio_df.loc[i, inst + " w"] = inst_w

            portfolio_df.loc[i, "nominal expo"] = nominal_expo_tot
            portfolio_df.loc[i, "leverage"] = nominal_expo_tot / \
                portfolio_df.loc[i, "capital"]

            print(portfolio_df.loc[i])
