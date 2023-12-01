import pandas as pd
import numpy as np
from utils import get_pnl_stats, vwap


class Alpha2():
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
        """
        if MA_10 > MA_50 then fast_crossover is buy
        if MA_20 > MA_100 then med_crossover is buy
        if MA_50 > MA_200 then slow_crossover is buy

        1. fast_crossover
        3. medium_crossover
        2. slow_crossover

        output taking as value 0, 1, 2 or 3 for how many crossovers are buy

        """

        for inst in self.insts:

            inst_df = self.dfs[inst]

            fast = np.where(inst_df['close'].rolling(
                10).mean() > inst_df['close'].rolling(50).mean(), 1, 0)
            med = np.where(inst_df['close'].rolling(
                20).mean() > inst_df['close'].rolling(100).mean(), 1, 0)
            slow = np.where(inst_df['close'].rolling(
                50).mean() > inst_df['close'].rolling(200).mean(), 1, 0)
            inst_df["alpha_signal"] = fast + med + slow
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

    def run_backtest(self):
        print("Running Backtest...")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        portfolio_df = self.init_portfolio_settings(trade_range=date_range)

        self.compute_meta_informations(date_range)

        for i in portfolio_df.index:
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
                alpha_scores[inst] = self.dfs[inst].loc[date, "alpha_signal"]

            for inst in non_eligibles:
                portfolio_df.loc[i, "{} w".format(inst)] = 0
                portfolio_df.loc[i, "{} units".format(inst)] = 0

            absolute_scores = np.abs(
                [score for score in alpha_scores.values()])
            forecast_chips = np.sum(absolute_scores)
            nominal_expo_tot = 0  # How much money in markets basically

            for inst in eligibles:
                forecast = alpha_scores[inst]
                dollar_alloc = portfolio_df.loc[i, "capital"] / \
                    forecast_chips if forecast_chips > 0 else 0
                position = forecast * dollar_alloc / \
                    self.dfs[inst].loc[date, "close"]
                portfolio_df.loc[i, inst + " units"] = position
                nominal_expo_tot += abs(position *
                                        self.dfs[inst].loc[date, "close"])

            for inst in eligibles:
                units = portfolio_df.loc[i, inst + " units"]
                nominal_expo_inst = units*self.dfs[inst].loc[date, "close"]
                inst_w = nominal_expo_inst / nominal_expo_tot if nominal_expo_tot != 0 else 0
                portfolio_df.loc[i, inst + " w"] = inst_w

            portfolio_df.loc[i, "nominal expo"] = nominal_expo_tot
            portfolio_df.loc[i, "leverage"] = nominal_expo_tot / \
                portfolio_df.loc[i, "capital"]
        print("finished backtest")

        return portfolio_df
