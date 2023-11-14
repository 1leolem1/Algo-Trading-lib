import lzma
import dill as pickle
import pandas as pd


def load_pickle(path):
    with lzma.open(path, "rb") as fp:
        file = pickle.load(fp)
    return file


def save_pickle(path, obj):
    with lzma.open(path, "wb") as fp:
        file = pickle.dump(obj, fp)
    return file


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
        portfolio_df.loc[0, "capital"] = 1000  # Init capital
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

    def run_backtest(self):
        print("Running BT...")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        portfolio_df = self.init_portfolio_settings(trade_range=date_range)
        for i in portfolio_df.index:
            date = portfolio_df.loc[i, "datetime"]
