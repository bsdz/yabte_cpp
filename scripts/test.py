import sys

sys.path.append("/home/blair/projects/yabte_cpp/build/debug/pybind")


import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from yabte.tests._helpers import generate_nasdaq_dataset
from yabte.utilities.strategy_helpers import crossover
from yabte_cpp_backtest import (
    Book,
    OHLCAsset,
    SimpleOrder,
    Strategy,
    StrategyRunner,
    __version__,
)

__version__


# load some data
assets, df_combined = generate_nasdaq_dataset()

table = pa.Table.from_pandas(df_combined)

# table = pq.read_table(
#     "/home/blair/projects/yabte_cpp/src_test/data/data_sample.parquet"
# )


# create a book with 100000 cash
book = Book(name="Main", cash=100000)

assets = [OHLCAsset(name=a.name) for a in assets]


class SMAXO(Strategy):
    def __init__(self):
        Strategy.__init__(self)

    def clone(self):
        cloned = SMAXO.__new__(SMAXO)
        Strategy.__init__(cloned, self)
        cloned.__dict__.update(self.__dict__)
        return cloned

    def extend_data(self, data):
        # enhance data with simple moving averages

        df = data.to_pandas()

        p = self.params
        days_short = p["days_short"]
        days_long = p["days_long"]

        close_sma_short = (
            df.loc[:, (slice(None), "Close")]
            .rolling(days_short)
            .mean()
            .rename({"Close": "CloseSMAShort"}, axis=1, level=1)
        )
        close_sma_long = (
            df.loc[:, (slice(None), "Close")]
            .rolling(days_long)
            .mean()
            .rename({"Close": "CloseSMALong"}, axis=1, level=1)
        )
        new_df = pd.concat([close_sma_short, close_sma_long], axis=1).sort_index(axis=1)

        return pa.Table.from_pandas(new_df)

    def init(self): ...

    def on_close(self):
        # create some orders

        p = self.params
        days_long = p["days_long"]

        if (L := len(self.data)) < days_long + 1:
            return

        # for symbol in ["GOOG", "MSFT"]:
        for symbol in [
            "GOOG",
        ]:
            key_long = f"('{symbol}', 'CloseSMALong')"
            key_short = f"('{symbol}', 'CloseSMAShort')"

            close_sma_short = self.data[key_short].slice(L - 2, 2).to_pandas()
            close_sma_long = self.data[key_long].slice(L - 2, 2).to_pandas()

            if crossover(close_sma_short, close_sma_long):
                self.orders.append(SimpleOrder(asset_name=symbol, size=-100))
            elif crossover(close_sma_long, close_sma_short):
                self.orders.append(SimpleOrder(asset_name=symbol, size=100))


strategies = [SMAXO()]
books = [book]

# foo = Foo(assets, strategies, books)

sr = StrategyRunner(table, assets, strategies, books)
srr = sr.run(dict(days_short=10, days_long=20))

srr.books[0].history

...
