import yfinance as yf
import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover

# --- helper ---------------------------------------------------------------
def SMA(series, n):
    """Simple moving average of a 1‑D numpy/pandas-like array."""
    return pd.Series(series).rolling(n).mean()

# --- strategy -------------------------------------------------------------
class MACross(Strategy):
    n_short = 7
    n_long  = 21

    def init(self):
        self.ma_short = self.I(SMA, self.data.Close, self.n_short)
        self.ma_long  = self.I(SMA, self.data.Close, self.n_long)

    def next(self):
        if crossover(self.ma_short, self.ma_long):
            self.position.close()
            self.buy()
        elif crossover(self.ma_long, self.ma_short):
            self.position.close()
            self.sell()

# --- runner ---------------------------------------------------------------
def main():
    ohlcv_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    spy = (
        yf.Ticker("SPY")
          .history(start="2015-01-01", auto_adjust=False, actions=False)
          [ohlcv_cols]
          .dropna()
    )

    bt = Backtest(spy, MACross, commission=0.002, cash=10_000)
    stats = bt.run()
    bt.plot()
    print(stats[['Return [%]', 'Max. Drawdown [%]', 'Sharpe Ratio']])

if __name__ == "__main__":
    main()