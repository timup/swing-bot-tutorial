import yfinance as yf
from QuantLib import (
    Option,
    TARGET,
    Date,
    Settings,
    SimpleQuote,
    Actual365Fixed,
    Period,
    Days,
    YieldTermStructureHandle,
    FlatForward,
    BlackVolTermStructureHandle,
    BlackConstantVol,
    BlackScholesProcess,
    QuoteHandle,
    AnalyticEuropeanEngine,
    PlainVanillaPayoff,
    EuropeanExercise,
    EuropeanOption,
)
import datetime
import argparse
import traceback


def get_option_data(ticker_symbol: str):
    """
    Fetches option data for a given ticker symbol.
    Targets the first available (nearest) expiry and finds the At-The-Money (ATM) call option.
    """
    print(f"Fetching option data for {ticker_symbol}...")
    stock = yf.Ticker(ticker_symbol)

    stock_info = stock.info
    current_price = stock_info.get("currentPrice") or stock_info.get(
        "regularMarketPrice"
    )
    if current_price is None:
        # Try another common field for current price
        current_price = stock_info.get("previousClose")
        if current_price is None:
            raise ValueError(
                f"Could not fetch current price for {ticker_symbol} from 'currentPrice', 'regularMarketPrice', or 'previousClose'."
            )

    available_expiries = stock.options
    if not available_expiries:
        raise ValueError(f"No option expiry dates found for {ticker_symbol}")

    # Use the first available expiry date
    target_expiry_str = available_expiries[0]
    print(f"Using first available expiry: {target_expiry_str}")

    # Fetch the option chain for the selected expiry
    # yfinance option_chain can sometimes return None or an object without .calls
    option_chain_data = stock.option_chain(target_expiry_str)
    if (
        option_chain_data is None
        or option_chain_data.calls is None
        or option_chain_data.calls.empty
    ):
        raise ValueError(
            f"No call options data found for {ticker_symbol} on expiry {target_expiry_str}"
        )

    chain = option_chain_data.calls

    # Find the ATM (At-The-Money) strike price
    # This is the strike closest to the current stock price
    atm_row_idx = (chain["strike"] - current_price).abs().idxmin()
    atm_row = chain.loc[atm_row_idx]

    # The expiry date string needs to be converted to a date object
    expiry_date_obj = datetime.datetime.strptime(target_expiry_str, "%Y-%m-%d").date()

    S = current_price
    K = atm_row.strike
    sigma = (
        atm_row.impliedVolatility
    )  # This is usually annualized decimal (e.g., 0.20 for 20%)

    # yfinance might return 0 for IV if not available, handle this
    if sigma == 0 or sigma is None:
        print(
            f"Warning: Implied Volatility for {ticker_symbol} strike {K} expiry {target_expiry_str} is {sigma}. Pricing may be inaccurate."
        )
        # Fallback or error? For now, proceed with a warning.
        # A small non-zero value might be used as a placeholder if critical, e.g., 0.00001,
        # but that would be arbitrary. Let's allow 0 and see QuantLib's behavior.
        # QuantLib might raise an error if vol is zero.
        if sigma == 0:
            print(
                "Implied volatility is zero. QuantLib may fail. Consider a different option or data source if this persists."
            )

    return S, K, sigma, expiry_date_obj


def price_option_quantlib(
    S_val,
    K_val,
    sigma_val,
    T_val,
    r_val,
    option_type=Option.Call,
    calendar_choice=TARGET(),
):
    """Prices a European option using QuantLib's Black-Scholes(-Merton) model."""

    # QuantLib global settings
    calculation_date = Date.todaysDate()  # QuantLib's today
    Settings.instance().evaluationDate = calculation_date

    # Option parameters
    spot_price_quote = SimpleQuote(S_val)
    strike_price = K_val
    risk_free_rate = r_val
    day_count = Actual365Fixed()  # Common day count convention

    # Maturity date for QuantLib
    # T_val is time to maturity in years. Convert to days for Period.
    maturity_date_ql = calculation_date + Period(int(T_val * 365), Days)

    # Term structures
    # Risk-free rate term structure (flat forward)
    flat_rf_ts_handle = YieldTermStructureHandle(
        FlatForward(calculation_date, risk_free_rate, day_count)
    )

    # Volatility term structure (flat volatility)
    # Ensure sigma_val is not zero if BlackConstantVol cannot handle it
    if sigma_val <= 0:
        # BlackConstantVol might require positive volatility.
        print(
            f"Warning: Sigma is {sigma_val}. Adjusting to a very small positive number for QuantLib BlackConstantVol."
        )
        sigma_val_adjusted = 1e-9  # A very small positive number
        flat_vol_ts_handle = BlackVolTermStructureHandle(
            BlackConstantVol(
                calculation_date, calendar_choice, sigma_val_adjusted, day_count
            )
        )
    else:
        flat_vol_ts_handle = BlackVolTermStructureHandle(
            BlackConstantVol(calculation_date, calendar_choice, sigma_val, day_count)
        )

    # Setup Black-Scholes process
    # The user's original code used BlackScholesProcess.
    # It expects: QuoteHandle(underlying), YieldTermStructureHandle(riskFreeRateForDrift), BlackVolTermStructureHandle(volatility)
    # The riskFreeRateForDrift is effectively (r-q). If q=0, it's just r.
    # The discounting is done using the riskFreeRateForDrift in BlackScholesProcess if not using BSM.
    bs_process = BlackScholesProcess(
        QuoteHandle(spot_price_quote),
        flat_rf_ts_handle,  # This rate is used for drift (r-q) and discounting in BS
        flat_vol_ts_handle,
    )

    # Pricing engine
    engine = AnalyticEuropeanEngine(bs_process)

    # Option instrument
    payoff = PlainVanillaPayoff(option_type, strike_price)
    exercise = EuropeanExercise(maturity_date_ql)
    option = EuropeanOption(payoff, exercise)
    option.setPricingEngine(engine)

    return option.NPV()


def main():
    """Main function to parse arguments, fetch data, and price the option."""
    parser = argparse.ArgumentParser(
        description="Price a European stock option using yfinance and QuantLib."
    )
    parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL, MSFT).")
    parser.add_argument(
        "--rate",
        type=float,
        default=0.05,
        help="Annual risk-free interest rate (decimal, e.g., 0.05 for 5%%).",
    )
    parser.add_argument(
        "--type",
        type=str,
        default="call",
        choices=["call", "put"],
        help="Option type: 'call' or 'put'.",
    )

    args = parser.parse_args()

    ticker = args.ticker.upper()
    risk_free_rate = args.rate
    option_type_str = args.type.lower()

    ql_option_type = Option.Call if option_type_str == "call" else Option.Put

    try:
        S, K, sigma, expiry_date = get_option_data(ticker)

        today = datetime.date.today()
        if expiry_date <= today:
            print(
                f"Error: Expiry date {expiry_date} for {ticker} is not in the future."
            )
            return

        T_days = (expiry_date - today).days
        if T_days <= 0:  # Should be caught by above, but as a safeguard for T_years
            print(
                f"Error: Option expiry {expiry_date} results in non-positive time to maturity."
            )
            return
        T_years = T_days / 365.0

        print("--- Input Parameters ---")
        print(f"Stock: {ticker}")
        print(f"Current Stock Price (S): {S:.2f}")
        print(f"Strike Price (K): {K:.2f}")
        print(f"Implied Volatility (sigma): {sigma:.4f}")
        print(f"Expiry Date: {expiry_date}")
        print(f"Time to Maturity (T in years): {T_years:.4f} ({T_days} days)")
        print(f"Risk-free rate (r): {risk_free_rate:.4f}")
        print(f"Option Type: {option_type_str.capitalize()}")
        print("------------------------")

        model_price = price_option_quantlib(
            S, K, sigma, T_years, risk_free_rate, ql_option_type
        )

        if (
            model_price is None
        ):  # Should not happen with AnalyticEuropeanEngine unless error in inputs
            print("Error: Could not calculate option price. NPV is None.")
        else:
            print(
                f"QuantLib Model Price for {ticker} {option_type_str.capitalize()}: ${model_price:.2f}"
            )

    except ValueError as ve:
        print(f"Input Error: {ve}")
    except ConnectionError as ce:
        print(f"Network Error: Could not connect to yfinance. {ce}")
    except RuntimeError as qe:  # QuantLib often raises RuntimeErrors
        print(f"QuantLib Runtime Error: {qe}")
        print(
            "This might be due to invalid market data (e.g., zero volatility, past maturity)."
        )
    except Exception:
        print("An unexpected error occurred:")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
