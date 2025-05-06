import asyncio
import pathlib
from typing import List

import aiofiles
import pandas as pd
import yfinance as yf

# Base path for storing raw data, relative to the project root where swing-bot is.
# Assuming the script is run from swing-bot directory or project root.
# For robustness, this could be made configurable or determined dynamically.
BASE_RAW_DATA_PATH = pathlib.Path("data/raw/yfinance")


async def _save_df_to_json(df: pd.DataFrame, file_path: pathlib.Path):
    """Asynchronously saves a Pandas DataFrame to a JSON file."""
    print(f"Saving DataFrame to {file_path}...")
    try:
        # Ensure parent directory exists
        # file_path.parent.mkdir(parents=True, exist_ok=True) # Handled by _fetch_single_expiry

        # Convert DataFrame to JSON string in a separate thread to avoid blocking
        json_data = await asyncio.to_thread(
            df.to_json, orient="records", indent=2, date_format="iso"
        )

        async with aiofiles.open(file_path, mode="w", encoding="utf-8") as f:
            await f.write(json_data)
        print(f"Successfully saved data to {file_path}")
    except Exception as e:
        print(f"Error saving DataFrame to {file_path}: {e}")


async def _fetch_single_expiry(
    ticker_obj: yf.Ticker,
    ticker_symbol: str,
    expiry_date: str,
    base_save_path: pathlib.Path,
) -> List[pd.DataFrame]:
    """
    Fetches call and put option data for a single expiry date for a given ticker.
    Saves the raw data to JSON files and returns a list of [calls_df, puts_df].
    Returns an empty list if data fetching fails for this expiry.
    """
    print(f"Fetching options for {ticker_symbol} expiry {expiry_date}...")
    fetched_dfs = []
    try:
        option_chain = await asyncio.to_thread(ticker_obj.option_chain, expiry_date)

        if option_chain is None:
            print(
                f"No option chain data returned for {ticker_symbol} expiry {expiry_date}."
            )
            return []

        target_dir = base_save_path / ticker_symbol
        await asyncio.to_thread(target_dir.mkdir, parents=True, exist_ok=True)

        calls_df = option_chain.calls
        if calls_df is not None and not calls_df.empty:
            calls_df = calls_df.copy()  # Avoid SettingWithCopyWarning
            calls_df["expiryDate"] = expiry_date
            calls_df["optionType"] = "call"
            await _save_df_to_json(calls_df, target_dir / f"{expiry_date}_calls.json")
            fetched_dfs.append(calls_df)
        else:
            print(f"No call options data for {ticker_symbol} expiry {expiry_date}.")

        puts_df = option_chain.puts
        if puts_df is not None and not puts_df.empty:
            puts_df = puts_df.copy()  # Avoid SettingWithCopyWarning
            puts_df["expiryDate"] = expiry_date
            puts_df["optionType"] = "put"
            await _save_df_to_json(puts_df, target_dir / f"{expiry_date}_puts.json")
            fetched_dfs.append(puts_df)
        else:
            print(f"No put options data for {ticker_symbol} expiry {expiry_date}.")

    except Exception as e:
        print(f"Error fetching/processing {ticker_symbol} expiry {expiry_date}: {e}")
        # Optionally, re-raise or handle more gracefully depending on requirements

    return fetched_dfs


async def fetch_and_store_option_chains(
    ticker_symbol: str, expiry_dates: List[str]
) -> pd.DataFrame:
    """
    Fetches complete option chains (calls and puts) for a user-supplied ticker
    and list of expiry dates from yfinance.
    Stores raw JSON data in data/raw/yfinance/{ticker}/{expiry_date}_{type}.json.
    Returns a single Pandas DataFrame concatenating all fetched option data.
    """
    print(
        f"Starting option chain ingestion for ticker: {ticker_symbol}, expiries: {expiry_dates}"
    )

    try:
        # Initialize Ticker object in a thread to avoid blocking
        ticker_obj = await asyncio.to_thread(yf.Ticker, ticker_symbol)
        # Basic check if ticker is valid by trying to access info (which also blocks)
        # A more robust check might be needed if .info is too slow or unreliable for this purpose.
        # If ticker_obj.info is empty or raises, it indicates an issue.
        # However, yf.Ticker() itself doesn't usually raise for invalid tickers immediately.
        # It often fails later when data is requested.
        # Let's try to fetch options list to see if it works
        _ = await asyncio.to_thread(lambda: ticker_obj.options)
    except Exception as e:
        print(
            f"Failed to initialize yfinance.Ticker for {ticker_symbol} or fetch options list: {e}"
        )
        return pd.DataFrame()  # Return empty DataFrame on ticker initialization failure

    all_option_data_dfs = []

    # Create tasks for fetching each expiry concurrently
    tasks = [
        _fetch_single_expiry(ticker_obj, ticker_symbol, expiry, BASE_RAW_DATA_PATH)
        for expiry in expiry_dates
    ]

    # asyncio.gather collects results from all tasks
    # return_exceptions=True allows us to handle individual task failures if needed
    results_for_expiries = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results_for_expiries:
        if isinstance(result, Exception):
            print(f"A task failed during fetching: {result}")
        elif isinstance(result, list):
            all_option_data_dfs.extend(
                result
            )  # result is a list of DataFrames (calls, puts)
        else:
            print(f"Unexpected result type from _fetch_single_expiry: {type(result)}")

    if not all_option_data_dfs:
        print(f"No option data fetched for {ticker_symbol} across specified expiries.")
        return pd.DataFrame()  # Return empty DataFrame if nothing was fetched

    # Concatenate all collected DataFrames into a single master DataFrame
    try:
        final_df = pd.concat(all_option_data_dfs, ignore_index=True)
        print(
            f"Successfully fetched and combined data for {ticker_symbol}. Shape: {final_df.shape}"
        )
        return final_df
    except Exception as e:
        print(f"Error concatenating DataFrames for {ticker_symbol}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on concatenation error


if __name__ == "__main__":
    # Example usage for testing
    async def run_test():
        print("Running yfinance_ingestor test...")
        # test_ticker = "AAPL"
        # test_ticker = "MSFT"
        test_ticker = "NVDA"  # A ticker with many options

        print(f"Fetching available expiry dates for {test_ticker}...")
        try:
            # Getting options list is also blocking, access as property via lambda
            expiries = await asyncio.to_thread(lambda: yf.Ticker(test_ticker).options)
        except Exception as e:
            print(f"Could not fetch expiry dates for {test_ticker}: {e}")
            return

        if not expiries:
            print(f"No expiry dates found for {test_ticker}. Cannot run test.")
            return

        # Select a few expiries for testing, e.g., the first 2-3
        # test_expiry_dates = list(expiries[:min(len(expiries), 3)])
        # Let's try a specific farther out expiry if available
        if len(expiries) > 5:
            test_expiry_dates = [
                expiries[0],
                expiries[len(expiries) // 2],
                expiries[-1],
            ]  # near, mid, far
            test_expiry_dates = list(
                set(test_expiry_dates)
            )  # ensure unique if selection overlaps
            test_expiry_dates.sort()
            if len(test_expiry_dates) > 3:
                test_expiry_dates = test_expiry_dates[:3]  # Cap at 3 for test
        elif expiries:
            test_expiry_dates = list(expiries[: min(len(expiries), 3)])
        else:
            print(f"Not enough expiry dates for {test_ticker} to select a diverse set.")
            return

        if not test_expiry_dates:
            print(
                f"No test expiry dates selected for {test_ticker}. Test cannot proceed."
            )
            return

        print(f"Test parameters: Ticker={test_ticker}, Expiries={test_expiry_dates}")

        # Ensure the base data directory exists before running the main function
        # Although the function itself should handle subdir creation.
        BASE_RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        print(f"Ensured base directory exists: {BASE_RAW_DATA_PATH.resolve()}")

        df_result = await fetch_and_store_option_chains(test_ticker, test_expiry_dates)

        if not df_result.empty:
            print("\n--- Fetched DataFrame Head ---")
            print(df_result.head())
            print("\n--- Fetched DataFrame Info ---")
            df_result.info()
            print(f"\nTotal options contracts fetched: {len(df_result)}")

            # Verify columns
            print("\n--- DataFrame Columns ---")
            print(df_result.columns.tolist())
            assert "expiryDate" in df_result.columns
            assert "optionType" in df_result.columns
            print("expiryDate and optionType columns verified.")

            print("\n--- Sample of expiryDate and optionType ---")
            print(
                df_result[["expiryDate", "optionType"]].sample(min(5, len(df_result)))
            )

            print(
                f"\nTest completed for {test_ticker}. Check 'data/raw/yfinance/{test_ticker}' for JSON files."
            )
        else:
            print(f"Test for {test_ticker} resulted in an empty DataFrame.")

    # Python 3.7+ syntax for running asyncio main
    asyncio.run(run_test())
