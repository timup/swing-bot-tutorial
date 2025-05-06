import asyncio
import logging
from pathlib import Path
from typing import List, Optional

import aiofiles
import pandas as pd
import yfinance as yf

from core.config import Config
from core.logging_config import get_logger

# Configure logging
logger = get_logger(__name__)

# Use the Config class for data paths
BASE_RAW_DATA_PATH = Config.yfinance_data_dir()


async def _save_df_to_json(df: pd.DataFrame, file_path: Path) -> None:
    """Asynchronously saves a Pandas DataFrame to a JSON file.
    
    Args:
        df: DataFrame to save
        file_path: Target file path
        
    Raises:
        IOError: If file cannot be written
    """
    logger.info(f"Saving DataFrame to {file_path}...")
    try:
        # Convert DataFrame to JSON string in a separate thread to avoid blocking
        json_data = await asyncio.to_thread(
            df.to_json, orient="records", indent=2, date_format="iso"
        )

        async with aiofiles.open(file_path, mode="w", encoding="utf-8") as f:
            await f.write(json_data)
        logger.info(f"Successfully saved data to {file_path}")
    except Exception as e:
        logger.error(f"Error saving DataFrame to {file_path}: {e}")
        raise IOError(f"Failed to save data to {file_path}: {str(e)}") from e


async def _fetch_single_expiry(
    ticker_obj: yf.Ticker,
    ticker_symbol: str,
    expiry_date: str,
    base_save_path: Path,
) -> List[pd.DataFrame]:
    """
    Fetches call and put option data for a single expiry date for a given ticker.
    Saves the raw data to JSON files and returns a list of [calls_df, puts_df].
    
    Args:
        ticker_obj: yfinance Ticker object
        ticker_symbol: Ticker symbol string
        expiry_date: Expiration date string in YYYY-MM-DD format
        base_save_path: Base directory for saving JSON files
        
    Returns:
        List of DataFrames containing call and put options data
        
    Raises:
        ValueError: If data fetching fails or produces invalid results
    """
    logger.info(f"Fetching options for {ticker_symbol} expiry {expiry_date}...")
    fetched_dfs = []
    try:
        option_chain = await asyncio.to_thread(ticker_obj.option_chain, expiry_date)

        if option_chain is None:
            logger.warning(
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
            logger.warning(f"No call options data for {ticker_symbol} expiry {expiry_date}.")

        puts_df = option_chain.puts
        if puts_df is not None and not puts_df.empty:
            puts_df = puts_df.copy()  # Avoid SettingWithCopyWarning
            puts_df["expiryDate"] = expiry_date
            puts_df["optionType"] = "put"
            await _save_df_to_json(puts_df, target_dir / f"{expiry_date}_puts.json")
            fetched_dfs.append(puts_df)
        else:
            logger.warning(f"No put options data for {ticker_symbol} expiry {expiry_date}.")

    except Exception as e:
        logger.error(f"Error fetching/processing {ticker_symbol} expiry {expiry_date}: {e}")
        # Don't raise here to allow other expiries to be processed

    return fetched_dfs


async def fetch_and_store_option_chains(
    ticker_symbol: str, expiry_dates: List[str]
) -> pd.DataFrame:
    """
    Fetches complete option chains (calls and puts) for a user-supplied ticker
    and list of expiry dates from yfinance.
    Stores raw JSON data in data/raw/yfinance/{ticker}/{expiry_date}_{type}.json.
    Returns a single Pandas DataFrame concatenating all fetched option data.
    
    Args:
        ticker_symbol: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        expiry_dates: List of expiration dates in YYYY-MM-DD format
        
    Returns:
        DataFrame containing all option data, or empty DataFrame if no data available
        
    Raises:
        ValueError: If the ticker is invalid or no data could be fetched
    """
    logger.info(
        f"Starting option chain ingestion for ticker: {ticker_symbol}, expiries: {expiry_dates}"
    )

    try:
        # Initialize Ticker object in a thread to avoid blocking
        ticker_obj = await asyncio.to_thread(yf.Ticker, ticker_symbol)
        # Check if ticker is valid by trying to access info
        _ = await asyncio.to_thread(lambda: ticker_obj.options)
    except Exception as e:
        logger.error(
            f"Failed to initialize yfinance.Ticker for {ticker_symbol} or fetch options list: {e}"
        )
        raise ValueError(f"Invalid ticker symbol or data unavailable: {ticker_symbol}") from e

    all_option_data_dfs = []

    # Create tasks for fetching each expiry concurrently
    tasks = [
        _fetch_single_expiry(ticker_obj, ticker_symbol, expiry, BASE_RAW_DATA_PATH)
        for expiry in expiry_dates
    ]

    # asyncio.gather collects results from all tasks
    results_for_expiries = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results_for_expiries:
        if isinstance(result, Exception):
            logger.error(f"A task failed during fetching: {result}")
        elif isinstance(result, list):
            all_option_data_dfs.extend(
                result
            )  # result is a list of DataFrames (calls, puts)
        else:
            logger.warning(f"Unexpected result type from _fetch_single_expiry: {type(result)}")

    if not all_option_data_dfs:
        logger.warning(f"No option data fetched for {ticker_symbol} across specified expiries.")
        return pd.DataFrame()  # Return empty DataFrame if nothing was fetched

    # Concatenate all collected DataFrames into a single master DataFrame
    try:
        final_df = pd.concat(all_option_data_dfs, ignore_index=True)
        logger.info(
            f"Successfully fetched and combined data for {ticker_symbol}. Shape: {final_df.shape}"
        )
        return final_df
    except Exception as e:
        logger.error(f"Error concatenating DataFrames for {ticker_symbol}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on concatenation error


if __name__ == "__main__":
    # Configure logging for standalone execution
    from core.logging_config import configure_logging
    
    configure_logging()
    
    # Example usage for testing
    async def run_test():
        logger.info("Running yfinance_ingestor test...")
        test_ticker = "NVDA"  # A ticker with many options

        logger.info(f"Fetching available expiry dates for {test_ticker}...")
        try:
            # Getting options list is also blocking, access as property via lambda
            expiries = await asyncio.to_thread(lambda: yf.Ticker(test_ticker).options)
        except Exception as e:
            logger.error(f"Could not fetch expiry dates for {test_ticker}: {e}")
            return

        if not expiries:
            logger.error(f"No expiry dates found for {test_ticker}. Cannot run test.")
            return

        # Select a few expiries for testing
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
            logger.error(f"Not enough expiry dates for {test_ticker} to select a diverse set.")
            return

        if not test_expiry_dates:
            logger.error(
                f"No test expiry dates selected for {test_ticker}. Test cannot proceed."
            )
            return

        logger.info(f"Test parameters: Ticker={test_ticker}, Expiries={test_expiry_dates}")

        # Ensure the base data directory exists before running the main function
        BASE_RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured base directory exists: {BASE_RAW_DATA_PATH.resolve()}")

        try:
            df_result = await fetch_and_store_option_chains(test_ticker, test_expiry_dates)
        except ValueError as e:
            logger.error(f"Error during test: {e}")
            return

        if not df_result.empty:
            logger.info(f"\n--- Fetched DataFrame summary ---")
            logger.info(f"Shape: {df_result.shape}")
            logger.info(f"Columns: {df_result.columns.tolist()}")
            
            # Verify expected columns
            if "expiryDate" in df_result.columns and "optionType" in df_result.columns:
                logger.info("expiryDate and optionType columns verified.")
            else:
                logger.error("Missing expected columns in result DataFrame")
            
            logger.info(
                f"Test completed for {test_ticker}. Check 'data/raw/yfinance/{test_ticker}' for JSON files."
            )
        else:
            logger.error(f"Test for {test_ticker} resulted in an empty DataFrame.")

    # Python 3.7+ syntax for running asyncio main
    asyncio.run(run_test())
