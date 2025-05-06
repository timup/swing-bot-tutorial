import argparse
import asyncio
import logging
from datetime import datetime, date

from data_ingestion.openbb_ingestor import ingest_historical_chains_for_ticker_period
from core.logging_config import configure_logging # Corrected name

logger = logging.getLogger(__name__)

def main():
    configure_logging() # Initialize logging configuration # Corrected name

    parser = argparse.ArgumentParser(
        description="Run historical option chain ingestion using OpenBB."
    )
    parser.add_argument(
        "ticker", type=str, help="Ticker symbol to fetch data for (e.g., AAPL)."
    )
    parser.add_argument(
        "start_date",
        type=str,
        help="Start date for historical data ingestion (YYYY-MM-DD).",
    )
    parser.add_argument(
        "end_date",
        type=str,
        help="End date for historical data ingestion (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--provider",
        type=str,
        default="cboe",
        help="OpenBB provider for option chains (default: cboe). Examples: cboe, tmx.",
    )

    args = parser.parse_args()

    try:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format. Please use YYYY-MM-DD. Error: {e}")
        print(f"Error: Invalid date format. Please use YYYY-MM-DD. {e}")
        return

    if start_dt > end_dt:
        logger.error("Start date cannot be after end date.")
        print("Error: Start date cannot be after end date.")
        return

    logger.info(
        f"Initiating OpenBB historical ingestion for {args.ticker} "
        f"from {start_dt} to {end_dt} using provider {args.provider}."
    )

    try:
        asyncio.run(
            ingest_historical_chains_for_ticker_period(
                ticker=args.ticker,
                start_date=start_dt,
                end_date=end_dt,
                provider=args.provider,
            )
        )
        logger.info("OpenBB historical ingestion script completed.")
    except Exception as e:
        logger.error(f"An error occurred during the ingestion script: {e}", exc_info=True)
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main() 