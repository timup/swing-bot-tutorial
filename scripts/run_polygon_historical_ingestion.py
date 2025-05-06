import argparse
import asyncio
import logging
import os
from datetime import datetime, date

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from data_ingestion.polygon_ingestor import ingest_historical_chain_for_day
from core import configure_logging, Config # Import Config to check for API key
from core.config import Config  # Ensures POLYGON_API_KEY is loaded via .env
from core.database import get_db, engine  # engine for direct check
from core.db_models import OptionQuote, OptionContract

logger = logging.getLogger(__name__)

async def check_if_data_exists(session: AsyncSession, underlying_ticker: str, as_of_date: date) -> bool:
    """Checks if Polygon EOD quotes exist for the given ticker and date."""
    stmt = (
        select(func.count(OptionQuote.id))
        .join(OptionContract, OptionQuote.contract_id == OptionContract.id)
        .where(
            and_(
                OptionContract.underlying_ticker == underlying_ticker.upper(),
                OptionQuote.quote_date == as_of_date,
                OptionQuote.data_source == 'polygon_io_eod'
            )
        )
    )
    result = await session.execute(stmt)
    count = result.scalar_one_or_none()
    return count is not None and count > 0

async def main():
    parser = argparse.ArgumentParser(description="Fetch historical option chain data from Polygon.io and store it.")
    parser.add_argument("--ticker", type=str, required=True, help="Underlying stock ticker symbol (e.g., AAPL).")
    parser.add_argument("--date", type=str, required=True, help="Date for historical data in YYYY-MM-DD format.")
    
    args = parser.parse_args()

    try:
        as_of_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD.")
        return

    configure_logging() # Use centralized logging config

    if not Config.POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY is not set. Please configure it in your .env file or environment.")
        return

    logger.info(f"CLI request to ingest Polygon data for {args.ticker} on {as_of_date}")

    # Idempotency Check
    async for session in get_db():
        try:
            data_exists = await check_if_data_exists(session, args.ticker, as_of_date)
            if data_exists:
                logger.info(f"Polygon EOD data for {args.ticker} on {as_of_date} already exists in the database. Skipping ingestion.")
                return
        except Exception as e:
            logger.error(f"Error during idempotency check: {e}", exc_info=True)
            return # Exit if check fails
        finally:
            # Session is managed by get_db context manager
            pass
    
    # Proceed with ingestion
    logger.info(f"No existing Polygon EOD data found for {args.ticker} on {as_of_date}. Starting ingestion.")
    try:
        await ingest_historical_chain_for_day(
            underlying_ticker=args.ticker,
            as_of_date=as_of_date
        )
        logger.info(f"Ingestion process completed for {args.ticker} on {as_of_date}.")
    except Exception as e:
        logger.error(f"An error occurred during the ingestion process for {args.ticker} on {as_of_date}: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure PYTHONASYNCIODEBUG is set for more detailed asyncio debugging if needed
    # os.environ['PYTHONASYNCIODEBUG'] = '1'
    asyncio.run(main()) 