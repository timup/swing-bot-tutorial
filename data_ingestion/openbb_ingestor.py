import asyncio
import logging
from datetime import date
from typing import Optional

import pandas as pd
from openbb import obb
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db # Assuming get_db_session is the way to get a session
from core.db_models import OptionContract, OptionQuote # Corrected casing
# from core.config import settings # If API keys or other configs were needed

logger = logging.getLogger(__name__)


async def get_historical_option_chain_for_date(
    ticker: str, trade_date: date, provider: str = "cboe"
) -> Optional[pd.DataFrame]:
    """
    Fetches the historical option chain for a given ticker and date using OpenBB.
    """
    logger.info(f"Fetching OpenBB option chain for {ticker} on {trade_date} via {provider}...")
    try:
        # obb.derivatives.options.chains is a synchronous call
        # We need to run it in a separate thread to avoid blocking asyncio event loop
        chain_data_obbject = await asyncio.to_thread(
            obb.derivatives.options.chains,
            symbol=ticker,
            date=str(trade_date),  # OpenBB might expect date as string
            provider=provider,
        )
        if chain_data_obbject and hasattr(chain_data_obbject, 'results'):
            # Assuming results is a list of OBBject items or a DataFrame directly
            # If it's not a DataFrame, .to_df() might be needed on chain_data_obbject or its parts
            # For now, let's assume .to_df() works on the main object if it's not already a df
            if isinstance(chain_data_obbject.results, pd.DataFrame):
                raw_df = chain_data_obbject.results
            elif hasattr(chain_data_obbject, 'to_df'): # Common pattern for OBBjects
                 raw_df = await asyncio.to_thread(chain_data_obbject.to_df)
            elif isinstance(chain_data_obbject.results, list) and len(chain_data_obbject.results) > 0:
                 # If results is a list of Pydantic models, convert to list of dicts then DataFrame
                 # This part is speculative and depends on actual OpenBB output for chains
                 try:
                    list_of_dicts = [res.model_dump() for res in chain_data_obbject.results]
                    raw_df = pd.DataFrame(list_of_dicts)
                 except Exception as e_todict:
                    logger.error(f"Could not convert OpenBB results to DataFrame for {ticker} on {trade_date}: {e_todict}")
                    return None
            else:
                logger.warning(f"OpenBB returned data for {ticker} on {trade_date}, but results are not a DataFrame or convertible list.")
                return None

            if raw_df.empty:
                logger.info(f"No option chain data found for {ticker} on {trade_date} via {provider}.")
                return None
            logger.info(f"Successfully fetched {len(raw_df)} contracts for {ticker} on {trade_date} via {provider}.")
            return raw_df
        else:
            logger.info(f"No option chain data returned by OpenBB for {ticker} on {trade_date} via {provider}.")
            return None
    except Exception as e:
        logger.error(f"Error fetching OpenBB option chain for {ticker} on {trade_date} via {provider}: {e}")
        # Consider specific exception handling for OpenBB errors if they have custom types
        return None


async def transform_openbb_chain_data(
    raw_df: pd.DataFrame, ticker: str, trade_date: date, provider: str
) -> Optional[pd.DataFrame]:
    """
    Transforms the raw DataFrame from OpenBB into the structure needed for DB persistence.
    Column names will depend on the provider (CBOE assumed first).
    """
    logger.info(f"Transforming OpenBB data for {ticker} on {trade_date} from {provider}...")
    if raw_df is None or raw_df.empty:
        logger.warning("Raw DataFrame is empty, cannot transform.")
        return None

    processed_rows = []
    # Placeholder for column mapping - THIS WILL NEED ADJUSTMENT BASED ON ACTUAL CBOE OUTPUT
    # Expected columns from OpenBB CBOE (example, verify actuals):
    # 'contractSymbol', 'optionType', 'expiration', 'strike', 'lastTradeDate',
    # 'lastPrice', 'bid', 'ask', 'change', 'percentChange', 'volume', 'openInterest', 'impliedVolatility'

    # Our DB schema (simplified for this placeholder):
    # OptionsContract: contract_symbol, underlying_ticker, option_type, expiration_date, strike_price
    # OptionsQuote: contract_id (FK), quote_date, last_price, bid, ask, volume, open_interest, implied_volatility, data_source

    # Iterate and transform. This is a basic sketch.
    for _, row in raw_df.iterrows():
        try:
            # TODO: Define actual column mapping from OpenBB (provider CBOE) to our schema
            # contract_symbol = row.get('contractSymbol') or f"{ticker.upper()}{row.get('expiration').replace('-', '')[2:]}{row.get('optionType')[0].upper()}{int(row.get('strike')*1000):08d}"
            # option_type_map = {'call': 'C', 'put': 'P'} # example
            
            # This is highly speculative and needs real data inspection
            processed_row = {
                'contract_symbol': row.get('contract_symbol'), # Or generate if not directly available
                'underlying_ticker': ticker.upper(),
                'option_type': str(row.get('option_type')).lower(), # 'call' or 'put'
                'expiration_date': pd.to_datetime(row.get('expiration_date')).date(), # ensure date object
                'strike_price': float(row.get('strike')),
                'quote_date': trade_date,
                'last_price': float(row.get('last_trade_price', row.get('lastPrice', None))), # Handle different possible names
                'bid': float(row.get('bid')),
                'ask': float(row.get('ask')),
                'volume': int(row.get('volume', 0)),
                'open_interest': int(row.get('open_interest', row.get('openInterest',0))),
                'implied_volatility': float(row.get('implied_volatility', row.get('impliedVolatility', None))),
                'data_source': f"openbb_{provider}",
                # Add other fields like greeks if available and in schema
                # 'delta': float(row.get('delta', None)),
                # 'gamma': float(row.get('gamma', None)),
                # 'theta': float(row.get('theta', None)),
                # 'vega': float(row.get('vega', None)),
                # 'rho': float(row.get('rho', None)),
            }
            # Basic validation
            if not all([processed_row['contract_symbol'], processed_row['option_type'], processed_row['expiration_date'], processed_row['strike_price'] is not None]):
                 logger.warning(f"Skipping row due to missing core contract fields: {row}")
                 continue
            processed_rows.append(processed_row)
        except Exception as e:
            logger.error(f"Error transforming row: {row}. Error: {e}")
            continue
    
    if not processed_rows:
        logger.warning(f"No rows could be transformed for {ticker} on {trade_date}.")
        return None

    transformed_df = pd.DataFrame(processed_rows)
    logger.info(f"Successfully transformed {len(transformed_df)} contracts for {ticker} on {trade_date}.")
    return transformed_df


async def persist_option_chain_data(
    session: AsyncSession, processed_df: pd.DataFrame #, underlying_ticker: str, quote_date: date
):
    """
    Persists the transformed option chain data (contracts and quotes) to the database.
    `processed_df` should contain all necessary fields after transformation.
    """
    if processed_df is None or processed_df.empty:
        logger.warning("Processed DataFrame is empty, nothing to persist.")
        return 0
    
    # from core.db_models import OptionsContract, OptionsQuote # Import here or at module level
    from sqlalchemy.dialects.postgresql import insert as pg_insert # For on_conflict_do_nothing
    from sqlalchemy import select # For fetching existing contract ID

    count_persisted = 0
    logger.info(f"Persisting {len(processed_df)} processed option records...")

    # This section will need actual DB models and SQLAlchemy logic
    # For now, it's a placeholder
    #
    # Example logic sketch:
    # for _, row_data in processed_df.iterrows():
    #     try:
    #         # 1. Upsert OptionsContract
    #         contract_stmt = pg_insert(OptionsContract).values(
    #             contract_symbol=row_data['contract_symbol'],
    #             underlying_ticker=row_data['underlying_ticker'],
    #             option_type=row_data['option_type'],
    #             expiration_date=row_data['expiration_date'],
    #             strike_price=row_data['strike_price']
    #             # ... other contract fields if any
    #         ).on_conflict_do_nothing(
    #             index_elements=['contract_symbol'] # Assuming 'contract_symbol' is unique constraint
    #         ).returning(OptionsContract.id)
    #
    #         result = await session.execute(contract_stmt)
    #         contract_id = result.scalar_one_or_none()
    #
    #         if contract_id is None: # If conflict and nothing returned, fetch existing
    #             select_stmt = select(OptionsContract.id).where(OptionsContract.contract_symbol == row_data['contract_symbol'])
    #             result = await session.execute(select_stmt)
    #             contract_id = result.scalar_one()
    #
    #         # 2. Insert OptionsQuote
    #         quote_stmt = pg_insert(OptionsQuote).values(
    #             contract_id=contract_id,
    #             quote_date=row_data['quote_date'],
    #             last_price=row_data.get('last_price'),
    #             bid=row_data.get('bid'),
    #             ask=row_data.get('ask'),
    #             volume=row_data.get('volume'),
    #             open_interest=row_data.get('open_interest'),
    #             implied_volatility=row_data.get('implied_volatility'),
    #             data_source=row_data['data_source']
    #             # ... other quote fields
    #         ).on_conflict_do_nothing( # Assuming unique on (contract_id, quote_date, data_source)
    #             index_elements=['contract_id', 'quote_date', 'data_source']
    #         )
    #         await session.execute(quote_stmt)
    #         count_persisted += 1
    #     except Exception as e:
    #         logger.error(f"Error persisting row {row_data.get('contract_symbol', 'UNKNOWN')} for date {row_data.get('quote_date', 'UNKNOWN')}: {e}")
    #         await session.rollback() # Rollback for this specific row error or handle differently
    #         # raise # Or continue to next row
    #
    # if count_persisted > 0:
    #    await session.commit()
    # logger.info(f"Successfully persisted {count_persisted} option quote records.")
    # return count_persisted
    
    # Placeholder return until DB logic is filled
    logger.warning("DB Persistence logic is a placeholder in openbb_ingestor.py.")
    return 0


async def ingest_historical_chains_for_ticker_period(
    ticker: str, start_date: date, end_date: date, provider: str = "cboe"
):
    """
    Main orchestrator to fetch, transform, and persist historical option chains
    for a given ticker over a specified date range.
    """
    logger.info(
        f"Starting historical option chain ingestion for {ticker} "
        f"from {start_date} to {end_date} via {provider}."
    )
    
    # Iterate through dates (simple iteration, could be refined for trading days)
    current_date = start_date
    total_quotes_ingested = 0
    
    # Create a single session for the whole period, or per day?
    # For long periods, committing per day might be better.
    # async with get_db_session() as session: # if get_db_session is a context manager
    
    db_session: Optional[AsyncSession] = None
    try:
        # How get_db_session works is important. Assuming it yields a session.
        # This part needs to align with how database.py provides sessions.
        # If it's a generator:
        # async for session_in_loop in get_db_session():
        #    db_session = session_in_loop
        #    break # Get one session for now
        # Or if it returns a session directly (less common for proper teardown):
        # db_session = await get_db_session() # This is not typical for `async with` patterns

        # For now, let's assume we need to manage the session explicitly if not using `async with`
        # This is a simplification and should be replaced with proper session management from core.database
        
        date_range = pd.date_range(start_date, end_date, freq='B') # 'B' for business days

        for trade_date_dt in date_range:
            trade_date_actual = trade_date_dt.date()
            logger.info(f"Processing {ticker} for date: {trade_date_actual}")

            raw_df = await get_historical_option_chain_for_date(
                ticker=ticker, trade_date=trade_date_actual, provider=provider
            )

            if raw_df is not None and not raw_df.empty:
                transformed_df = await transform_openbb_chain_data(
                    raw_df=raw_df, ticker=ticker, trade_date=trade_date_actual, provider=provider
                )
                if transformed_df is not None and not transformed_df.empty:
                    # Create a new session for each day's transaction or manage a longer-lived one
                    async for session in get_db(): # Assuming get_db_session is an async generator
                        try:
                            persisted_count = await persist_option_chain_data(
                                session=session, processed_df=transformed_df
                            )
                            total_quotes_ingested += persisted_count
                            await session.commit() # Commit after each successful day's persistence
                            logger.info(f"Committed {persisted_count} quotes for {ticker} on {trade_date_actual}.")
                        except Exception as e_persist:
                            logger.error(f"Failed to persist or commit for {ticker} on {trade_date_actual}: {e_persist}")
                            await session.rollback()
                        break # Assuming get_db_session yields one session then we are done with it.
            await asyncio.sleep(1) # Small delay to be polite to the API

    except Exception as e:
        logger.error(f"Overall ingestion error for {ticker} [{start_date}-{end_date}]: {e}")
    finally:
        # Ensure session is closed if manually managed and opened
        # if db_session:
        #    await db_session.close()
        pass

    logger.info(
        f"Completed historical option chain ingestion for {ticker} "
        f"from {start_date} to {end_date}. Total new quotes ingested: {total_quotes_ingested}."
    )
    return total_quotes_ingested

# Example usage (for testing within the module)
if __name__ == "__main__":
    import logging
    # Ensure setup_logging is correctly imported if it's in core.logging_config
    try:
        from core.logging_config import configure_logging # Corrected name
        configure_logging() # Configure logging # Corrected name
    except ImportError:
        logging.basicConfig(level=logging.INFO)
        logging.warning("core.logging_config.configure_logging not found, using basicConfig.")

    async def main_test():
        # Test fetching
        sample_ticker = "AAPL"
        sample_date = date(2024, 7, 12) # A recent past Friday
        target_provider = "yfinance" # Changed to yfinance
        logger.info(f"--- Running test: fetching data for {sample_ticker} on {sample_date} using provider {target_provider} ---")
        df = await get_historical_option_chain_for_date(sample_ticker, sample_date, provider=target_provider)
        if df is not None:
            logger.info(f"Fetched {len(df)} rows for {sample_ticker} on {sample_date}")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info("Head of DataFrame:")
            # Convert DataFrame to string for logging to avoid truncation issues with default print
            logger.info(f"\n{df.head().to_string()}")
            # Test transformation (will be very basic until we know columns)
            # transformed = await transform_openbb_chain_data(df, sample_ticker, sample_date, "cboe")
            # if transformed is not None:
            #     logger.info("Transformed data (head):")
            #     logger.info(f"\n{transformed.head().to_string()}")
        else:
            logger.warning(f"No data fetched for {sample_ticker} on {sample_date}.")

        # Test full ingestion period (requires DB setup and models uncommented)
        # await ingest_historical_chains_for_ticker_period(
        #     ticker="MSFT",
        #     start_date=date(2023, 12, 1),
        #     end_date=date(2023, 12, 5) # Short period
        # )

    asyncio.run(main_test()) 