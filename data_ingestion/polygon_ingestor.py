import asyncio
import logging
import os
from datetime import date, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

from polygon import RESTClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import select, func
import pandas as pd

from core.config import Config
from core.database import get_db
from core.db_models import OptionContract, OptionQuote

logger = logging.getLogger(__name__)

POLYGON_API_KEY = Config.POLYGON_API_KEY

# Polygon.io free tier: 5 calls/minute.
# This semaphore limits concurrent outgoing API requests.
# Actual rate limiting over time (e.g., 5 per 60s) needs careful handling of delays.
RATE_LIMIT_SEMAPHORE = asyncio.Semaphore(5) 
# Delay after each API call batch (list_contracts) or individual call (get_aggs)
# Helps in not overwhelming the semaphore or hitting overall minute limits too fast.
# 5 calls/min = 1 call every 12 seconds on average.
REQUEST_DELAY_AFTER_CALL = 12 # seconds


async def list_option_contracts(
    api_client: RESTClient,
    underlying_ticker: str,
    as_of_date: date
) -> List[Dict[str, Any]]:
    """
    Fetches a list of option contracts for a given underlying ticker, active as of a specific date.
    Uses Polygon's /v3/reference/options/contracts endpoint.
    """
    contracts: List[Dict[str, Any]] = []
    processed_tickers = set() # To store unique option symbols

    polygon_ticker = underlying_ticker.upper()
    if polygon_ticker.startswith("^") or polygon_ticker in ["SPX", "VIX"]:
        polygon_ticker = f"I:{polygon_ticker.replace('^','')}"

    params = {
        "underlying_ticker": polygon_ticker,
        "as_of": as_of_date.strftime("%Y-%m-%d"),
        "limit": 1000,  # Max limit per Polygon API page
        "expired": False 
    }
    logger.info(f"Listing option contracts for {polygon_ticker} as of {as_of_date} with params: {params}")

    try:
        all_api_results = []

        # Helper function to be run in a thread, as list_options_contracts with paginate=True is a generator
        # that makes multiple blocking I/O calls.
        def _fetch_all_contracts_sync():
            temp_results = []
            # Iterate directly; client handles pagination internally by yielding from next_url.
            for contract_data in api_client.list_options_contracts(**params):
                temp_results.append(contract_data)
                if len(temp_results) >= 20000:
                    logger.warning(
                        f"Safety limit of 20000 contracts reached for {polygon_ticker} on {as_of_date}. "
                        "Consider if query is too broad or if this is expected."
                    )
                    break
            return temp_results

        async with RATE_LIMIT_SEMAPHORE: # Acquire semaphore before starting (potentially multiple) API calls
            all_api_results = await asyncio.to_thread(_fetch_all_contracts_sync)
            # Apply delay after the entire paginated operation, as it might have made multiple calls.
            await asyncio.sleep(REQUEST_DELAY_AFTER_CALL) 

        logger.info(f"Polygon API returned {len(all_api_results)} contract items for {polygon_ticker} as of {as_of_date}.")

        for contract_data_obj in all_api_results:
            contract_attrs = [
                'ticker', 'underlying_ticker', 'cfi', 'contract_type', 
                'exercise_style', 'expiration_date', 'primary_exchange', 
                'shares_per_contract', 'strike_price', 'open_interest', 
                'correction'
            ]
            contract_dict = {}
            valid_contract = True
            for attr_name in contract_attrs:
                if hasattr(contract_data_obj, attr_name):
                    value = getattr(contract_data_obj, attr_name)
                    if isinstance(value, date): # Ensure date objects are stringified
                        contract_dict[attr_name] = str(value)
                    else:
                        contract_dict[attr_name] = value
                else:
                    # Optional fields can be None
                    contract_dict[attr_name] = None
                    # Log if a typically essential one is missing, but don't skip unless it's 'ticker'
                    if attr_name == 'ticker':
                        logger.error(f"Attribute 'ticker' missing from contract_data_obj. Skipping contract: {contract_data_obj}")
                        valid_contract = False
                        break
                    elif attr_name in ['underlying_ticker', 'expiration_date', 'strike_price', 'contract_type']:
                         logger.warning(f"Attribute '{attr_name}' missing from contract_data_obj for potential ticker {getattr(contract_data_obj, 'ticker', 'UNKNOWN_TICKER')}. Field will be None.")
            
            if not valid_contract:
                continue
            
            option_symbol = contract_dict.get("ticker")
            if option_symbol and option_symbol not in processed_tickers:
                contracts.append(contract_dict)
                processed_tickers.add(option_symbol)
            elif not option_symbol:
                logger.warning(f"Contract data missing 'ticker' (option symbol): {contract_dict}")

    except Exception as e:
        logger.error(f"Error listing contracts for {polygon_ticker} as of {as_of_date}: {e}", exc_info=True)
        return [] 

    logger.info(f"Successfully processed {len(contracts)} unique option contracts for {polygon_ticker} as of {as_of_date}.")
    return contracts


async def fetch_daily_ohlc_for_contract(
    api_client: RESTClient,
    option_symbol: str,
    as_of_date: date
) -> Optional[Dict[str, Any]]:
    """
    Fetches the 1-day OHLCV bar for a given option contract ticker on a specific date.
    """
    date_str = as_of_date.strftime("%Y-%m-%d")
    logger.debug(f"Fetching OHLC for {option_symbol} on {date_str}")

    try:
        async with RATE_LIMIT_SEMAPHORE: # Acquire semaphore for this single API call
            # Wrap the synchronous API call in asyncio.to_thread
            aggs_result_list = await asyncio.to_thread(
                api_client.get_aggs,
                ticker=option_symbol,
                multiplier=1,
                timespan="day",
                from_=date_str,
                to=date_str,
                raw=False,  # Get parsed Pydantic models or dicts
                limit=1     # We expect only one aggregate for the day
            )
            await asyncio.sleep(REQUEST_DELAY_AFTER_CALL) # Delay after the API call

        if aggs_result_list and len(aggs_result_list) > 0:
            ohlc_data_obj = aggs_result_list[0]
            
            # Attributes from Polygon Agg model: open, high, low, close, volume, vwap, timestamp, transactions, otc, (and 'oi' for options)
            # We need to map them to keys expected by transform_polygon_ohlc_data: 'open', 'high', 'low', 'close', 'volume', 'oi'
            
            ohlc_dict = {}
            # Direct mapping for attributes that share names with dict keys
            for key in ['open', 'high', 'low', 'close', 'volume', 'oi']:
                if hasattr(ohlc_data_obj, key):
                    ohlc_dict[key] = getattr(ohlc_data_obj, key)
                else:
                    ohlc_dict[key] = None # Set to None if attribute doesn't exist

            # Example for attributes with different names, if any were needed:
            # if hasattr(ohlc_data_obj, 'v'): # 'v' is volume in Polygon Agg
            #     ohlc_dict['volume'] = getattr(ohlc_data_obj, 'v')
            # else:
            #     ohlc_dict['volume'] = None
            # Current direct mapping for 'volume' and 'oi' should work if attributes exist with those names.

            logger.debug(f"Successfully fetched OHLC for {option_symbol} on {date_str}: {ohlc_dict}")
            return ohlc_dict
        else:
            logger.info(f"No OHLC data found for {option_symbol} on {date_str}.")
            return None
    except Exception as e: # Consider catching specific polygon.exceptions like NotFoundError
        logger.error(f"Error fetching OHLC for {option_symbol} on {date_str}: {e}", exc_info=True)
        return None

async def transform_polygon_contract_data(
    contract_info: Dict[str, Any],
    underlying_input_ticker: str 
) -> Optional[Dict[str, Any]]:
    """Transforms Polygon contract data to our OptionsContract schema fields."""
    try:
        # Polygon contract data is now expected to be flat in contract_info
        
        option_type_str = contract_info.get("contract_type")
        expiration_date_str = contract_info.get("expiration_date") # Already stringified if from date object
        strike_price_val = contract_info.get("strike_price")
        open_interest_val = contract_info.get("open_interest") # Top-level

        if not all([option_type_str, expiration_date_str, strike_price_val is not None]):
            logger.warning(f"Contract data missing critical fields (type, expiry, strike): {contract_info} for {contract_info.get('ticker')}")
            return None

        # Normalize underlying ticker (remove "I:" if present for storage)
        underlying_polygon_ticker = contract_info.get("underlying_ticker", "").upper()
        db_underlying_ticker = underlying_polygon_ticker[2:] if underlying_polygon_ticker.startswith("I:") else underlying_polygon_ticker

        transformed = {
            "contract_symbol": contract_info.get("ticker"), 
            "underlying_ticker": db_underlying_ticker,
            "option_type": option_type_str.lower(), 
            "expiration_date": pd.to_datetime(expiration_date_str).date(),
            "strike_price": float(strike_price_val),
            "open_interest_at_listing": int(open_interest_val) if open_interest_val is not None else None,
            "cfi_code": contract_info.get("cfi"),
            "exercise_style": contract_info.get("exercise_style"),
            "primary_exchange": contract_info.get("primary_exchange"),
            "shares_per_contract": int(contract_info.get("shares_per_contract")) if contract_info.get("shares_per_contract") is not None else None,
        }
        return transformed
    except KeyError as e:
        logger.error(f"KeyError transforming contract data {contract_info.get('ticker','UNKNOWN')}: missing {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error transforming contract data {contract_info.get('ticker','UNKNOWN')}: {e}", exc_info=True)
        return None


async def transform_polygon_ohlc_data(
    ohlc_info: Dict[str, Any],
    contract_symbol: str, 
    as_of_date: date,
    open_interest_from_contract_details: Optional[int] = None # Renamed for clarity
) -> Optional[Dict[str, Any]]:
    """Transforms Polygon OHLC data to our OptionsQuote schema fields."""
    try:
        # Polygon EOD agg fields: o, h, l, c, v, vw (vwap), n (number of trades)
        # Polygon option EOD aggs might also have 'oi' for open interest directly for that day
        daily_open_interest = ohlc_info.get("oi") 

        transformed = {
            "contract_symbol": contract_symbol, 
            "quote_date": as_of_date,
            "open": float(ohlc_info["open"]) if ohlc_info.get("open") is not None else None,
            "high": float(ohlc_info["high"]) if ohlc_info.get("high") is not None else None,
            "low": float(ohlc_info["low"]) if ohlc_info.get("low") is not None else None,
            "close": float(ohlc_info["close"]) if ohlc_info.get("close") is not None else None,
            "volume": int(ohlc_info["volume"]) if ohlc_info.get("volume") is not None else None,
            # Prioritize 'oi' from the daily bar if available, else use from contract details
            "open_interest": int(daily_open_interest) if daily_open_interest is not None else open_interest_from_contract_details,
            "implied_volatility": None, # Not in Polygon EOD aggs
            "bid": None, # Not in EOD aggs
            "ask": None, # Not in EOD aggs
            "data_source": "polygon_io_eod",
            "vwap": float(ohlc_info.get("vwap")) if ohlc_info.get("vwap") is not None else None,
            # "transactions": int(ohlc_info.get("transactions")) if ohlc_info.get("transactions") is not None else None # Polygon's 'n' - if schema has it
        }
        return transformed
    except KeyError as e:
        logger.error(f"KeyError transforming OHLC data for {contract_symbol}: missing {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error transforming OHLC data for {contract_symbol}: {e}", exc_info=True)
        return None


async def persist_polygon_data_day(session: AsyncSession, contracts_data: List[Dict[str, Any]], quotes_data: List[Dict[str, Any]]) -> tuple[int, int]:
    """
    Persists transformed contract and quote data to the database for a specific day.
    Uses INSERT ... ON CONFLICT DO UPDATE for contracts and quotes.
    Returns the number of contracts and quotes successfully upserted.
    """
    contracts_upserted_count = 0
    quotes_upserted_count = 0

    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY not configured. Skipping persistence.")
        # Create an issue with the mcp_github_create_issue tool
        # This should be handled by the workflow engine if it's a persistent issue
        return 0,0

    # Persist OptionContracts
    if contracts_data:
        stmt_contracts = pg_insert(OptionContract).values(contracts_data)
        # Define what to do on conflict: update existing record
        # Assuming 'contract_symbol' is the unique identifier for an OptionContract.
        # If other fields define uniqueness (like underlying, expiry, strike, type), use that constraint.
        # The model has uq_option_contract_details and contract_symbol is unique.
        # Let's use contract_symbol as the conflict target.
        stmt_contracts = stmt_contracts.on_conflict_do_update(
            index_elements=[OptionContract.contract_symbol],
            set_={
                # Define fields to update on conflict. For contracts, these are mostly static.
                # If a contract symbol exists, its core details (expiry, strike, type) shouldn't change.
                # Perhaps only 'updated_at' if we want to track last seen.
                # For now, if it exists, we might not need to update much.
                # Let's ensure underlying_ticker is updated if it somehow changed (though unlikely for a fixed symbol)
                "underlying_ticker": stmt_contracts.excluded.underlying_ticker,
                "option_type": stmt_contracts.excluded.option_type,
                "expiration_date": stmt_contracts.excluded.expiration_date,
                "strike_price": stmt_contracts.excluded.strike_price,
                "updated_at": func.now(),
            },
        )
        result_contracts = await session.execute(stmt_contracts)
        contracts_upserted_count = result_contracts.rowcount
        logger.info(f"Upserted {contracts_upserted_count} option contracts.")

    # Persist OptionQuotes
    # We need contract_id for quotes. We should fetch or have contract_ids from the contract persistence step.
    # This implies a two-step process:
    # 1. Upsert contracts, get their DB IDs.
    # 2. Map these IDs to the quotes and then upsert quotes.

    # Let's adjust the flow:
    # Upsert contracts and retrieve their IDs along with their symbols.
    persisted_contracts_map = {} # contract_symbol -> db_id
    if contracts_data:
        # contracts_data now comes directly from transform_polygon_contract_data
        # and should contain all necessary fields for OptionContract model, including new metadata.
        
        # Ensure 'open_interest_at_listing' is not in the dicts meant for OptionContract table directly
        # as it was popped in ingest_historical_chain_for_day and used for quotes.
        # This shouldn't be an issue if transform_polygon_contract_data is the sole source and it doesn't include it.
        # However, the original contracts_data list from the caller might still have it before transform.
        # The current transform_polygon_contract_data returns a dict that already has the correct fields for the model.

        # No need to recreate option_contract_values; contracts_data should be directly usable.
        # The fields in contracts_data should match the OptionContract model columns.

        stmt_contracts_insert = pg_insert(OptionContract).values(contracts_data) # Use contracts_data directly
        stmt_contracts_insert = stmt_contracts_insert.on_conflict_do_update(
            index_elements=[OptionContract.contract_symbol],
            set_={
                "underlying_ticker": stmt_contracts_insert.excluded.underlying_ticker,
                "option_type": stmt_contracts_insert.excluded.option_type,
                "expiration_date": stmt_contracts_insert.excluded.expiration_date,
                "strike_price": stmt_contracts_insert.excluded.strike_price,
                "cfi_code": stmt_contracts_insert.excluded.cfi_code, # Add new fields to update
                "exercise_style": stmt_contracts_insert.excluded.exercise_style,
                "primary_exchange": stmt_contracts_insert.excluded.primary_exchange,
                "shares_per_contract": stmt_contracts_insert.excluded.shares_per_contract,
                "updated_at": func.now(),
            }
        )
        stmt_contracts_insert = stmt_contracts_insert.returning(OptionContract.id, OptionContract.contract_symbol)
        result_contracts = await session.execute(stmt_contracts_insert)
        
        persisted_contract_records = result_contracts.fetchall()
        contracts_upserted_count = len(persisted_contract_records) # More accurate count
        logger.info(f"Upserted and fetched IDs for {contracts_upserted_count} option contracts.")
        for record in persisted_contract_records:
            persisted_contracts_map[record.contract_symbol] = record.id
        
        # If some contracts already existed and were not part of the RETURNING (because they were not updated due to no change or specific conflict handling)
        # we might need to fetch IDs for all relevant contract_symbols if persisted_contracts_map is not complete.
        # For simplicity, let's assume RETURNING gives us what we need for newly upserted/updated ones.
        # If a contract wasn't in contracts_data but its quote is, we have an issue.
        # The quotes_data should only be for contracts processed in this run.

        # Fetch IDs for any symbols in quotes_data that might not have been returned
        # (e.g. if they existed and on_conflict_do_update didn't change them, rowcount might be 0 for them)
        # This ensures we have IDs for all contracts for which we have quotes.
        all_quote_contract_symbols = {q['contract_symbol'] for q in quotes_data}
        missing_symbols_from_map = all_quote_contract_symbols - set(persisted_contracts_map.keys())

        if missing_symbols_from_map:
            select_stmt = select(OptionContract.id, OptionContract.contract_symbol).where(OptionContract.contract_symbol.in_(missing_symbols_from_map))
            result_existing_contracts = await session.execute(select_stmt)
            for record in result_existing_contracts.fetchall():
                 persisted_contracts_map[record.contract_symbol] = record.id
            logger.info(f"Fetched IDs for {len(missing_symbols_from_map)} additional existing contracts relevant to quotes.")


    if quotes_data:
        # Prepare quotes data with contract_id
        quotes_to_insert = []
        for quote_dict in quotes_data:
            contract_symbol = quote_dict["contract_symbol"]
            contract_id = persisted_contracts_map.get(contract_symbol)
            if contract_id:
                # Remove contract_symbol as OptionQuote uses contract_id
                quote_values = {k: v for k, v in quote_dict.items() if k != "contract_symbol"}
                quote_values["contract_id"] = contract_id
                # Ensure all fields match OptionQuote model
                # 'vwap' is not in OptionQuote. Remove or add to model. For now, remove.
                if "vwap" in quote_values:
                    del quote_values["vwap"] 
                quotes_to_insert.append(quote_values)
            else:
                logger.warning(f"Could not find contract_id for symbol {contract_symbol}. Skipping quote.")
        
        if quotes_to_insert:
            stmt_quotes = pg_insert(OptionQuote).values(quotes_to_insert)
            # Unique constraint for OptionQuote: uq_option_quote_source_date (contract_id, quote_date, data_source)
            stmt_quotes = stmt_quotes.on_conflict_do_update(
                index_elements=[OptionQuote.contract_id, OptionQuote.quote_date, OptionQuote.data_source],
                set_={
                    "open": stmt_quotes.excluded.open,
                    "high": stmt_quotes.excluded.high,
                    "low": stmt_quotes.excluded.low,
                    "close": stmt_quotes.excluded.close,
                    "volume": stmt_quotes.excluded.volume,
                    "open_interest": stmt_quotes.excluded.open_interest,
                    "implied_volatility": stmt_quotes.excluded.implied_volatility,
                    "bid": stmt_quotes.excluded.bid,
                    "ask": stmt_quotes.excluded.ask,
                    "delta": stmt_quotes.excluded.delta, # If we add greeks later
                    "gamma": stmt_quotes.excluded.gamma,
                    "theta": stmt_quotes.excluded.theta,
                    "vega": stmt_quotes.excluded.vega,
                    "rho": stmt_quotes.excluded.rho,
                    "fetched_at": func.now(), # Always update fetched_at
                }
            )
            result_quotes = await session.execute(stmt_quotes)
            quotes_upserted_count = result_quotes.rowcount
            logger.info(f"Upserted {quotes_upserted_count} option quotes.")

    await session.commit()
    return contracts_upserted_count, quotes_upserted_count


async def ingest_historical_chain_for_day(
    underlying_ticker: str,
    as_of_date: date
):
    """
    Orchestrates the ingestion of option chain data for a given underlying ticker and date from Polygon.io.
    1. Lists all relevant option contracts.
    2. For each contract, fetches its EOD OHLC data.
    3. Transforms contract and OHLC data.
    4. Persists data to the database.
    """
    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY not set. Cannot ingest from Polygon.io.")
        # Potentially raise an error or notify, depending on desired behavior
        return

    # Initialize Polygon RESTClient.
    # Note: RESTClient is synchronous. Calls need to be wrapped with asyncio.to_thread.
    # The client manages its own httpx.Client or requests.Session internally.
    # No explicit async close seems to be available or needed for RESTClient if used per operation.
    # If we were to use it across many top-level calls, we might initialize once.
    # For a single run of this function, creating it here is fine.
    
    # client = RESTClient(POLYGON_API_KEY) # This should be outside if get_db is called inside loop

    all_transformed_contracts = []
    all_transformed_quotes = []
    
    processed_contract_symbols = set() # To avoid duplicate processing if API gives redundant contract info

    # The client should be created once for the ingestion process
    # However, if this function is the sole entry point for a "day's ingestion", 
    # creating it here and passing it around or using it directly in sub-functions is okay.
    # Let's keep it simple and instantiate it for the scope of this function.
    # The sub-functions list_option_contracts and fetch_daily_ohlc_for_contract already expect an api_client.
    
    logger.info(f"Starting Polygon.io ingestion for {underlying_ticker} on {as_of_date}.")

    # Using a try/finally for client closing if it were async and had a close method.
    # For RESTClient, context management or explicit close was problematic.
    # Create client directly.
    client = RESTClient(POLYGON_API_KEY)
    try:
        raw_contracts = await list_option_contracts(client, underlying_ticker, as_of_date)

        if not raw_contracts:
            logger.info(f"No option contracts found for {underlying_ticker} on {as_of_date}. Ingestion complete.")
            return

        logger.info(f"Found {len(raw_contracts)} raw contract items. Processing and fetching OHLC...")

        # TEMP: Limit for testing - uncomment below and comment out the original loop line
        # contracts_to_process = raw_contracts[:2] # Process first 2 for example
        # logger.info(f"TEMPORARILY CAPPED: Processing only the first {len(contracts_to_process)} contracts for testing.")
        # for i, raw_contract_info in enumerate(contracts_to_process):
        # Original loop (process all contracts):
        for i, raw_contract_info in enumerate(raw_contracts):
            contract_symbol = raw_contract_info.get("ticker")
            if not contract_symbol or contract_symbol in processed_contract_symbols:
                if not contract_symbol:
                    logger.warning(f"Skipping raw contract due to missing ticker: {raw_contract_info}")
                # else:
                    # logger.debug(f"Skipping already processed contract symbol: {contract_symbol}") # Can be noisy
                continue
            
            logger.debug(f"Processing contract {i+1}/{len(raw_contracts)}: {contract_symbol}")

            # 1. Transform contract data
            # The 'underlying_input_ticker' is the ticker symbol the user initially provided for the chain.
            transformed_contract = await transform_polygon_contract_data(raw_contract_info, underlying_ticker)
            
            if not transformed_contract:
                logger.warning(f"Failed to transform contract data for {contract_symbol}. Skipping.")
                continue

            # Extract open interest from contract details to pass to OHLC transform
            # The key is "open_interest_at_listing" in the transformed_contract dict.
            open_interest_from_listing = transformed_contract.pop("open_interest_at_listing", None)

            # Add to list for bulk persistence
            all_transformed_contracts.append(transformed_contract)
            processed_contract_symbols.add(contract_symbol) # Mark as processed based on its data being added

            # 2. Fetch OHLC data for this contract on the as_of_date
            # Note: as_of_date for list_option_contracts ensures contracts are *active* on this date.
            # For OHLC, we fetch for this *exact* date.
            ohlc_data = await fetch_daily_ohlc_for_contract(client, contract_symbol, as_of_date)

            if ohlc_data:
                # 3. Transform OHLC data
                transformed_quote = await transform_polygon_ohlc_data(
                    ohlc_data,
                    contract_symbol,
                    as_of_date,
                    open_interest_from_contract_details=open_interest_from_listing # Pass OI from contract listing
                )
                if transformed_quote:
                    all_transformed_quotes.append(transformed_quote)
                else:
                    logger.warning(f"Failed to transform OHLC data for {contract_symbol} on {as_of_date}.")
            else:
                logger.info(f"No OHLC data found for {contract_symbol} on {as_of_date}. Only contract will be persisted if new.")
        
        logger.info(f"Processed {len(all_transformed_contracts)} contracts and {len(all_transformed_quotes)} quotes for {underlying_ticker} on {as_of_date}.")

        # 4. Persist data
        if all_transformed_contracts or all_transformed_quotes:
            async for session in get_db(): # Get a DB session
                try:
                    contracts_count, quotes_count = await persist_polygon_data_day(session, all_transformed_contracts, all_transformed_quotes)
                    logger.info(f"Successfully upserted {contracts_count} contracts and {quotes_count} quotes for {underlying_ticker} on {as_of_date}.")
                    await session.commit() # Commit if persist_polygon_data_day doesn't
                except Exception as e:
                    logger.error(f"Database persistence error for {underlying_ticker} on {as_of_date}: {e}", exc_info=True)
                    await session.rollback() 
                finally:
                    # The get_db context manager should handle session closing
                    pass 
        else:
            logger.info(f"No data to persist for {underlying_ticker} on {as_of_date}.")
            
    except Exception as e:
        logger.error(f"Overall ingestion error for {underlying_ticker} on {as_of_date}: {e}", exc_info=True)
        # If client had an explicit close method that needs to be called even on error:
        # if client and hasattr(client, 'close'): client.close() # or await client.close() if async
    finally:
        # If RESTClient had an explicit close method, it would be called here.
        # Based on previous observations and user summary, it seems it might not, relying on internal httpx client management.
        logger.debug(f"RESTClient for {underlying_ticker} on {as_of_date} processing finished.")

    logger.info(f"Polygon.io ingestion finished for {underlying_ticker} on {as_of_date}.")


async def main_test_polygon():
    """Main function for testing Polygon.io ingestion logic."""
    # Ensure logging is configured for testing
    # This might be better handled by the script calling this test function
    try:
        from core.logging_config import configure_logging
        # Set to DEBUG for more verbose test output
        # configure_logging(log_level=os.getenv("LOG_LEVEL_TEST", "DEBUG")) 
        # For direct run, let's use a simpler config to avoid issues if core.logging_config is complex
        logging.basicConfig(level=os.getenv("LOG_LEVEL_TEST", "INFO"), 
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.info("Basic logging configured for main_test_polygon.")

    except ImportError:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.warning("core.logging_config.configure_logging not found or failed, using basicConfig for test.")


    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY environment variable not set. Cannot run test.")
        return

    # Test parameters
    # test_underlying_ticker = "SPY"  # AAPL, SPY, I:SPX
    test_underlying_ticker = "UNH" # Using a common stock ticker
    # test_as_of_date = date(2024, 7, 12) # A recent date with likely options activity
    test_as_of_date = date.today() - timedelta(days=1) # Test with yesterday's EOD data
    
    # Ensure date is a weekday
    while test_as_of_date.weekday() >= 5: # 5 for Saturday, 6 for Sunday
        logger.info(f"Test date {test_as_of_date} is a weekend. Adjusting to previous Friday.")
        test_as_of_date -= timedelta(days=1)
    logger.info(f"Using effective test_as_of_date: {test_as_of_date}") # Added log line
    
    logger.info(f"--- Starting Polygon Ingestion Test for {test_underlying_ticker} as of {test_as_of_date} ---")

    # client = RESTClient(POLYGON_API_KEY) # Client is now managed within ingest_historical_chain_for_day

    # Test: ingest_historical_chain_for_day
    try:
        await ingest_historical_chain_for_day(
            underlying_ticker=test_underlying_ticker,
            as_of_date=test_as_of_date
        )
        logger.info(f"--- Polygon Ingestion Test completed for {test_underlying_ticker} as of {test_as_of_date} ---")
    except Exception as e:
        logger.error(f"Error during main_test_polygon for {test_underlying_ticker} on {test_as_of_date}: {e}", exc_info=True)
    # finally:
        # if client and hasattr(client, 'close') and asyncio.iscoroutinefunction(client.close):
        #     await client.close()
        # elif client and hasattr(client, 'close'): # For synchronous close like in httpx.Client used by RESTClient context manager
        #     client.close() # RESTClient as context manager handles this
        # logger.info("Polygon client closed if applicable.")


if __name__ == "__main__":
    # This allows running the test directly: python -m data_ingestion.polygon_ingestor
    # Make sure .env file with POLYGON_API_KEY is in the root directory when running like this,
    # or that the key is otherwise available in the environment.
    # Example: PYTHONPATH=. python swing-bot/data_ingestion/polygon_ingestor.py
    # Or from `swing-bot` dir: python -m data_ingestion.polygon_ingestor
    
    # Load .env file if present, for direct script execution convenience
    from dotenv import load_dotenv
    # Assuming .env is in the project root (e.g., tutorials/swing-bot/.env or tutorials/.env)
    # Determine project root. __file__ is swing-bot/data_ingestion/polygon_ingestor.py
    project_root = Path(__file__).resolve().parent.parent.parent # Gets to tutorials/
    dotenv_path_swing_bot = Path(__file__).resolve().parent.parent / '.env' # swing-bot/.env
    dotenv_path_project_root = project_root / '.env' # tutorials/.env (if workspace is tutorials)

    if dotenv_path_swing_bot.exists():
        load_dotenv(dotenv_path=dotenv_path_swing_bot)
        print(f"Loaded .env from {dotenv_path_swing_bot}")
    elif dotenv_path_project_root.exists():
        load_dotenv(dotenv_path=dotenv_path_project_root)
        print(f"Loaded .env from {dotenv_path_project_root}")
    else:
        print("No .env file found at expected locations. Ensure POLYGON_API_KEY is set in environment.")
        
    # Re-check POLYGON_API_KEY after attempting to load .env
    POLYGON_API_KEY = Config.POLYGON_API_KEY or os.getenv("POLYGON_API_KEY")
    if not POLYGON_API_KEY:
        print("Error: POLYGON_API_KEY is not set after checking .env and Config. Exiting test.")
        exit(1)
    else:
        # Mask parts of the API key for logging, if it's printed or logged via Config initialization
        # Config itself doesn't print it, but good practice if we were to log it here.
        # print(f"POLYGON_API_KEY found: {...POLYGON_API_KEY[-4:] if POLYGON_API_KEY else 'Not set'}")
        pass 

    asyncio.run(main_test_polygon()) 