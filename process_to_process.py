import asyncio
import asyncpg
import logging
import os
import pandas as pd
from typing import List, Tuple
import time

from SECscraper import SECScraper, FilingInfo
from DatabaseConnector import DatabaseConnector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DSN = os.environ.get('DATABASE_URL')
USER_AGENT = 'Makon324/test/makon324@yahoo.com'

WORKERS = 30
QUEUE_MAXSIZE = 1000


async def process_single_filing(filing: FilingInfo, scraper: SECScraper, db: DatabaseConnector, pool):
    """Process a single filing"""
    async with pool.acquire() as conn:
        try:
            df = await scraper.scrape_filing(filing)
            # Pass the entire filing object instead of just accession_number
            await db.insert_dataframe(conn, filing, df)

        except Exception as e:
            logger.error(f"Failed to process filing: {filing.accession_number}, error: {e}")
            return


async def worker(
    worker_id: int,
    q: asyncio.Queue[FilingInfo | None],
    scraper: SECScraper,
    db: DatabaseConnector,
    pool: asyncpg.Pool
) -> int:
    """Process filings from the queue until a sentinel is received.

    Args:
        worker_id: Unique identifier for the worker.
        q: Queue containing FilingInfo objects or None to stop.
        scraper: SEC scraper instance.
        db: Database connector.
        pool: Connection pool.

    Returns:
        Number of successfully processed filings.
    """
    filings_processed = 0
    while True:
        try:
            filing = await asyncio.wait_for(q.get(), timeout=30.0)  # Prevent hangs
            if filing is None:
                break
            await process_single_filing(filing, scraper, db, pool)
            filings_processed += 1
            logger.debug("Worker %d: Processed filing %s", worker_id, filing.accession_number)
        except asyncio.TimeoutError:
            logger.warning("Worker %d: Queue timeout, checking for shutdown", worker_id)
            continue
        except Exception as e:
            logger.error("Worker %d: Error processing filing: %s", worker_id, e, exc_info=True)
        finally:
            q.task_done()
    logger.info("Worker %d: Completed - Processed %d filings", worker_id, filings_processed)
    return filings_processed


async def main():
    """Main function to orchestrate the processing of all filings"""
    # Create database pool with proper context management
    async with asyncpg.create_pool(dsn=DSN, min_size=20, max_size=50) as pool:
        db = DatabaseConnector()
        async with SECScraper(user_agent=USER_AGENT) as scraper:
            try:
                # Create queue
                q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

                async def producer():
                    """Producer to feed FilingsInfo into the queue"""
                    while True:
                        async with pool.acquire() as conn:
                            filings: FilingInfo = await db.get_to_process_filings(conn, 500)
                        for filing in filings:
                            await q.put(filing)
                        if len(filings) < 500:
                            break

                    # Add sentinels to shut down workers
                    for _ in range(WORKERS):
                        await q.put(None)
                    logger.info("Producer finished queueing all to_process")

                # Start workers and producer
                workers = [asyncio.create_task(worker(i, q, scraper, db, pool))
                           for i in range(WORKERS)]
                producer_task = asyncio.create_task(producer())

                # Wait for all tasks to complete
                await q.join()
                await producer_task

                # Gather results from workers
                results = await asyncio.gather(*workers, return_exceptions=True)

                # Process results
                pass

            except Exception as e:
                logger.error(f"Failed in main process: {str(e)}")
                raise


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    logger.info("Total execution time: %.2f seconds", end_time - start_time)