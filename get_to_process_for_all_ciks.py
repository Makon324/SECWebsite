import asyncio
import asyncpg
import logging
import os
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


async def process_single_cik(cik: str, scraper: SECScraper, db: DatabaseConnector, pool) -> Tuple[int, int]:
    """Process a single CIK and insert unprocessed filings directly to to_process"""
    async with pool.acquire() as conn:
        try:
            filings: List[FilingInfo] = await scraper.get_filings(cik)
            found_count = len(filings)

            if not filings:
                logger.info(f"CIK {cik}: No Form 4 filings found")
                return 0, 0

            processed_count = await db.insert_to_process(conn, filings)

            logger.info(f"CIK {cik}: Found: {found_count} filings, added: {processed_count}")
            return found_count, processed_count

        except Exception as e:
            logger.error(f"Failed to process CIK {cik}: {str(e)}")
            return 0, 0


async def worker(worker_id: int, q: asyncio.Queue, scraper: SECScraper, db: DatabaseConnector, pool: asyncpg.Pool) -> \
Tuple[int, int, int]:
    """Worker process to handle CIKs from the queue"""
    ciks_processed = 0
    total_found = 0
    total_added = 0

    while True:
        cik = await q.get()
        if cik is None:
            q.task_done()
            break

        try:
            found_count, added_count = await process_single_cik(cik, scraper, db, pool)
            ciks_processed += 1
            total_found += found_count
            total_added += added_count

            logger.debug(f"Worker {worker_id}: Processed CIK {cik} - Found: {found_count}, Added: {added_count}")

        except Exception as e:
            logger.error(f"Worker {worker_id}: Failed to process CIK {cik}: {str(e)}")
        finally:
            q.task_done()

    logger.info(
        f"Worker {worker_id}: Completed - Processed {ciks_processed} CIKs, Found {total_found} filings, Added {total_added} filings")
    return ciks_processed, total_found, total_added


async def main():
    """Main function to orchestrate the processing of all CIKs"""
    # Create database pool with proper context management
    async with asyncpg.create_pool(dsn=DSN, min_size=20, max_size=50) as pool:
        db = DatabaseConnector()
        async with SECScraper(user_agent=USER_AGENT) as scraper:
            try:
                ciks: List[str] = await scraper.get_ciks()
                total_ciks = len(ciks)
                logger.info("Retrieved %d CIKs", total_ciks)

                if not ciks:
                    logger.warning("No CIKs retrieved, exiting")
                    return

                # Create queue
                q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

                async def producer():
                    """Producer to feed CIKs into the queue"""
                    for cik in ciks:
                        await q.put(cik)
                    # Add sentinels to shut down workers
                    for _ in range(WORKERS):
                        await q.put(None)
                    logger.info("Producer finished queueing all CIKs")

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
                total_ciks_processed = 0
                total_filings_found = 0
                total_filings_added = 0

                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Worker {i} failed with exception: {result}")
                    elif isinstance(result, tuple):
                        ciks_processed, found, added = result
                        total_ciks_processed += ciks_processed
                        total_filings_found += found
                        total_filings_added += added

                logger.info("Processing complete - "
                            f"CIKs processed: {total_ciks_processed}/{total_ciks}, "
                            f"Total filings found: {total_filings_found}, "
                            f"Total filings added: {total_filings_added}")

            except Exception as e:
                logger.error(f"Failed in main process: {str(e)}")
                raise


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    logger.info("Total execution time: %.2f seconds", end_time - start_time)