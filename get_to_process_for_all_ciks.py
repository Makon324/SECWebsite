import asyncio
import asyncpg
import logging
import os
from typing import List, Tuple
import time
from config import Config, setup_logging

from SECscraper import SECScraper, FilingInfo
from DatabaseConnector import DatabaseConnector

logger = logging.getLogger(__name__)


async def process_single_cik(
    cik: str,
    scraper: SECScraper,
    db: DatabaseConnector,
    pool: asyncpg.Pool
) -> Tuple[int, int]:
    """
    Process a single CIK by fetching its Form 4 filings and inserting
    unprocessed ones directly into the 'to_process' table.

    Args:
        cik: The Central Index Key to process.
        scraper: The SECScraper instance.
        db: The DatabaseConnector instance.
        pool: The asyncpg connection pool.

    Returns:
        A tuple of (filings_found_count, filings_added_count).
    """
    async with pool.acquire() as conn:
        try:
            filings: List[FilingInfo] = await scraper.get_filings(cik)
            found_count = len(filings)

            if not filings:
                logger.info(f"No Form 4 filings found for CIK {cik}")
                return 0, 0

            added_count = await db.insert_to_process(conn, filings)
            logger.info(f"Processed CIK {cik}: Found {found_count} filings, added {added_count}")
            return found_count, added_count

        except Exception as e:
            logger.error(f"Failed to process CIK {cik}: {str(e)}", exc_info=True)
            return 0, 0


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    scraper: SECScraper,
    db: DatabaseConnector,
    pool: asyncpg.Pool
) -> Tuple[int, int, int]:
    """
    Worker coroutine that processes CIKs from the queue.

    Args:
        worker_id: Unique identifier for the worker.
        queue: The asyncio.Queue containing CIKs to process.
        scraper: The SECScraper instance.
        db: The DatabaseConnector instance.
        pool: The asyncpg connection pool.

    Returns:
        A tuple of (ciks_processed, total_filings_found, total_filings_added).
    """
    ciks_processed = 0
    total_found = 0
    total_added = 0

    while True:
        cik = await queue.get()
        if cik is None:
            queue.task_done()
            break

        try:
            found_count, added_count = await process_single_cik(cik, scraper, db, pool)
            ciks_processed += 1
            total_found += found_count
            total_added += added_count
            logger.debug(f"Worker {worker_id} processed CIK {cik}: Found {found_count}, Added {added_count}")

        except Exception as e:
            logger.error(f"Worker {worker_id} failed to process CIK {cik}: {str(e)}", exc_info=True)
        finally:
            queue.task_done()

    logger.info(
        f"Worker {worker_id} completed: Processed {ciks_processed} CIKs, "
        f"Found {total_found} filings, Added {total_added} filings"
    )
    return ciks_processed, total_found, total_added


async def main() -> None:
    """
    Main coroutine to orchestrate the retrieval and processing of all CIKs.
    Fetches CIKs, queues them for workers, and aggregates results.
    """
    config = Config()
    start_time = time.time()

    async with asyncpg.create_pool(
        dsn=config.database_url,
        min_size=config.pool_min_size,
        max_size=config.pool_max_size
    ) as pool:
        db = DatabaseConnector()
        async with SECScraper() as scraper:
            try:
                ciks: List[str] = await scraper.get_ciks()
                total_ciks = len(ciks)
                if total_ciks == 0:
                    logger.warning("No CIKs retrieved, exiting early")
                    return

                logger.info(f"Retrieved {total_ciks} CIKs")

                queue: asyncio.Queue = asyncio.Queue(maxsize=config.queue_max_size)

                async def producer() -> None:
                    """Producer coroutine to feed CIKs into the queue."""
                    for cik in ciks:
                        await queue.put(cik)
                    for _ in range(config.num_workers):
                        await queue.put(None)
                    logger.info("Producer finished queueing all CIKs and sentinels")

                # Start producer and workers
                producer_task = asyncio.create_task(producer())
                worker_tasks = [
                    asyncio.create_task(worker(i, queue, scraper, db, pool))
                    for i in range(1, config.num_workers + 1)
                ]

                # Wait for queue to be fully processed
                await queue.join()
                await producer_task

                # Gather and process worker results
                results = await asyncio.gather(*worker_tasks, return_exceptions=True)

                total_ciks_processed = 0
                total_filings_found = 0
                total_filings_added = 0

                for i, result in enumerate(results, start=1):
                    if isinstance(result, Exception):
                        logger.error(f"Worker {i} failed with exception: {result}", exc_info=True)
                    elif isinstance(result, tuple):
                        ciks_proc, found, added = result
                        total_ciks_processed += ciks_proc
                        total_filings_found += found
                        total_filings_added += added

                logger.info(
                    f"Processing complete: CIKs processed {total_ciks_processed}/{total_ciks}, "
                    f"Total filings found: {total_filings_found}, "
                    f"Total filings added: {total_filings_added}"
                )

            except Exception as e:
                logger.error(f"Main process failed: {str(e)}", exc_info=True)
                raise

    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())