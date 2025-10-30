import asyncio
import asyncpg
import logging
import time
from typing import List

from SECscraper import SECScraper, FilingInfo
from DatabaseConnector import DatabaseConnector
from config import Config, setup_logging

logger = logging.getLogger(__name__)


async def process_single_filing(
        filing: FilingInfo,
        scraper: SECScraper,
        db: DatabaseConnector,
        pool: asyncpg.Pool
) -> None:
    """
    Process a single filing by scraping its data and inserting it into the database.

    Args:
        filing: The FilingInfo object to process.
        scraper: The SEC scraper instance.
        db: The database connector.
        pool: The connection pool.

    Raises:
        Exception: If processing or insertion fails.
    """
    async with pool.acquire() as conn:
        try:
            df = await scraper.scrape_filing(filing)
            await db.insert_dataframe(conn, filing, df)
            logger.info("Processed filing: %s", filing.accession_number)
        except Exception as e:
            logger.error("Failed to process filing %s: %s", filing.accession_number, e)
            raise


async def worker(
        worker_id: int,
        queue: asyncio.Queue,
        scraper: SECScraper,
        db: DatabaseConnector,
        pool: asyncpg.Pool
) -> int:
    """
    Worker coroutine that processes filings from the queue until a sentinel is received.

    Args:
        worker_id: Unique identifier for the worker.
        queue: Queue containing FilingInfo objects or None to stop.
        scraper: SEC scraper instance.
        db: Database connector.
        pool: Connection pool.

    Returns:
        Number of successfully processed filings.
    """
    filings_processed = 0
    while True:
        try:
            filing = await asyncio.wait_for(queue.get(), timeout=30.0)
            if filing is None:
                break
            await process_single_filing(filing, scraper, db, pool)
            filings_processed += 1
        except asyncio.TimeoutError:
            logger.warning("Worker %d: Queue timeout, checking for shutdown", worker_id)
            continue
        except Exception as e:
            logger.error("Worker %d: Error processing filing: %s", worker_id, e)
        finally:
            queue.task_done()

    logger.info("Worker %d completed. Processed %d filings.", worker_id, filings_processed)
    return filings_processed


async def producer(
        queue: asyncio.Queue,
        db: DatabaseConnector,
        pool: asyncpg.Pool
) -> None:
    """
    Producer coroutine that fetches batches of filings from the database and adds them to the queue.

    Args:
        queue: The queue to put FilingInfo objects into.
        db: The database connector.
        pool: The connection pool.
    """
    config = Config()
    while True:
        async with pool.acquire() as conn:
            filings: List[FilingInfo] = await db.get_to_process_filings(conn, config.batch_size)

        for filing in filings:
            await queue.put(filing)

        logger.info("Queued %d filings.", len(filings))

        if len(filings) < config.batch_size:
            break

    # Add sentinels for workers
    for _ in range(config.num_workers):
        await queue.put(None)

    logger.info("Producer finished queuing all filings.")


async def main() -> None:
    """
    Main coroutine to orchestrate the processing of all filings using asynchronous workers.
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
                queue: asyncio.Queue = asyncio.Queue(maxsize=config.queue_max_size)

                # Start producer
                producer_task = asyncio.create_task(producer(queue, db, pool))

                # Start workers
                worker_tasks = [
                    asyncio.create_task(worker(i, queue, scraper, db, pool))
                    for i in range(1, config.num_workers + 1)
                ]

                # Wait for queue to be fully processed
                await queue.join()

                # Wait for producer to finish
                await producer_task

                # Gather results from workers
                results = await asyncio.gather(*worker_tasks, return_exceptions=True)

                # Log total processed filings
                total_processed = sum(r for r in results if isinstance(r, int))
                logger.info("All workers completed. Total filings processed: %d", total_processed)

                # Handle any exceptions from workers
                for result in results:
                    if isinstance(result, Exception):
                        logger.error("Worker exception: %s", result)

            except Exception as e:
                logger.error("Main process failed: %s", e)
                raise
            finally:
                end_time = time.time()
                logger.info("Total execution time: %.2f seconds", end_time - start_time)


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())