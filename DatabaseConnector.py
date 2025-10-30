import json
import datetime
import pandas as pd
import logging
from SECscraper import FilingInfo
import asyncpg  # for type hints

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """
    A small helper that provides async methods to interact with the database for SEC filing transactions.
    - This class does NOT create or manage a connection pool.
    - The caller must supply an active asyncpg connection when calling methods.
    - The caller should control transactions where necessary.
    """

    def _df_to_json_string(self, df: pd.DataFrame) -> str:
        """
        Convert DataFrame -> JSON string (array of objects) in a background thread.
        - Replaces NaN/NaT with None (so JSON null).
        - Converts pandas Timestamps / datetime -> ISO 8601 strings.
        """
        df_clean = df.where(pd.notnull(df), None).copy()
        records = df_clean.to_dict(orient="records")
        for rec in records:
            for k, v in list(rec.items()):
                if isinstance(v, (pd.Timestamp, datetime.datetime)):
                    # preserve timezone info if present
                    rec[k] = v.isoformat()
                # leave other Python types as-is (json.dumps can handle them or default=str used)
        return json.dumps(records, default=str)

    async def insert_dataframe(
            self, conn: asyncpg.Connection, filing: FilingInfo, df: pd.DataFrame
    ) -> None:
        """Insert DataFrame into the database via stored function, atomically.

        Args:
            conn: Active database connection.
            filing: Filing metadata.
            df: DataFrame of transaction data.

        Raises:
            ValueError: If conn is invalid or closed.
            asyncpg.IntegrityError: If insert fails.
            asyncpg.PostgresError: On database failures.
        """
        if conn is None or conn.is_closed():
            raise ValueError("conn must be an active, open asyncpg connection")
        json_data = self._df_to_json_string(df)
        async with conn.transaction():
            try:
                result = await conn.fetchval(
                    "SELECT insert_sec_filing_transactions_from_json($1, $2, $3::jsonb);",
                    filing.accession_number,
                    filing.acceptance_datetime,
                    json_data
                )
                if result is not None:  # Check stored function outcome if it returns status
                    logger.debug("Stored function result: %s", result)
                await conn.execute(
                    "DELETE FROM to_process WHERE accession_number = $1;",
                    filing.accession_number
                )

            except (asyncpg.UniqueViolationError,
                    asyncpg.ForeignKeyViolationError,
                    asyncpg.NotNullViolationError,
                    asyncpg.CheckViolationError,
                    asyncpg.ExclusionViolationError) as e:
                logger.error("Integrity/constraint violation for accession %s: %s",
                         filing.accession_number, e, exc_info=True)
                raise
            except asyncpg.PostgresError as e:
                logger.critical("Database error: %s", e, exc_info=True)
                raise

    async def is_accession_processed(self, conn: asyncpg.Connection, accession_number: str) -> bool:
        """
        Check if an accession number exists in the processed_filings table.
        Returns True if processed, False if not.
        """
        if conn is None or conn.is_closed():
            raise ValueError("conn must be an active asyncpg connection (not None)")

        result = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM processed_filings WHERE accession_number = $1);",
            accession_number
        )
        return bool(result)

    async def insert_to_process(self, conn: asyncpg.Connection, filings: list[FilingInfo]) -> int:
        """
        Insert a list of FilingInfo objects into the to_process table,
        but only those whose accession_numbers are not already in processed_filings.
        Returns the number of successfully inserted rows.
        """
        if conn is None:
            raise ValueError("conn must be an active asyncpg connection (not None)")

        if not filings:
            return 0

        async with conn.transaction():
            # Prepare data for bulk insertion
            ciks = []
            accession_numbers = []
            acceptance_datetimes = []

            for fi in filings:
                ciks.append(fi.cik)
                accession_numbers.append(fi.accession_number)
                acceptance_datetimes.append(fi.acceptance_datetime)

            # Insert and return the count of actually inserted rows
            result = await conn.fetchval("""
                                         WITH inserted AS (
                                         INSERT
                                         INTO to_process (cik, accession_number, acceptance_datetime)
                                         SELECT cik, acc_num, acc_dt
                                         FROM unnest($1::text[], $2::text[], $3::timestamptz[]) AS t(cik, acc_num, acc_dt)
                                         WHERE NOT EXISTS (SELECT 1
                                                           FROM processed_filings
                                                           WHERE accession_number = t.acc_num) ON CONFLICT (accession_number) DO NOTHING
                    RETURNING 1
                )
                                         SELECT COUNT(*)
                                         FROM inserted;
                                         """, ciks, accession_numbers, acceptance_datetimes)

            return result or 0

    async def get_to_process_filings(self, conn: asyncpg.Connection, n: int) -> list[FilingInfo]:
        """
        Retrieve up to n filing records from the to_process table atomically.
        Uses FOR UPDATE SKIP LOCKED to ensure concurrent calls get unique accession numbers.
        Returns a list of FilingInfo objects (up to n, or fewer if fewer exist).
        """
        if conn is None or conn.is_closed():
            raise ValueError("conn must be an active asyncpg connection (not None)")
        if n < 1:
            raise ValueError("n must be a positive integer")

            # No extra transaction; query is atomic
        records = await conn.fetch(
            """
            WITH next_filings AS (SELECT accession_number
                                  FROM to_process
                                  WHERE status = 'pending'
                                  ORDER BY created_at ASC
                LIMIT $1
                FOR
            UPDATE SKIP LOCKED
                )
            UPDATE to_process
            SET status      = 'in_progress'
            FROM next_filings
            WHERE to_process.accession_number = next_filings.accession_number
                RETURNING to_process.cik
                , to_process.accession_number
                , to_process.acceptance_datetime;
            """,
            n
        )
        # Convert records to FilingInfo objects
        filing_infos = [
            FilingInfo(
                cik=record["cik"],
                accession_number=record["accession_number"],
                acceptance_datetime=record["acceptance_datetime"]
            )
            for record in records
        ]
        return filing_infos

    async def reset_to_pending(self, conn: asyncpg.Connection):
        """Reset all rows in the to_process table to 'pending' status."""
        if conn is None or conn.is_closed():
            raise ValueError("conn must be an active asyncpg connection (not None)")

        await conn.execute(
            """
            UPDATE to_process
            SET status = 'pending'
            WHERE status != 'pending';
            """
        )
