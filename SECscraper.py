import asyncio
import datetime as dt
import json
import logging
import re
import time
from datetime import timezone
from typing import Optional
from collections import deque
import aiohttp
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dataclasses import dataclass

from config import Config

COLUMNS = [
    'X', 'Acceptance Date', 'Filing Date', 'Trade Date', 'Ticker', 'Insider Name',
    'Title', 'IsOfficer', 'IsDir', 'Is10%', 'Trade Type', 'Price', 'Qty', 'Value'
]


@dataclass
class FilingInfo:
    cik: str
    accession_number: str
    acceptance_datetime: dt.datetime


class InstantFlushHandler(logging.StreamHandler):
    """Custom logging handler that flushes immediately after emitting a record."""

    def emit(self, record):
        super().emit(record)
        self.flush()


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SECScraper:
    """Class for scraping SEC filings, particularly Form 4 insider trades."""

    def __init__(self):
        """Initialize the SECScraper instance."""
        self.config = Config()

        self._cik_map: Optional[dict] = None
        self.session: Optional[aiohttp.ClientSession] = None

        # For rate limiting
        self._rate_lock = asyncio.Lock()
        self._request_times = deque(maxlen=self.config.max_requests_sec)

    async def __aenter__(self):
        """Enter the async context manager."""
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.config.user_agent})
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit the async context manager."""
        if self.session:
            await self.session.close()
            self.session = None

    async def close(self):
        """Close the client session if it exists."""
        if self.session:
            await self.session.close()
            self.session = None

    async def _rate_limit(self):
        """Called before each request to comply with SECs rate limit."""
        async with self._rate_lock:
            now = time.time()

            # Prune timestamps older than 1 second (left is oldest)
            while self._request_times and self._request_times[0] <= now - 1:
                self._request_times.popleft()

            # If at limit, wait until the oldest ages out
            if len(self._request_times) >= self.config.max_requests_sec:
                wait_time = (self._request_times[0] + 1) - now
                if wait_time > 0:
                    await asyncio.sleep(wait_time)

                    # After sleep, update now and re-prune (in case of drift)
                    now = time.time()
                    while self._request_times and self._request_times[0] <= now - 1:
                        self._request_times.popleft()

            self._request_times.append(now)

    async def _make_sec_request(self, url: str, retries: int = 3) -> str:
        """
        Make SEC request with rate limiting and retries. Returns response text.

        Args:
            url (str): The URL to request.
            retries (int, optional): Number of retry attempts. Defaults to 3.

        Returns:
            str: The response text.

        Raises:
            RuntimeError: If the request fails after all retries.
            aiohttp.ClientError: On client-side errors.
        """
        if self.session is None:
            self.session = aiohttp.ClientSession(headers={'User-Agent': self.config.user_agent})

        for attempt in range(retries):
            await self._rate_limit()

            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with self.session.get(url, timeout=timeout) as resp:
                    status = resp.status
                    text = await resp.text()

                    # Rate-limited
                    if status == 429 or "Request Rate Threshold Exceeded" in text:
                        raise SecRateLimitError()

                    # Retry on server errors (5xx), but allow response.raise_for_status to raise for 4xx
                    if 500 <= status < 600 and attempt < retries - 1:
                        wait = 2 ** attempt
                        logger.warning(f"Server error ({status}), retry {attempt + 1}/{retries} in {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    # will raise for 4xx client errors
                    if status >= 400:
                        # raise a ClientResponseError-like exception for handling upstream if needed
                        resp.raise_for_status()

                    # success: return text
                    return text

            except SecRateLimitError:
                logger.warning("SEC rate limit exceeded;")
                time.sleep(self.config.to_wait_on_rate_limit)
                continue

            except aiohttp.ClientResponseError as e:
                # HTTP errors (status >= 400 handled above, but keep for completeness)
                logger.error(f"HTTP error after {attempt + 1}/{retries}: {e}")
                if 500 <= getattr(e, 'status', 0) < 600 and attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise

            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"Network error, retry {attempt + 1}/{retries} in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                logger.error(f"Network error after {retries} retries: {e}")
                raise

            except aiohttp.ClientError as e:
                logger.error(f"Fatal request error: {e}")
                raise

        raise RuntimeError(f"Failed to fetch {url} after {retries} attempts.")

    async def _load_cik_map(self):
        """Helper to fetch and build the CIK map."""
        try:
            text = await self._make_sec_request("https://www.sec.gov/files/company_tickers.json")
            data = json.loads(text)

            # Build lookup dicts
            self._cik_map = {
                'ticker_to_cik': {v['ticker'].upper(): str(v['cik_str']).zfill(10) for v in data.values()},
                'cik_to_ticker': {str(v['cik_str']).zfill(10): v['ticker'] for v in data.values()}
            }
            logger.info("CIK map successfully initialized.")
        except Exception as e:
            logger.error(f"Failed to fetch CIK map: {str(e)}")
            self._cik_map = {'ticker_to_cik': {}, 'cik_to_ticker': {}}

    async def _init_cik_map(self):
        """Initialize CIK map if not already loaded."""
        if self._cik_map is None:
            await self._load_cik_map()

    async def ticker_to_cik(self, ticker: str) -> Optional[str]:
        """
        Convert ticker to CIK.

        Args:
            ticker (str): The stock ticker symbol.

        Returns:
            Optional[str]: The corresponding CIK, or None if not found.
        """
        await self._init_cik_map()
        cik = self._cik_map['ticker_to_cik'].get(ticker.upper())
        if cik is None:
            logger.warning(f"Could not find CIK for ticker: {ticker}")
        return cik

    async def cik_to_ticker(self, cik: str) -> Optional[str]:
        """
        Convert CIK to ticker.

        Args:
            cik (str): The CIK number.

        Returns:
            Optional[str]: The corresponding ticker, or None if not found.
        """
        await self._init_cik_map()
        ticker = self._cik_map['cik_to_ticker'].get(str(cik).zfill(10))
        if ticker is None:
            logger.warning(f"Could not find ticker for CIK: {cik}")
        return ticker

    async def get_ciks(self):
        """
        Get list of CIKs on EDGAR.

        Returns:
            list[str]: List of CIKs.
        """
        await self._init_cik_map()
        return list(self._cik_map['cik_to_ticker'].keys())

    async def scrape_filing(self, filing: FilingInfo) -> pd.DataFrame:
        """
        Process single SEC filing (async).

        Args:
            filing (FilingInfo): The filing information to scrape.

        Returns:
            pd.DataFrame: DataFrame containing the parsed transaction data.
        """
        url = (f"https://www.sec.gov/Archives/edgar/data/"
               f"{filing.cik}/{filing.accession_number.replace('-', '')}/{filing.accession_number}.txt")

        try:
            text = await self._make_sec_request(url)
            soup = BeautifulSoup(text, 'xml')
        except Exception as e:
            logger.warning(f"Failed to retrieve filing {filing.accession_number}: {str(e)}")
            return pd.DataFrame()

        # Extract common filing information
        try:
            # Extract title and boolean flags
            title, is_officer, is_director, is_10_percent_owner = SECScraper._extract_title(soup)

            acceptance_match = re.search(r"<ACCEPTANCE-DATETIME>\s*(\d+)\s*", text)
            filing_date = None
            if acceptance_match:
                filing_date = dt.datetime.strptime(acceptance_match.group(1), "%Y%m%d%H%M%S")
            else:
                # fallback: try to find filingDate tag in XML
                fd_tag = soup.find('filingDate')
                filing_date = dt.datetime.strptime(fd_tag.text, "%Y-%m-%d") if fd_tag else filing.acceptance_datetime

            ticker_tag = soup.find('issuerTradingSymbol')
            owner_tag = soup.find('rptOwnerName')

            if ticker_tag is None or owner_tag is None:
                raise AttributeError("Missing ticker or owner name in filing XML.")

            filing_data = {
                'Acceptance Date': filing.acceptance_datetime,
                'Filing Date': filing_date,
                'Ticker': ticker_tag.text,
                'Insider Name': owner_tag.text,
                'Title': title,
                'IsOfficer': is_officer,
                'IsDir': is_director,
                'Is10%': is_10_percent_owner
            }
        except AttributeError as e:
            logger.warning(f"Missing required filing data in {filing.accession_number}: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Unexpected error parsing {filing.accession_number}: {str(e)}")
            return pd.DataFrame()

        # Process non-derivative transactions
        transactions = []
        for transaction in soup.find_all('nonDerivativeTransaction'):
            try:
                transactions.append(SECScraper._parse_transaction(transaction, filing_data))
            except Exception as e:
                logger.warning(f"Failed to parse non-derivative transaction: {str(e)}")

        # Process derivative transactions
        for transaction in soup.find_all('derivativeTransaction'):
            try:
                transactions.append(SECScraper._parse_transaction(transaction, filing_data))
            except Exception as e:
                logger.warning(f"Failed to parse derivative transaction: {str(e)}")

        if not transactions:
            return pd.DataFrame()

        return SECScraper._combine_records(
            pd.DataFrame(transactions)
        )

    @staticmethod
    def _extract_title(soup: BeautifulSoup) -> tuple:
        """
        Extract and format insider title along with boolean flags.

        Args:
            soup (BeautifulSoup): The parsed XML soup.

        Returns:
            tuple: (title, is_officer, is_director, is_10_percent_owner)
        """
        relationship = soup.find('reportingOwnerRelationship')
        title_parts = []
        is_officer = False
        is_director = False
        is_10_percent_owner = False

        if relationship:
            # Check officer status
            officer_elem = relationship.find('isOfficer')
            if officer_elem and officer_elem.text.lower() in ['true', '1']:
                is_officer = True
                title_elem = relationship.find('officerTitle')
                title_text = title_elem.text if title_elem else 'See Remarks'
                if 'See Remarks' in title_text:
                    remarks = soup.find('remarks')
                    title_text = remarks.text if remarks else title_text
                title_parts.append(title_text)

            # Check director status
            director_elem = relationship.find('isDirector')
            if director_elem and director_elem.text.lower() in ['true', '1']:
                is_director = True
                title_parts.append('Director')

            # Check 10% owner status
            ten_percent_elem = relationship.find('isTenPercentOwner')
            if ten_percent_elem and ten_percent_elem.text.lower() in ['true', '1']:
                is_10_percent_owner = True
                title_parts.append('10% Owner')

        title = ', '.join(title_parts) if title_parts else 'No Title'
        return title, is_officer, is_director, is_10_percent_owner

    @staticmethod
    def _parse_transaction(transaction: BeautifulSoup, filing_data: dict) -> dict:
        """
        Parse individual transaction data.

        Args:
            transaction (BeautifulSoup): The transaction XML element.
            filing_data (dict): Common filing data.

        Returns:
            dict: Parsed transaction details.
        """
        try:
            price_elem = transaction.find('transactionPricePerShare')
            # sometimes price is nested with <value> tag, sometimes not
            if price_elem and price_elem.find('value'):
                price = float(price_elem.find('value').text)
            elif price_elem and price_elem.text:
                price = float(price_elem.text)
            else:
                price = 0.0
        except (AttributeError, ValueError):
            price = 0.0

        try:
            qty_elem = transaction.find('transactionShares')
            if qty_elem and qty_elem.find('value'):
                qty = int(qty_elem.find('value').text)
            elif qty_elem and qty_elem.text:
                qty = int(qty_elem.text)
            else:
                qty = 0
        except (AttributeError, ValueError):
            qty = 0

        try:
            code_elem = transaction.find('transactionAcquiredDisposedCode')
            code = code_elem.find('value').text if (code_elem and code_elem.find('value')) else (
                code_elem.text if code_elem else '')
        except AttributeError:
            code = ''

        trade_date_tag = transaction.find('transactionDate')
        trade_date_value = trade_date_tag.find('value').text if (trade_date_tag and trade_date_tag.find('value')) else (
            trade_date_tag.text if trade_date_tag else None)

        return {
            **filing_data,
            'Trade Date': pd.to_datetime(trade_date_value).date() if trade_date_value else None,
            'Trade Type': (transaction.find('transactionCode').text if transaction.find('transactionCode') else ''),
            'Price': price,
            'Value': qty * price * (-1 if code == 'D' else 1),
            'Qty': qty * (-1 if code == 'D' else 1),
            'X': code
        }

    @staticmethod
    def _combine_records(df: pd.DataFrame) -> pd.DataFrame:
        """
        Combine transactions from a single filing to reduce duplicate entries.

        Args:
            df (pd.DataFrame): DataFrame of transactions.

        Returns:
            pd.DataFrame: Combined DataFrame.
        """
        if df.empty:
            return df

        # Define the grouping columns - these identify unique filing combinations
        group_cols = [
            'X', 'Acceptance Date', 'Filing Date', 'Ticker', 'Insider Name',
            'Title', 'IsOfficer', 'IsDir', 'Is10%', 'Trade Date', 'Trade Type'
        ]

        # Group and aggregate
        combined_df = df.groupby(group_cols, as_index=False).agg(
            Qty=('Qty', 'sum'),
            Value=('Value', 'sum'),
            Price=('Price', lambda p: np.average(p, weights=df.loc[p.index, 'Qty'])
            if df.loc[p.index, 'Qty'].sum() != 0 else p.mean())
        )

        # Make Price always positive
        combined_df['Price'] = combined_df['Price'].abs()

        # Reorder columns to match original structure
        combined_df = combined_df[COLUMNS]

        return combined_df

    @staticmethod
    def _get_dt_wofc(dt_obj: str) -> dt.datetime:
        """
        Parse an ISO-8601 date string (with fractional seconds or offset/Z)
        and return a UTC datetime (timezone-aware).

        Args:
            dt_obj (str): The date string to parse.

        Returns:
            dt.datetime: Parsed datetime in UTC.
        """
        # Decide which format to use
        if '.' in dt_obj:
            fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
        else:
            fmt = "%Y-%m-%dT%H:%M:%S%z"

        # If the string ends with 'Z', replace it so %z can parse as +00:00
        if dt_obj.endswith('Z'):
            dt_obj = dt_obj[:-1] + "+00:00"

        # Parse into an aware datetime
        parsed = dt.datetime.strptime(dt_obj, fmt)

        # Convert to UTC
        dt_utc = parsed.astimezone(timezone.utc)

        return dt_utc

    async def get_filings(self, cik: str) -> list[FilingInfo]:
        """
        Retrieve list of Form 4 filings for a CIK with acceptanceDateTime.

        Args:
            cik (str): The CIK number.

        Returns:
            list[FilingInfo]: List of Form 4 filings.
        """
        try:
            text = await self._make_sec_request(f"https://data.sec.gov/submissions/CIK{cik}.json")
            data = json.loads(text)
            return [
                FilingInfo(
                    cik=cik,
                    accession_number=acc,
                    acceptance_datetime=SECScraper._get_dt_wofc(acceptance_dt)
                )
                for acc, form, acceptance_dt in zip(
                    data['filings']['recent']['accessionNumber'],
                    data['filings']['recent']['form'],
                    data['filings']['recent']['acceptanceDateTime']
                ) if form == '4'
            ]
        except Exception as e:
            logger.error(f"Failed to get filings for CIK {cik}: {str(e)}")
            return []

    async def get_current_filings(self, page: int = 0) -> list[FilingInfo]:
        """
        Retrieve newest 100 Form 4 filings from SEC (async).

        Args:
            page (int, optional): Page number for pagination. Defaults to 0.

        Returns:
            list[FilingInfo]: List of recent Form 4 filings.
        """
        text = await self._make_sec_request(
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&count=100&start={100 * page}&output=atom"
        )

        soup = BeautifulSoup(text, 'xml')
        entries = soup.find_all('entry')
        results: list[FilingInfo] = []

        for entry in entries:
            link_tag = entry.find('link', rel='alternate')
            updated_tag = entry.find('updated')
            if updated_tag is None or link_tag is None:
                continue

            acceptanceDT = SECScraper._get_dt_wofc(updated_tag.text)

            filing_url = link_tag.get('href', '')
            parts = filing_url.split('/')
            try:
                data_index = parts.index('data')
            except ValueError:
                continue

            if data_index + 2 >= len(parts):
                continue

            cik = parts[data_index + 1].zfill(10)
            accession_num = parts[data_index + 2]
            accession_num = f"{accession_num[:10]}-{accession_num[10:12]}-{accession_num[12:]}"

            results.append(
                FilingInfo(
                    cik=cik,
                    accession_number=accession_num,
                    acceptance_datetime=acceptanceDT
                )
            )

        return results


class SecRateLimitError(Exception):
    """Raised when the SEC says the request rate threshold has been exceeded."""
    pass