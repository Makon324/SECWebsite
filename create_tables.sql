
-- Main filings table
CREATE TABLE sec_filing_transactions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- 'X' from your COLUMNS (D for derivative, else empty)
    x CHAR(1) NOT NULL DEFAULT '' CHECK (x IN ('D', 'A', '')),

    -- acceptance datetime from filing.acceptance_datetime
    acceptance_datetime TIMESTAMPTZ NOT NULL,

    -- filing datetime parsed from filing (keeps timezone if present)
    filing_datetime TIMESTAMPTZ,

    -- trade date usually a date only
    trade_date DATE,

    -- ticker and accession number
    ticker VARCHAR(16) NOT NULL,
    accession_number VARCHAR(64) NOT NULL,

    -- insider and role/title
    insider_name TEXT NOT NULL,
    title TEXT,

    -- boolean flags
    is_officer BOOLEAN NOT NULL DEFAULT FALSE,
    is_dir BOOLEAN NOT NULL DEFAULT FALSE,
    is_10_percent_owner BOOLEAN NOT NULL DEFAULT FALSE,

    -- transaction details
    trade_type CHAR(1),

    -- price and value: two decimal places (cents)
    price NUMERIC(14,2),
    qty INTEGER,
    value NUMERIC(18,2),

    -- housekeeping
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);




-- Processed filings
CREATE TABLE processed_filings (
  accession_number VARCHAR(64) PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- Table to store accession numbers to be processed with all FilingInfo fields
CREATE TABLE to_process (
    cik VARCHAR(10) NOT NULL,
    accession_number VARCHAR(64) PRIMARY KEY,
    acceptance_datetime TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


