CREATE OR REPLACE FUNCTION insert_sec_filing_transactions_from_json(
    p_accession VARCHAR,
    p_acceptance_datetime TIMESTAMPTZ,
    p_data JSONB
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM processed_filings WHERE accession_number = p_accession) THEN
        RAISE EXCEPTION 'accession % is already processed', p_accession;
    END IF;

    INSERT INTO sec_filing_transactions (
        x,
        acceptance_datetime,
        filing_datetime,
        trade_date,
        ticker,
        accession_number,
        insider_name,
        title,
        is_officer,
        is_dir,
        is_10_percent_owner,
        trade_type,
        price,
        qty,
        value
    )
    SELECT
        COALESCE(r."X", '')::char(1),
        p_acceptance_datetime,
        NULLIF(r."Filing Date", '')::timestamptz,
        NULLIF(r."Trade Date", '')::date,
        r."Ticker"::varchar,
        p_accession,
        r."Insider Name"::text,
        r."Title"::text,
        COALESCE(r."IsOfficer", FALSE)::boolean,
        COALESCE(r."IsDir", FALSE)::boolean,
        COALESCE(r."Is10%", FALSE)::boolean,
        NULLIF(r."Trade Type", '')::char(1),
        r."Price"::numeric,
        r."Qty"::integer,
        r."Value"::numeric
    FROM jsonb_to_recordset(p_data) AS r(
        "X" text,
        "Acceptance Date" text,  -- Added this column
        "Filing Date" text,
        "Trade Date" text,
        "Ticker" text,
        "Insider Name" text,
        "Title" text,
        "IsOfficer" boolean,
        "IsDir" boolean,
        "Is10%" boolean,
        "Trade Type" text,
        "Price" numeric,
        "Qty" integer,
        "Value" numeric
    )
    WHERE r."Ticker" IS NOT NULL AND r."Insider Name" IS NOT NULL;

    INSERT INTO processed_filings (accession_number)
    VALUES (p_accession);
END;
$$;