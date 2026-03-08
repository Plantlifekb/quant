CREATE TABLE IF NOT EXISTS tickers (
    ticker TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS prices (
    ticker     TEXT NOT NULL REFERENCES tickers(ticker),
    date       DATE NOT NULL,
    open       DOUBLE PRECISION,
    high       DOUBLE PRECISION,
    low        DOUBLE PRECISION,
    close      DOUBLE PRECISION,
    volume     DOUBLE PRECISION,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS returns (
    ticker     TEXT NOT NULL REFERENCES tickers(ticker),
    date       DATE NOT NULL,
    ret        DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    ticker          TEXT NOT NULL REFERENCES tickers(ticker),
    as_of_date      DATE NOT NULL,
    revenue         DOUBLE PRECISION,
    earnings        DOUBLE PRECISION,
    book_value      DOUBLE PRECISION,
    eps             DOUBLE PRECISION,
    pe_ratio        DOUBLE PRECISION,
    pb_ratio        DOUBLE PRECISION,
    PRIMARY KEY (ticker, as_of_date)
);