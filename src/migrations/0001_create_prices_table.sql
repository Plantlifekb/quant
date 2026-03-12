-- migrations/0001_create_prices_table.sql
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.prices (
  date date NOT NULL,
  adj_close numeric,
  close numeric,
  high numeric,
  low numeric,
  open numeric,
  volume bigint,
  ticker text,
  PRIMARY KEY (date, ticker)
);