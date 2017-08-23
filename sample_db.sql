-- Sample PostgreSQL DB creation script used as data source for basic
-- ETL code.

-- Cleanup existing objects to make script idempotent.
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS daily_transaction_stats;
DROP ROLE IF EXISTS "test";

CREATE TABLE transactions (
  id SERIAL PRIMARY KEY,
  customer_id integer NOT NULL,
  amount integer NOT NULL,
  purchased_at timestamp without time zone NOT NULL
);

CREATE TABLE daily_transaction_stats (
  customer_id integer NOT NULL,
  date date NOT NULL,
  amount integer NOT NULL,
  PRIMARY KEY (customer_id, date)
);

-- Example code will hardcode this user. For example purposes
-- we don't need a more detailed ACL.
CREATE ROLE "test" WITH SUPERUSER LOGIN PASSWORD 'test';

-- Test transactions

-- Totals
-- Day 1: 1180
-- Day 2: 96
-- Day 3: 183
-- Customer 1
-- Day 1: 180
-- Day 2: 96
-- Day 3: 128
-- Customer 2
-- Day 1: 1000
-- Day 2: 0
-- Day 3: 55
INSERT INTO "transactions" (customer_id, amount, purchased_at) VALUES
(1, 55, '2017-03-01 09:00:00'),
(1, 125, '2017-03-01 10:00:00'),
(1, 32, '2017-03-02 13:00:00'),
(1, 64, '2017-03-02 15:00:00'),
(1, 128, '2017-03-03 10:00:00'),
(2, 333, '2017-03-01 09:00:00'),
(2, 334, '2017-03-01 09:01:00'),
(2, 333, '2017-03-01 09:02:00'),
(2, 11, '2017-03-03 20:00:00'),
(2, 44, '2017-03-03 20:15:00');
