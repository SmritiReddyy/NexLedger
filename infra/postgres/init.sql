-- NexLedger PostgreSQL init
-- Schema migrations are handled by Flyway at startup

-- Ensure the database has the correct encoding
ALTER DATABASE nexledger SET timezone TO 'UTC';

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable pg_stat_statements for query analysis
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
