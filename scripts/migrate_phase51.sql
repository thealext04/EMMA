-- Phase 5.1 MVP field expansion
-- Adds 8 new columns to extracted_metrics for credit rating, operating
-- expenses, interest expense, forbearance/default booleans, gift revenue,
-- and municipal debt.
--
-- Safe to run multiple times (SQLite will error on duplicate ADD COLUMN,
-- so wrap each in a try block if running programmatically; run once manually).
--
-- Run with: sqlite3 data/emma.db < scripts/migrate_phase51.sql

ALTER TABLE extracted_metrics ADD COLUMN credit_rating TEXT;
ALTER TABLE extracted_metrics ADD COLUMN operating_expenses NUMERIC(18,2);
ALTER TABLE extracted_metrics ADD COLUMN interest_expense NUMERIC(18,2);
ALTER TABLE extracted_metrics ADD COLUMN technical_default BOOLEAN;
ALTER TABLE extracted_metrics ADD COLUMN forbearance_agreement BOOLEAN;
ALTER TABLE extracted_metrics ADD COLUMN forbearance_text TEXT;
ALTER TABLE extracted_metrics ADD COLUMN gift_revenue NUMERIC(18,2);
ALTER TABLE extracted_metrics ADD COLUMN municipal_debt NUMERIC(18,2);
