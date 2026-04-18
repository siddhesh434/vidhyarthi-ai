-- clear_dynamic_data.sql
-- Paste and run this in your Databricks SQL Editor to wipe all chat/quiz history 
-- but keep the table schemas intact for a fresh demo.

USE CATALOG bharat_bricks_sol;
USE SCHEMA default;

-- Truncate all user-generated tracking tables
TRUNCATE TABLE user_memory;
TRUNCATE TABLE raw_chat_history;
TRUNCATE TABLE business_dashboard;
TRUNCATE TABLE chat_sessions;
TRUNCATE TABLE quiz_history;

-- NOTE: I have intentionally OMITTED `ncert_gold_chunks` from being truncated.
-- If you delete the chunks, your bot will not be able to answer any questions 
-- until you re-run the 1_ingest_ncert.py pipeline!
-- If you REALLY need to wipe the embedded book knowledge too, uncomment this:
-- TRUNCATE TABLE ncert_gold_chunks;
