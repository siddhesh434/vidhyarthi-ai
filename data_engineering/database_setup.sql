-- database_setup.sql
-- Run this in a Databricks Notebook or SQL Editor to create your Delta Lake foundation for the hackathon.

USE CATALOG bharat_bricks_sol;
USE SCHEMA default;

-- 1. The Core RAG Dataset
CREATE TABLE IF NOT EXISTS ncert_gold_chunks (
    chunk_id STRING,
    class_level INT,
    language STRING,
    chapter STRING,
    page_number INT,
    text_content STRING
) USING DELTA;

-- 2. The Student "Report Card" Memory
CREATE TABLE IF NOT EXISTS user_memory (
    user_id STRING,
    class_level INT,
    proficiency STRING,
    strong_points STRING,
    weak_points STRING
) USING DELTA;

-- 3. The Chat Logs (For Async Processing)
CREATE TABLE IF NOT EXISTS raw_chat_history (
    session_id STRING,
    user_id STRING,
    prompt STRING,
    raw_response STRING,
    timestamp TIMESTAMP
) USING DELTA;

-- 4. The Judges' Dashboard Logs
CREATE TABLE IF NOT EXISTS business_dashboard (
    log_id STRING,
    metric_name STRING,
    metric_value STRING,
    timestamp TIMESTAMP
) USING DELTA;

-- 5. Chat Sessions (one row per conversation)
CREATE TABLE IF NOT EXISTS chat_sessions (
    session_id STRING,
    user_id STRING,
    title STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

-- 6. Quiz History (every quiz attempt, linked to a chat session)
CREATE TABLE IF NOT EXISTS quiz_history (
    quiz_id STRING,
    session_id STRING,
    user_id STRING,
    questions STRING,
    user_answers STRING,
    correct_answers STRING,
    score INT,
    total INT,
    strong_point STRING,
    weak_point STRING,
    created_at TIMESTAMP
) USING DELTA;
