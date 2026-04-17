# src/async_memory_updater.py
# This script updates the Delta Lake 'Memory' and 'Report Card' tables in the background.

import threading
import uuid
import requests

def construct_memory_prompt(student_prompt):
    return f"""You are an educational psychologist analyzing a student.
Based on the student's question below, simply identify one "Weak Point" (what they don't understand) and one "Strong Point" (what they are showing curiosity about).
Keep the answer under 15 words.

Student Question: "{student_prompt}"
Analysis:"""

def _background_update_task(spark, api_headers, user_id, class_level, student_prompt):
    """The internal function that runs invisibly on the Databricks node"""
    
    print(f"[Async Thread] Starting memory update for user: {user_id}...")
    
    prompt = construct_memory_prompt(student_prompt)
    
    payload = {
        "model": "sarvam-2b", # Using smaller model for fast background logic
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 200, # Increased for safety
        "temperature": 0.1
    }
    
    try:
        response = requests.post("https://api.sarvam.ai/v1/chat/completions", headers=api_headers, json=payload)
        response.raise_for_status()
        msg = response.json()['choices'][0]['message']
        raw_text = msg.get('content') or msg.get('reasoning_content') or "Unknown Analysis"
        psych_analysis = raw_text.strip().replace("'", "")
    except Exception as e:
        psych_analysis = f"Analysis Failed: Exception"
    
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.user_memory 
        VALUES ('{user_id}', {class_level}, 'Medium', 'Curious Learner', '{psych_analysis}')
    """)
    
    log_id = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.business_dashboard 
        VALUES ('{log_id}', 'Query Processed Class {class_level}', '1', current_timestamp())
    """)
    
    print(f"[Async Thread] Memory successfully updated into Delta Lake!")

def fire_and_forget_memory_update(spark, api_headers, user_id, class_level, student_prompt):
    """Call this function from your Streamlit App."""
    thread = threading.Thread(
        target=_background_update_task, 
        args=(spark, api_headers, user_id, class_level, student_prompt)
    )
    thread.daemon = True 
    thread.start()
