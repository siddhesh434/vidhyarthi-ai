# src/async_memory_updater.py
# This script updates the Delta Lake 'Memory' and 'Report Card' tables in the background.

import uuid
import requests
import json

def log_chat_to_db(spark, session_id, user_id, prompt, raw_response):
    """Saves the student's exact conversation directly into the Databricks `raw_chat_history` table."""
    safe_prompt = prompt.replace("'", "''")
    safe_response = raw_response.replace("'", "''")
    
    # We log this directly to Delta Lake so judges can audit student history!
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.raw_chat_history 
        VALUES ('{session_id}', '{user_id}', '{safe_prompt}', '{safe_response}', current_timestamp())
    """)
    
    # Update Business Dashboard Metric
    log_id = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.business_dashboard 
        VALUES ('{log_id}', 'Query Processed for {user_id}', '1', current_timestamp())
    """)

def submit_quiz_and_update_memory(spark, api_headers, user_id, class_level, user_answers, correct_answers, questions):
    """Evaluates the 5-question quiz and saves the TRUE psychological profile."""
    
    # FIRST FIX: Streamlit Radio Buttons return the FULL text (e.g. "B. Tiny organisms...")
    # But Sarvam's "answer" key is often just "B".
    # We must cleanly compare the first letter!
    score = 0
    clean_student_guesses = []
    
    for i, ans in enumerate(user_answers):
        # Even if ans is None, convert to string safely
        student_choice = str(ans).strip()
        correct_choice = str(correct_answers[i]).strip()
        
        # Check if the student choice starts with the correct answer (e.g., "B. " starts with "B")
        if student_choice.startswith(correct_choice) or student_choice == correct_choice:
            score += 1
            
        clean_student_guesses.append(student_choice)

    # 1. Ask Sarvam 105b to evaluate their actual performance!
    evaluation_prompt = f"""You are an educational psychologist. 
A student just took a 5 question quiz. Here are the questions, the correct answers, and what the student guessed:
Questions: {questions}
Correct Answers: {correct_answers}
Student Selected: {clean_student_guesses}

Analyze their actual conceptual gaps based on what they got wrong. Keep your analysis extremely brief (under 15 words).
You MUST output exactly two distinct lines.
Line 1 MUST start exactly with the words "Strong Point: " followed by your analysis.
Line 2 MUST start exactly with the words "Weak Point: " followed by your analysis.

Example Correct Output:
Strong Point: Understands the basic biological purpose of microscopes.
Weak Point: Struggles with naming specific multicellular organisms like Spirogyra.
"""
    
    payload = {
        "model": "sarvam-105b", # Changed from 2b to 105b to fix the 400 Endpoint Error!
        "messages": [{"role": "user", "content": evaluation_prompt}],
        "max_tokens": 500, 
        "temperature": 0.1
    }
    
    try:
        response = requests.post("https://api.sarvam.ai/v1/chat/completions", headers=api_headers, json=payload)
        response.raise_for_status()
        msg = response.json()['choices'][0]['message']
        raw_text = msg.get('content') or msg.get('reasoning_content') or ""
        
        # Parse it out
        strong = "General Comprehension"
        weak = "General Comprehension"
        for line in raw_text.splitlines():
            if "Strong Point:" in line: strong = line.replace("Strong Point:", "").replace("'", "").strip()
            if "Weak Point:" in line: weak = line.replace("Weak Point:", "").replace("'", "").strip()
            
    except Exception as e:
        strong, weak = "Error Extracting", f"Failed: {str(e)}"
    
    # 2. Insert verified psychology into Databricks Delta Lake Report Card
    proficiency = "High" if score >= 4 else "Medium" if score >= 2 else "Low"
    
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.user_memory 
        VALUES ('{user_id}', {class_level}, '{proficiency}', '{strong}', '{weak}')
    """)
    
    return score, strong, weak
