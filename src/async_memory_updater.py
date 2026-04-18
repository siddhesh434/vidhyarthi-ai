# src/async_memory_updater.py
# This script updates the Delta Lake 'Memory' and 'Report Card' tables in the background.
# Also handles multi-chat session management and quiz history persistence.

import uuid
import requests
import json


# ─── Session Management ───────────────────────────────────────────────────────

def create_chat_session(spark, session_id, user_id, title="New Chat"):
    """INSERT a new row into chat_sessions when a student starts a new conversation."""
    safe_title = title.replace("'", "''")
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.chat_sessions
        VALUES ('{session_id}', '{user_id}', '{safe_title}', current_timestamp(), current_timestamp())
    """)


def get_user_sessions(spark, user_id):
    """SELECT all chat sessions for a user, ordered by most recent first. Returns list of dicts."""
    rows = spark.sql(f"""
        SELECT session_id, title, created_at, updated_at
        FROM bharat_bricks_sol.default.chat_sessions
        WHERE user_id = '{user_id}'
        ORDER BY updated_at DESC
    """).collect()
    return [
        {
            "session_id": r["session_id"],
            "title": r["title"],
            "created_at": str(r["created_at"]),
            "updated_at": str(r["updated_at"]),
        }
        for r in rows
    ]


def get_session_messages(spark, session_id):
    """SELECT all messages from raw_chat_history for a given session, ordered by time ASC."""
    rows = spark.sql(f"""
        SELECT prompt, raw_response, timestamp
        FROM bharat_bricks_sol.default.raw_chat_history
        WHERE session_id = '{session_id}'
        ORDER BY timestamp ASC
    """).collect()
    messages = []
    for r in rows:
        messages.append({"role": "user", "content": r["prompt"]})
        messages.append({"role": "assistant", "content": r["raw_response"]})
    return messages


def update_session_timestamp(spark, session_id):
    """UPDATE updated_at on the session row so it bubbles to the top of the history list."""
    spark.sql(f"""
        UPDATE bharat_bricks_sol.default.chat_sessions
        SET updated_at = current_timestamp()
        WHERE session_id = '{session_id}'
    """)


def update_session_title(spark, session_id, title):
    """UPDATE the chat session title (auto-generated from the first user message)."""
    safe_title = title.replace("'", "''")
    spark.sql(f"""
        UPDATE bharat_bricks_sol.default.chat_sessions
        SET title = '{safe_title}'
        WHERE session_id = '{session_id}'
    """)


# ─── Quiz History ─────────────────────────────────────────────────────────────

def save_quiz_result(spark, session_id, user_id, quiz_data, user_answers, correct_answers, score, total, strong, weak):
    """INSERT a quiz attempt into quiz_history, linked to the current chat session."""
    quiz_id = str(uuid.uuid4())
    safe_questions = json.dumps(quiz_data).replace("'", "''")
    safe_user_ans = json.dumps(user_answers).replace("'", "''")
    safe_correct_ans = json.dumps(correct_answers).replace("'", "''")
    safe_strong = strong.replace("'", "''")
    safe_weak = weak.replace("'", "''")
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.quiz_history
        VALUES ('{quiz_id}', '{session_id}', '{user_id}', '{safe_questions}', '{safe_user_ans}', '{safe_correct_ans}', {score}, {total}, '{safe_strong}', '{safe_weak}', current_timestamp())
    """)


def get_session_quizzes(spark, session_id):
    """SELECT all quiz attempts for a given session, ordered by most recent first."""
    rows = spark.sql(f"""
        SELECT quiz_id, score, total, strong_point, weak_point, created_at
        FROM bharat_bricks_sol.default.quiz_history
        WHERE session_id = '{session_id}'
        ORDER BY created_at DESC
    """).collect()
    return [
        {
            "quiz_id": r["quiz_id"],
            "score": r["score"],
            "total": r["total"],
            "strong_point": r["strong_point"],
            "weak_point": r["weak_point"],
            "created_at": str(r["created_at"]),
        }
        for r in rows
    ]


# ─── Chat Logging (Original — unchanged) ─────────────────────────────────────

def log_chat_to_db(spark, session_id, user_id, prompt, raw_response):
    """Saves the student's exact conversation directly into the Databricks `raw_chat_history` table."""
    safe_prompt = prompt.replace("'", "''")
    safe_response = raw_response.replace("'", "''")
    
    # We log this directly to Delta Lake so judges can audit student history!
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.raw_chat_history 
        VALUES ('{session_id}', '{user_id}', '{safe_prompt}', '{safe_response}', current_timestamp())
    """)
    
    # Keep the session's updated_at current
    update_session_timestamp(spark, session_id)
    
    # Update Business Dashboard Metric
    log_id = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.business_dashboard 
        VALUES ('{log_id}', 'Query Processed for {user_id}', '1', current_timestamp())
    """)

def submit_quiz_and_update_memory(spark, api_headers, user_id, class_level, user_answers, correct_answers, questions):
    """Evaluates the quiz and returns (score, analysis_paragraph)."""

    # Score calculation
    score = 0
    clean_student_guesses = []
    for i, ans in enumerate(user_answers):
        student_choice = str(ans).strip()
        correct_choice = str(correct_answers[i]).strip()
        if student_choice.startswith(correct_choice) or student_choice == correct_choice:
            score += 1
        clean_student_guesses.append(student_choice)

    total = len(questions)

    # Build a clear Q&A breakdown for the model
    qa_lines = []
    wrong_count = 0
    for i, q in enumerate(questions):
        s_ans = clean_student_guesses[i]
        c_ans = correct_answers[i]
        correct = s_ans.startswith(c_ans) or s_ans == c_ans
        tag = "✓ CORRECT" if correct else "✗ WRONG"
        if not correct:
            wrong_count += 1
        qa_lines.append(
            f"Q{i+1} [{tag}]\n"
            f"  Question : {q}\n"
            f"  Student  : {s_ans}\n"
            f"  Correct  : {c_ans}"
        )
    qa_block = "\n\n".join(qa_lines)

    if wrong_count == 0:
        evaluation_prompt = f"""You are an expert educational psychologist. A student scored {score}/{total} — a PERFECT SCORE.

Questions they answered perfectly:
{qa_block}

Write ONE short paragraph (3-4 sentences) for the student's report card that:
1. Praises their specific conceptual strengths demonstrated by these questions.
2. Suggests one specific concept in this topic area they should explore next to grow further.

Be specific to the actual quiz topics. Do NOT write generic praise. Write directly to the student (use "you")."""
    else:
        evaluation_prompt = f"""You are an expert educational psychologist. A student scored {score}/{total}.

Their detailed quiz performance:
{qa_block}

Write ONE short paragraph (3-4 sentences) for the student's report card that:
1. Acknowledges what they understood correctly (from the CORRECT answers) — be specific to the topic.
2. Clearly explains what concepts they are confused about (from the WRONG answers) — name the exact concept.
3. Gives one concrete suggestion on how to improve.

Be specific to the actual quiz topics. Do NOT write "General Comprehension". Write directly to the student (use "you")."""

    payload = {
        "model": "sarvam-105b",
        "messages": [{"role": "user", "content": evaluation_prompt}],
        "max_tokens": 4095,
        "temperature": 0.2,
    }

    analysis = "Your quiz has been recorded. Detailed analysis unavailable — please try again."

    try:
        response = requests.post(
            "https://api.sarvam.ai/v1/chat/completions",
            headers=api_headers,
            json=payload,
            timeout=90,
        )
        response.raise_for_status()

        msg = response.json()['choices'][0]['message']
        # content = final answer for reasoning models; reasoning_content = the thinking chain
        content_text   = (msg.get('content')           or "").strip()
        reasoning_text = (msg.get('reasoning_content') or "").strip()

        print(f"[EVAL v7 content]:   {repr(content_text[:400])}")
        print(f"[EVAL v7 reasoning]: {repr(reasoning_text[:200])}")

        # Prefer the clean final answer; fall back to reasoning if content is empty
        raw = content_text or reasoning_text
        if raw:
            # Strip any markdown code fences the model might add
            import re
            raw = re.sub(r'```.*?```', '', raw, flags=re.DOTALL).strip()
            raw = raw.replace("'", "''")
            if len(raw) > 20:  # Accept anything longer than a stub
                analysis = raw

    except Exception as e:
        print(f"[EVAL EXCEPTION]: {e}")

    # Store the full paragraph in strong_points; weak_points kept for schema compatibility
    proficiency = "High" if score >= 4 else "Medium" if score >= 2 else "Low"
    safe_analysis = analysis.replace("'", "''")

    # Purge old memory for this user so we only ever have ONE row per student
    spark.sql(f"DELETE FROM bharat_bricks_sol.default.user_memory WHERE user_id = '{user_id}'")

    # Insert verified verified psychology into Databricks Delta Lake Report Card
    spark.sql(f"""
        INSERT INTO bharat_bricks_sol.default.user_memory
        VALUES ('{user_id}', {class_level}, '{proficiency}', '{safe_analysis}', '{safe_analysis}')
    """)

    return score, analysis


