# app.py
# This is the Main UI deployed via Databricks Apps for the Hackathon.
import streamlit as st
import uuid

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# When deployed on Databricks Apps, we must use databricks-connect
from databricks.connect import DatabricksSession
import os

try:
    # Attempt to securely connect to Databricks Serverless Compute
    spark = DatabricksSession.builder.serverless().getOrCreate()
except Exception as e:
    # Fallback to a specific cluster if provided by the environment
    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    if cluster_id:
        spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
    else:
        st.error(f"Failed to initialize Spark connection. Error: {e}")
        raise e

from src.llm_engine import VidyarthiAgent
from src.async_memory_updater import log_chat_to_db, submit_quiz_and_update_memory

# --- UI CONFIGURATION (To achieve the WOW factor) ---
st.set_page_config(page_title="Vidyarthi-AI | Bharat Educator", page_icon="🎓", layout="wide")
st.title("🎓 Vidyarthi-AI: The Open NCERT Tutor")

# --- ONBOARDING SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Student Setup")
    user_id = st.text_input("Student ID:", value="Student_IND_001")
    class_level = st.selectbox("Select your Class:", [8, 9, 10], index=0)
    language = st.selectbox("Preferred Instruction Language:", ["Hindi", "English", "Urdu"])
    
    st.markdown("---")
    st.markdown("### 🎙️ Native Speech Support")
    st.markdown("*Use your computer's built-in dictation (Win+H or Mac Dictate) directly in the chat box below to instantly utilize native browser Speech-to-Text with zero cloud latency!*")
    
# --- THREE-TAB HACKATHON ARCHITECTURE ---
tab1, tab2, tab3 = st.tabs(["💬 Chat Tutor", "📝 Adaptive Quiz", "📊 My Report Card"])

# Initialize Session Globals
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []

with tab1:
    st.markdown("### Ask a question about your syllabus:")

    # Display previous messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Wait for Student Input
    if prompt := st.chat_input(f"E.g., What are microorganisms? (Speaking in {language} is supported)"):
        
        # 1. Show user message instantly
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 2. Generate the AI Answer
        with st.chat_message("assistant"):
            with st.spinner("Searching the NCERT Database..."):
                agent = VidyarthiAgent(spark) 
                response = agent.ask_tutor(prompt)
                
                # 3. Log the RAW chat reliably into the Delta Lake raw_chat_history
                log_chat_to_db(spark, st.session_state.session_id, user_id, prompt, response)
            
            st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response})

with tab2:
    st.header("📝 Generate Adaptive Knowledge Quiz")
    st.markdown("Generates a dynamic 5-question logic quiz based on the exact topics you have been analyzing in the Chat Tutor.")
    
    if st.button("Generate Quiz Now!"):
        topic_messages = [m['content'] for m in st.session_state.messages if m['role'] == 'user']
        if not topic_messages:
            st.warning("Please ask the tutor at least one question first so we know what to test you on!")
        else:
            with st.spinner("Sarvam-105b is analyzing your chat history and writing your custom quiz..."):
                topics = ", ".join(topic_messages[-3:]) # Only use last 3 topics so it stays hyper-relevant
                agent = VidyarthiAgent(spark)
                quiz_data = agent.generate_quiz(topics)
                if quiz_data and isinstance(quiz_data, list):
                    st.session_state.current_quiz = quiz_data
                    st.session_state.quiz_submitted = False
                else:
                    st.error("Network hiccup generating JSON from Sarvam. Please try again.")

    if "current_quiz" in st.session_state:
        st.markdown("---")
        user_answers = []
        correct_answers = []
        questions_text = []
        
        with st.form("quiz_form"):
            for i, q in enumerate(st.session_state.current_quiz):
                st.markdown(f"**Q{i+1}: {q['question']}**")
                choice = st.radio("Select Answer:", q['options'], key=f"q_radio_{i}")
                user_answers.append(choice)
                correct_answers.append(q['answer'])
                questions_text.append(q['question'])
                st.markdown("---")
                
            submitted = st.form_submit_button("Submit Quiz for AI Evaluation")
            if submitted and not st.session_state.get("quiz_submitted", False):
                with st.spinner("sarvam-2b is mathematically evaluating your psychology based on your wrong answers..."):
                    agent = VidyarthiAgent(spark)
                    score, strong, weak = submit_quiz_and_update_memory(
                        spark, agent.headers, user_id, class_level, 
                        user_answers, correct_answers, questions_text
                    )
                    st.session_state.quiz_submitted = True
                    st.success(f"Quiz Complete! You scored {score} out of {len(st.session_state.current_quiz)}.")
                    st.info(f"Verified Strong Point: {strong}")
                    st.warning(f"Verified Weak Point: {weak}")
                    st.markdown("**Your Databricks Report Card has been permanently updated! Go check Tab 3.**")

with tab3:
    st.header(f"📈 Performance Analytics for {user_id}")
    st.markdown("This dashboard reads directly from the Databricks **Delta Lake** memory table.")
    
    if st.button("Refresh Dashboards"):
        if spark:
            try:
                # Query the Delta Lake table directly!
                df = spark.sql(f"""
                    SELECT strong_points, weak_points 
                    FROM bharat_bricks_sol.default.user_memory 
                    WHERE user_id = '{user_id}'
                """).toPandas()
                
                if not df.empty:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.success("### 💪 Strong Points")
                        st.write(df['strong_points'].iloc[-1])
                    with col2:
                        st.error("### ⚠️ Needs Improvement")
                        st.write(df['weak_points'].iloc[-1])
                else:
                    st.info("Take the Quiz in Tab 2 to generate your verified psychological profile!")
            except Exception as e:
                st.warning("Table not fully initialized yet in Databricks.")
