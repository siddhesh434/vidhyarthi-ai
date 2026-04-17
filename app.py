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
from src.async_memory_updater import fire_and_forget_memory_update

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
    
# --- TWO-TAB HACKATHON ARCHITECTURE ---
tab1, tab2 = st.tabs(["💬 Chat Tutor", "📊 My Report Card"])

with tab1:
    st.markdown("### Ask a question about your syllabus:")
    
    # Initialize Chat History
    if "messages" not in st.session_state:
        st.session_state.messages = []

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
                # Initialization should happen once in a real app, placed here for hackathon simplicity
                agent = VidyarthiAgent(spark) 
                response = agent.ask_tutor(prompt)
                
                # Fire the Async Memory Updater behind the scenes
                fire_and_forget_memory_update(spark, agent.headers, user_id, class_level, prompt)
            
            st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response})

with tab2:
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
                    st.info("Ask your first question to generate your psychological profile!")
            except Exception as e:
                st.warning("Table not fully initialized yet in Databricks.")
        else:
            # Local Mock
            col1, col2 = st.columns(2)
            col1.success("### 💪 Strong Points\nShows deep curiosity about physics.")
            col2.error("### ⚠️ Needs Improvement\nStruggling slightly with cell biology concepts.")
