# src/llm_engine.py
# This script bridges the Retrieval system and the SARVAM AI API.

import requests
import json
import sys

try:
    from src.retrieval import VidyarthiRetriever
except ImportError:
    pass

class VidyarthiAgent:
    def __init__(self, spark_session):
        self.retriever = VidyarthiRetriever(spark_session)
        
        self.SARVAM_API_KEY = "sk_j5e1dzr6_cPtyc8I9SvTfFPnDpK8kQPRB"
        self.url = "https://api.sarvam.ai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.SARVAM_API_KEY}",
            "api-subscription-key": self.SARVAM_API_KEY,
            "Content-Type": "application/json"
        }
        
        # Using Sarvam-105b because it is the state-of-the-art for Indic languages!
        self.model_id = "sarvam-105b"

    def ask_tutor(self, question):
        print("🔍 Step 1: Searching NCERT Database...")
        context = self.retriever.get_relevant_context(question, top_k=5)
        
        if "No relevant context found" in context:
            return "Mujhe khed hai, par yeh jankari Class 8 NCERT mein nahi mili."

        # Keep the prompt extremely strict for hackathon judging
        prompt = f"""You are a helpful Indian university teaching assistant. Use the provided Context Blocks to answer the student's question comprehensively.
            
CRITICAL INSTRUCTION 1: You MUST synthesize the knowledge from ALL the provided Context Blocks. At the end of your answer, you MUST list the exact [Source: ...] tags for EVERY Context Block you used in a neat list. DO NOT make up information.
CRITICAL INSTRUCTION 2: You MUST detect the language of the student's question and write your final response ENTIRELY in that exact same language (e.g. Hindi, English, etc).

### CONTEXT BLOCKS:
{context}

### STUDENT QUESTION:
{question}
"""
        
        print(f"🤖 Step 2: Generating Answer with {self.model_id}...")
        
        payload = {
            "model": self.model_id,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4095, # Increased massively because it is a reasoning model!
            "temperature": 0.2
        }
        
        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status() # Raise exception if bad status code
            msg = response.json()['choices'][0]['message']
            
            # Reasoning models (like o1 or sarvam-105b) often use 'reasoning_content' first
            final_text = msg.get('content') or msg.get('reasoning_content') or "Error: Empty response"
            return final_text.strip()
        except Exception as e:
            return f"Error contacting Sarvam API: {str(e)} \n\nRaw Output: {response.text}"
