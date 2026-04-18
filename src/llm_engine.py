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
        # get_relevant_context now returns (context_string, sources_list)
        context, sources = self.retriever.get_relevant_context(question, top_k=5)

        if "No relevant context found" in context:
            return "Mujhe khed hai, par yeh jankari Class 8 NCERT mein nahi mili.", []

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
            "max_tokens": 4095,
            "temperature": 0.2
        }

        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()
            msg = response.json()['choices'][0]['message']

            # Reasoning models (like o1 or sarvam-105b) often use 'reasoning_content' first
            final_text = msg.get('content') or msg.get('reasoning_content') or "Error: Empty response"
            return final_text.strip(), sources
        except Exception as e:
            return f"Error contacting Sarvam API: {str(e)} \n\nRaw Output: {response.text}", []
            
    def generate_quiz(self, topics):
        import json
        print("🧠 Generating 5-question logic quiz from topics...")
        
        prompt = f"""You are an expert exam creator. The student has been asking about these topics: {topics}.
Generate a 5-question multiple choice quiz testing their knowledge on these specific topics.

CRITICAL INSTRUCTION: You MUST output ONLY valid JSON. No markdown, no backticks, no introduction. Just the JSON array.
Format MUST strictly be a JSON list of objects exactly like this string format:
[
  {{"question": "What is...", "options": ["A. Full text", "B. Full text", "C. Full text", "D. Full text"], "answer": "B"}},
  {{"question": "Why does...", "options": ["A. Explanation", "B. Explanation", "C. Explanation", "D. Explanation"], "answer": "A"}}
]
!!! Make sure the text inside 'options' contains the actual descriptive choices, NOT just the letter!
"""
        payload = {
            "model": "sarvam-105b", 
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4095,
            "temperature": 0.1
        }
        
        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()
            msg = response.json()['choices'][0]['message']
            raw_text = msg.get('content') or msg.get('reasoning_content') or "[]"
            
            # Robust JSON extraction: LLMs often add text before/after the JSON
            import re
            import random
            match = re.search(r'\[.*\]', raw_text, re.DOTALL)
            if match:
                clean_json_str = match.group(0)
                quiz_data = json.loads(clean_json_str)
                
                # POST-PROCESSING: Shuffle options to prevent "A is always correct"
                for q in quiz_data:
                    opt_list = q.get('options', [])
                    ans_letter = q.get('answer', '').strip()
                    
                    if len(opt_list) >= 2 and ans_letter:
                        # Find the full text of the correct option
                        correct_full = next((o for o in opt_list if o.startswith(ans_letter)), None)
                        
                        if correct_full:
                            # Strip the "A. " / "B) " prefix from all options
                            raw_opts = [re.sub(r'^[A-Z][\.\)]\s*', '', o) for o in opt_list]
                            correct_raw = re.sub(r'^[A-Z][\.\)]\s*', '', correct_full)
                            
                            # Shuffle the underlying answers
                            random.shuffle(raw_opts)
                            
                            # Re-apply A, B, C, D in their new random positions
                            letters = ['A', 'B', 'C', 'D', 'E', 'F']
                            new_opts = []
                            for idx, raw in enumerate(raw_opts):
                                letter = letters[idx % len(letters)]
                                new_opts.append(f"{letter}. {raw}")
                                # If this was our correct answer, update the master key
                                if raw == correct_raw:
                                    q['answer'] = letter
                                    
                            q['options'] = new_opts
                            
                return quiz_data
            else:
                return []
        except Exception as e:
            print(f"Quiz Generation Failed: {e}\nRaw Output: {raw_text if 'raw_text' in locals() else 'None'}")
            return []

    def synthesize_evaluations(self, paragraphs):
        print("🧠 Synthesizing multiple quiz evaluations into a master report card...")
        
        if not paragraphs:
            return "No quizzes taken in this session yet."
            
        paragraphs_text = "\n\n".join([f"Quiz {i+1} Output:\n{p}" for i, p in enumerate(paragraphs)])
        
        prompt = f"""You are an expert Educational Psychologist. Below are several individual quiz evaluations for a student from their current study session.

{paragraphs_text}

Synthesize all of these evaluations into ONE highly professional, 4-5 sentence Comprehensive Session Report Card paragraph.
1. Highlight their overarching strengths across these quizzes.
2. Identify any recurring vulnerabilities, confusions, or blind spots.
3. Provide a master recommendation for their next strategic learning steps.

Write directly to the student (use "you"). Be specific to the actual topics discussed. Do not output anything except the final synthesized paragraph."""

        payload = {
            "model": self.model_id, 
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4095,
            "temperature": 0.3
        }
        
        try:
            response = requests.post(self.url, headers=self.headers, json=payload, timeout=90)
            response.raise_for_status()
            msg = response.json()['choices'][0]['message']
            content = msg.get('content') or msg.get('reasoning_content') or ""
            
            import re
            clean_text = re.sub(r'```.*?```', '', content, flags=re.DOTALL).strip()
            return clean_text
        except Exception as e:
            print(f"Synthesis Failed: {e}")
            return "Failed to generate comprehensive report. Please try again."
