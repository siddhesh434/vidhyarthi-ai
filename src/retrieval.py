# src/retrieval.py
# This script handles grabbing the smartest possible textbook chunks for a student's question.

import faiss
import json
import numpy as np
from sentence_transformers import SentenceTransformer

class VidyarthiRetriever:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
        self.encoder = SentenceTransformer(self.model_name)
        
        # Paths where we saved our FAISS DB and map
        import os
        
        # Check if the files are physically in the same folder as app.py first!
        # If they are, it bypasses Databricks Volume security entirely.
        if os.path.exists("ncert_faiss.bin"):
            self.faiss_path = "ncert_faiss.bin"
            self.mapping_path = "ncert_chunk_mapping.json"
        elif os.path.exists("src/ncert_faiss.bin"):
            self.faiss_path = "src/ncert_faiss.bin"
            self.mapping_path = "src/ncert_chunk_mapping.json"
        else:
            # Fallback to Volume
            self.faiss_path = "/Volumes/bharat_bricks_sol/default/raw_data/ncert_faiss.bin"
            self.mapping_path = "/Volumes/bharat_bricks_sol/default/raw_data/ncert_chunk_mapping.json"
        
        # Load the index into memory
        self.index = faiss.read_index(self.faiss_path)
        with open(self.mapping_path, "r") as f:
            self.mapping = json.load(f)
            
    def get_relevant_context(self, user_question, top_k=3):
        """
        Embeds the student's question, finds the 3 mathematically closest paragraphs,
        and retrieves their actual English/Hindi/Urdu text from the Delta Table.
        """
        # 1. Turn the completely new question into a vector
        question_vector = self.encoder.encode([user_question], convert_to_numpy=True)
        
        # 2. Search FAISS for the nearest chunks
        distances, indices = self.index.search(question_vector, top_k)
        
        # 3. Get the actual Chunk IDs from our JSON map
        retrieved_chunk_ids = []
        for idx in indices[0]:
            if idx != -1: # Ensure it found a valid match
                retrieved_chunk_ids.append(self.mapping[str(idx)])
                
        if not retrieved_chunk_ids:
            return "No relevant context found in textbooks."
            
        # 4. Fetch the real text and metadata straight from Databricks Delta Lake!
        chunk_ids_sql = ",".join([f"'{cid}'" for cid in retrieved_chunk_ids])
        
        query = f"""
            SELECT text_content, class_level, chapter, page_number 
            FROM bharat_bricks_sol.default.ncert_gold_chunks 
            WHERE chunk_id IN ({chunk_ids_sql})
        """
        
        results = self.spark.sql(query).collect()
        
        # 5. Format it beautifully so the LLM enforces citations
        formatted_contexts = []
        for r in results:
            chap_str = str(r['chapter'])
            chap_num = chap_str[-2:] if len(chap_str) >= 2 else chap_str
            
            context = (
                f"[Source: Class {r['class_level']} Science, Chapter {chap_num}, Page {r['page_number']}]\n"
                f"TEXT: {r['text_content']}"
            )
            formatted_contexts.append(context)
            
        # Combine into one massive context block for the LLM
        return "\n\n".join(formatted_contexts)

# Quick Test (If you run this directly in a notebook cell):
# retriever = VidyarthiRetriever(spark)
# context = retriever.get_relevant_context("What are microorganisms?")
# print(context)
