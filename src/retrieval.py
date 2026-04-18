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

        # Resolve PDF directory — Volume is primary (no permission issues confirmed).
        # Falls back to the local synced repo path if Volume isn't reachable.
        _vol_pdf = "/Volumes/bharat_bricks_sol/default/raw_data/hecu1dd"
        _local_pdf = "raw_data/hecu1dd"
        if os.path.isdir(_vol_pdf):
            self.pdf_base_dir = _vol_pdf
        elif os.path.exists(_local_pdf):
            self.pdf_base_dir = _local_pdf
        else:
            self.pdf_base_dir = _vol_pdf  # best-effort fallback

    def get_relevant_context(self, user_question, top_k=3):
        """
        Embeds the student's question, finds the closest NCERT paragraphs, and
        retrieves their text + metadata from Delta Lake.

        Returns:
            (context_string, sources_list) — context_string is the LLM prompt block;
            sources_list is a list of dicts with pdf_path, page_number, chapter, class_level.
        """
        # 1. Turn the question into a vector
        question_vector = self.encoder.encode([user_question], convert_to_numpy=True)

        # 2. Search FAISS for the nearest chunks
        distances, indices = self.index.search(question_vector, top_k)

        # 3. Map FAISS indices → chunk IDs
        retrieved_chunk_ids = []
        for idx in indices[0]:
            if idx != -1:
                retrieved_chunk_ids.append(self.mapping[str(idx)])

        if not retrieved_chunk_ids:
            return "No relevant context found in textbooks.", []

        # 4. Fetch real text + metadata from Delta Lake
        chunk_ids_sql = ",".join([f"'{cid}'" for cid in retrieved_chunk_ids])
        query = f"""
            SELECT text_content, class_level, chapter, page_number
            FROM bharat_bricks_sol.default.ncert_gold_chunks
            WHERE chunk_id IN ({chunk_ids_sql})
        """
        results = self.spark.sql(query).collect()

        # 5. Build formatted context blocks AND structured source metadata
        formatted_contexts = []
        sources = []
        seen_sources = set()  # deduplicate by (chapter, page)

        for r in results:
            chap_str  = str(r['chapter'])
            chap_num  = chap_str[-2:] if len(chap_str) >= 2 else chap_str
            page_num  = int(r['page_number'])
            class_lvl = r['class_level']

            # LLM context block (unchanged format — keeps citations working)
            context = (
                f"[Source: Class {class_lvl} Science, Chapter {chap_num}, Page {page_num}]\n"
                f"TEXT: {r['text_content']}"
            )
            formatted_contexts.append(context)

            # PDF filename pattern: hecu1{chap_num}.pdf  e.g. hecu101.pdf, hecu109.pdf
            pdf_filename = f"hecu1{chap_num}.pdf"
            pdf_path = f"{self.pdf_base_dir}/{pdf_filename}"

            dedup_key = (chap_num, page_num)
            if dedup_key not in seen_sources:
                seen_sources.add(dedup_key)
                sources.append({
                    "class_level": class_lvl,
                    "chapter":      chap_num,
                    "page_number":  page_num,
                    "pdf_path":     pdf_path,
                    "pdf_filename": pdf_filename,
                    "label": f"Class {class_lvl} Science — Chapter {chap_num}, Page {page_num}",
                })

        return "\n\n".join(formatted_contexts), sources

# Quick Test (If you run this directly in a notebook cell):
# retriever = VidyarthiRetriever(spark)
# context = retriever.get_relevant_context("What are microorganisms?")
# print(context)
