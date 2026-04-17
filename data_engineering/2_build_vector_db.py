# data_engineering/2_build_vector_db.py
# Paste this code into a new Databricks Notebook cell.
# Make sure your notebook has the required libraries installed:
# %pip install sentence-transformers faiss-cpu
# dbutils.library.restartPython()

import faiss
import json
import numpy as np
from sentence_transformers import SentenceTransformer

# --- CONFIGURATION ---
SOURCE_TABLE = "bharat_bricks_sol.default.ncert_gold_chunks"
# We save the FAISS database right next to your PDFs in the same Volume!
FAISS_INDEX_PATH = "/Volumes/bharat_bricks_sol/default/raw_data/ncert_faiss.bin"
MAPPING_PATH = "/Volumes/bharat_bricks_sol/default/raw_data/ncert_chunk_mapping.json"

# We use this multilingual model because it is tiny (perfect for Free Tier CPU) 
# but understands English, Hindi, and Urdu simultaneously!
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

print(f"Loading embedding model: {MODEL_NAME}...")
model = SentenceTransformer(MODEL_NAME)

print(f"Querying {SOURCE_TABLE} to get chunks...")
# We do a 'spark.sql' to pull your 703 chunks back out of the database
df = spark.sql(f"SELECT chunk_id, text_content FROM {SOURCE_TABLE}")
rows = df.collect()

chunk_ids = []
texts = []
for r in rows:
    chunk_ids.append(r["chunk_id"])
    texts.append(r["text_content"])

print(f"Embedding {len(texts)} chunks (This might take ~1 minute on CPU)...")
# Encode all text chunks into mathematical vectors
embeddings = model.encode(texts, convert_to_numpy=True)

# --- BUILD FAISS VECTOR DB ---
print("Building FAISS Memory Index...")
vector_dimension = embeddings.shape[1] 
# IndexFlatL2 is extremely fast for our small dataset
index = faiss.IndexFlatL2(vector_dimension) 
index.add(embeddings)

# FAISS only understands row numbers (0, 1, 2...), so we must save a mapping 
# that associates Row #0 with your actual 'chunk_id' from the database.
mapping_dict = {i: chunk_id for i, chunk_id in enumerate(chunk_ids)}

# --- SAVE TO VOLUME ---
print(f"💾 Saving FAISS index to {FAISS_INDEX_PATH}...")
faiss.write_index(index, FAISS_INDEX_PATH)

print(f"💾 Saving Mapping Dictionary to {MAPPING_PATH}...")
with open(MAPPING_PATH, "w") as f:
    json.dump(mapping_dict, f)

print("✅ Success! Your textbook vectors are now saved and ready to be searched by the LLM.")
