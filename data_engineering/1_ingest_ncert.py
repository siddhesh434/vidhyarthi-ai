# data_engineering/1_ingest_ncert.py
# This script is designed to be pasted entirely into a Databricks Notebook cell.
# Make sure to install dependencies on your cluster first by running:
# %pip install PyMuPDF

import os
import uuid
import fitz  # PyMuPDF
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- CONFIGURATION ---
# In Databricks, uploaded files to volumes look like: /Volumes/main/default/hackathon_data/hecu1dd/
# Since we are prepping locally, we use a placeholder path that you will change in Databricks.
RAW_DATA_PATH = "/Volumes/bharat_bricks_sol/default/raw_data"
# Using the exact 3-level Unity Catalog namespace so it doesn't get lost in 'workspace'
TARGET_TABLE = "bharat_bricks_sol.default.ncert_gold_chunks"
CHUNK_SIZE_CHARS = 1000
CHUNK_OVERLAP_CHARS = 200

def extract_and_chunk(pdf_path, filename):
    """Reads a PDF, extracts page-by-page text, and strictly maps the metadata."""
    doc = fitz.open(pdf_path)
    records = []
    
    # Simple heuristic: hecu101.pdf -> Chapter 1
    # We strip the non-numeric parts for the chapter name.
    chapter = filename.replace(".pdf", "")
    
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        text = page.get_text("text").replace("\n", " ").strip()
        
        if not text:
            continue
            
        # Standard sliding window chunking to preserve context across paragraphs
        for i in range(0, len(text), CHUNK_SIZE_CHARS - CHUNK_OVERLAP_CHARS):
            chunk = text[i : i + CHUNK_SIZE_CHARS]
            
            # The Metadata row! This guarantees citations.
            record = {
                "chunk_id": str(uuid.uuid4()),
                "class_level": 8, # Updating to Class 8
                "language": "English", # Update logic if multiple languages are mixed
                "chapter": chapter,
                "page_number": page_num + 1, # +1 because index is 0-based
                "text_content": chunk
            }
            records.append(record)
            
    return records

# --- EXECUTION ---
print("Scanning Directory for NCERT PDFs...")
all_chunks = []

# Loop through the raw_data folder
for filename in os.listdir(RAW_DATA_PATH):
    if filename.endswith(".pdf"):
        file_path = os.path.join(RAW_DATA_PATH, filename)
        print(f"Processing {filename}...")
        chunks_for_file = extract_and_chunk(file_path, filename)
        all_chunks.extend(chunks_for_file)

print(f"Total Chunks Generated: {len(all_chunks)}")

# --- DATABRICKS DELTA LAKE WRITE ---
print(f"Writing into Delta Table: {TARGET_TABLE}")

# Define the precise schema to match our database_setup.sql exactly
schema = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("class_level", IntegerType(), True),
    StructField("language", StringType(), True),
    StructField("chapter", StringType(), True),
    StructField("page_number", IntegerType(), True),
    StructField("text_content", StringType(), True)
])

# Create a PySpark DataFrame
# (spark is globally available inside a Databricks Notebook)
df = spark.createDataFrame(all_chunks, schema=schema)

# Perform the Delta sink Write!
df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

print("Ingestion Complete! The NCERT Gold Chunks are safely in Delta Lake.")
