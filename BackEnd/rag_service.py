# rag_service.py

import pandas as pd
import json
import os
from pathlib import Path
from typing import List, Dict, Any
import re 
# from google import genai 
# from google.genai.errors import APIError

# rag_service.py: Configuration Block (Lines 8-20)

# --- CONFIGURATION (Must match your file names) ---
ENGLISH_FILE = 'english_prompt_final.parquet'    # <--- CHANGED
MALAY_FILE = 'malay_prompt_final.parquet'        # <--- CHANGED
FLOOR_DATA_FILE = 'FLOOR DATA SET.parquet'       # <--- NEW
PROCESSED_JSON = Path("data") / "processed_docs.json"

# Column names from your parquet files (These column names are assumed to be unchanged)
ENGLISH_QUERY_COL = 'input_fromuser'
ENGLISH_ANSWER_COL = 'answer_queries'
MALAY_QUERY_COL = 'Input_penerima'
MALAY_ANSWER_COL = 'pengesahan_khabarangin'

# CRITICAL: You must inspect the column names of your new FLOOR DATA SET.parquet
# We assume the main knowledge text is in a column named 'Text Embedding' or similar.
FLOOR_KNOWLEDGE_COL = 'Text' # <--- VERIFY THIS COLUMN NAME
# --------------------------------------------------
DOCUMENTS: List[Dict[str, Any]] = []


def normalize_text(text: str) -> str:
    """
    Normalizes text by lowercasing, stripping, and handling common dashes/spaces/PDF artifacts.
    """
    if not isinstance(text, str):
        text = str(text) 

    # 1. Aggressive cleaning for RAG-specific issues (quotes, PDF glyphs, etc.)
    text = text.strip().lower()
    
    # Fix 1: Safer regex to remove surrounding quotes and backslashes
    text = re.sub(r'^["\']|[\\"\']$', '', text) 
    
    # Fix 2: Cleaner regex for removing common PDF bullet/glyph characters and leading numbers/headers
    text = re.sub(r'^\s*(\[.*?\]|\S*\.)\s*', '', text) 
    
    # 3. Simple cleanup for common PDF/OCR artifacts
    text = text.replace('\n', ' ').replace('\r', ' ').replace('(cid:127)', '•')
    
    # 4. Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_and_extract_context(full_text: str, query: str) -> str:
    # Placeholder for more sophisticated context extraction logic.
    return full_text

def load_documents_from_parquets(english_file: str, malay_file: str):
    """
    Loads Q&A data from the Parquet files into the global DOCUMENTS list.
    """
    global DOCUMENTS
    
    # 1. Load English data
    try:
        df_en = pd.read_parquet(english_file, engine='pyarrow')
        for _, row in df_en.iterrows():
            question = row[ENGLISH_QUERY_COL]
            answer = row[ENGLISH_ANSWER_COL]
            
            # Combine Q&A into a single searchable chunk
            full_text = f"Question: {question}. Answer: {answer}"
            
            DOCUMENTS.append({
                "id": f"en_qa_{_}",
                "doc_id": "Parquet_English",
                "text": full_text,
                "text_normalized": normalize_text(full_text),
                "source": "Parquet_English"
            })
        print(f"--- [RAG Service] Loaded {len(df_en)} English Q&A pairs. ---")
    except Exception as e:
        print(f"--- [RAG Service] Error loading English Parquet: {e} ---")

    # 2. Load Malay data
    try:
        df_ms = pd.read_parquet(malay_file, engine='pyarrow')
        for _, row in df_ms.iterrows():
            question = row[MALAY_QUERY_COL]
            answer = row[MALAY_ANSWER_COL]
            
            full_text = f"Soalan: {question}. Jawapan: {answer}"
            
            DOCUMENTS.append({
                "id": f"ms_qa_{_}",
                "doc_id": "Parquet_Malay",
                "text": full_text,
                "text_normalized": normalize_text(full_text),
                "source": "Parquet_Malay"
            })
        print(f"--- [RAG Service] Loaded {len(df_ms)} Malay Q&A pairs. ---")
    except Exception as e:
        print(f"--- [RAG Service] Error loading Malay Parquet: {e} ---")

# 3. Load the new FLOOR DATA SET.parquet (NEW KNOWLEDGE CHUNKS)
    try:
        df_floor = pd.read_parquet(FLOOR_DATA_FILE, engine='pyarrow') 
        floor_count = 0

        for _, row in df_floor.iterrows():
            # Use the correct column name 'Text'
            if FLOOR_KNOWLEDGE_COL in row and pd.notna(row[FLOOR_KNOWLEDGE_COL]):
                full_text = str(row[FLOOR_KNOWLEDGE_COL])
                
                DOCUMENTS.append({
                    "id": f"floor_chunk_{_}",
                    "doc_id": "Parquet_FloorData",
                    "text": full_text,
                    "text_normalized": normalize_text(full_text),
                    "source": "Parquet_FloorData"
                })
                floor_count += 1

        print(f"--- [RAG Service] Loaded {floor_count} knowledge chunks from FLOOR DATA SET. ---")
    except Exception as e:
        print(f"--- [RAG Service] Error loading FLOOR DATA SET Parquet: {e} ---")
        print(f"--- [RAG Service] TIP: Check if the column name '{FLOOR_KNOWLEDGE_COL}' is correct for this file. ---")
        
def load_documents_from_processed(processed_json_path: Path = PROCESSED_JSON):
    """
    Loads all document chunks (Parquet and PDF) into the global list.
    FIXED: Uses encoding="utf-8" to prevent UnicodeDecodeError.
    """
    global DOCUMENTS
    DOCUMENTS = []
    
    # 1. Load Parquet Data (Q&A)
    load_documents_from_parquets(ENGLISH_FILE, MALAY_FILE)
    
    # 2. Load PDF Chunks from processed_docs.json
    try:
        if processed_json_path.exists():
            # === CRITICAL FIX (UnicodeDecodeError) ===
            with processed_json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            
            pdf_chunk_count = 0
            for doc in data:
                for chunk in doc.get("chunks", []):
                    # For RAG, we need a normalized text field
                    chunk['text_normalized'] = normalize_text(chunk.get("text", ""))
                    chunk['source'] = doc['id']
                    DOCUMENTS.append(chunk)
                    pdf_chunk_count += 1

            print(f"--- [RAG Service] Loaded {pdf_chunk_count} PDF chunks for RAG. ---")
            
    except (json.JSONDecodeError, FileNotFoundError, UnicodeDecodeError) as e:
        print(f"--- [RAG Service] Error loading processed documents: {e} ---")
        print("--- [RAG Service] TIP: If this is a UnicodeDecodeError, please delete 'data/processed_docs.json' and restart. ---")
        
    print(f"--- [RAG Service] TOTAL LOADED DOCUMENTS: {len(DOCUMENTS)} ---")


def search_documents(query: str, language: str) -> Dict[str, Any]:
    """
    Searches the loaded documents using simple keyword matching (40% term overlap).
    """
    
    query_terms = normalize_text(query).split()
    
    for doc in DOCUMENTS:
        matches = 0
        doc_text = doc.get("text_normalized", "")
        
        for term in query_terms:
            if term in doc_text:
                matches += 1
        
        # Condition 1: Check if a significant portion of the query terms match
        if len(query_terms) > 0 and (matches / len(query_terms)) >= 0.40:
            print(f"--- [RAG Service]: Flexible Keyword match found ({(matches/len(query_terms))*100:.0f}%) for query: {query} ---")
            
            relevant_context = clean_and_extract_context(doc["text"], query)
            
            return {
                "retrieved_context": relevant_context,
                "source": doc["source"],
                "query": query,
                "language": language
            }

    # Fallback: No relevant context found
    return {
        "retrieved_context": "No relevant context found in documents.",
        "source": "N/A",
        "query": query,
        "language": language
    }


def get_simulated_llm_response(search_result: Dict[str, Any], prompt_templates: Dict[str, Any]) -> str:
    """
    Returns the retrieved context as the 'golden answer' with bilingual fallbacks.
    """
    context = search_result['retrieved_context']
    source = search_result['source']
    lang = search_result['language']
    
    if "No relevant context found" not in context:
        
        # --- FALLBACK: Use the original simulated response if LLM is not connected ---
        return f"[LLM GENERATION (Simulated)]: The LLM has processed the context. For now, here is the raw context: {context} (Source: {source})"
        
    # Condition 3: Fallback (No Context Found in search_documents)
    if lang == "ms":
        return "Saya tidak dapat mencari maklumat yang berkaitan dengan soalan anda dalam dokumen yang disediakan. (Sumber: N/A)"
    else:
        return "I could not find information relevant to your question in the documents provided. (Source: N/A)"

# --- INITIALIZATION ---
# load_documents_from_processed() # This is called in app.py now
# ----------------------