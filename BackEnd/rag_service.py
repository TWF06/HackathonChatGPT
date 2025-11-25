# --- rag_service.py (Integrated with Parquet Data) ---

import pandas as pd
import os
from typing import List, Dict, Any

# --- CONFIGURATION (Must match your file names) ---
ENGLISH_FILE = 'english_prompt (4).parquet'
MALAY_FILE = 'malay_prompt (1).parquet'

# These columns are derived from your prompt engineer's file structure (from extract_prompts.py)
# We map the user's query to a key in the RAG data, and the answer to the text content.
ENGLISH_QUERY_COL = 'input_fromuser'
ENGLISH_ANSWER_COL = 'answer_queries' # Using this column for the RAG context/answer

MALAY_QUERY_COL = 'Input_penerima'
MALAY_ANSWER_COL = 'pengesahan_khabarangin' # Using this column for the RAG context/answer
# --------------------------------------------------

DOCUMENTS: List[Dict[str, Any]] = []


def load_documents_from_processed() -> List[Dict[str, Any]]:
    """
    Loads data from the English and Malay Parquet files and formats it for RAG.
    """
    global DOCUMENTS
    
    loaded_docs = []
    
    # 1. Load English Data
    if os.path.exists(ENGLISH_FILE):
        try:
            df_en = pd.read_parquet(ENGLISH_FILE, engine='pyarrow')
            
            # Combine the relevant columns into a list of dictionaries
            english_docs = [
                {
                    "query": row[ENGLISH_QUERY_COL].strip().lower(), # Store query for lookup
                    "text": row[ENGLISH_ANSWER_COL].strip(),       # Store answer as RAG context
                    "source": f"Source: {ENGLISH_FILE}", 
                    "language": "en"
                }
                # Filter out rows where the answer or query column is empty/missing
                for _, row in df_en.dropna(subset=[ENGLISH_QUERY_COL, ENGLISH_ANSWER_COL]).iterrows()
            ]
            loaded_docs.extend(english_docs)
            print(f"--- [RAG Service]: Loaded {len(english_docs)} English records from {ENGLISH_FILE}. ---")
        except Exception as e:
            print(f"--- [RAG Service ERROR]: Failed to load English Parquet: {e} ---")

    # 2. Load Malay Data
    if os.path.exists(MALAY_FILE):
        try:
            df_ms = pd.read_parquet(MALAY_FILE, engine='pyarrow')
            
            # Combine the relevant columns into a list of dictionaries
            malay_docs = [
                {
                    "query": row[MALAY_QUERY_COL].strip().lower(), # Store query for lookup
                    "text": row[MALAY_ANSWER_COL].strip(),       # Store answer as RAG context
                    "source": f"Source: {MALAY_FILE}", 
                    "language": "ms"
                }
                # Filter out rows where the answer or query column is empty/missing
                for _, row in df_ms.dropna(subset=[MALAY_QUERY_COL, MALAY_ANSWER_COL]).iterrows()
            ]
            loaded_docs.extend(malay_docs)
            print(f"--- [RAG Service]: Loaded {len(malay_docs)} Malay records from {MALAY_FILE}. ---")
        except Exception as e:
            print(f"--- [RAG Service ERROR]: Failed to load Malay Parquet: {e} ---")
            
    DOCUMENTS = loaded_docs
    return DOCUMENTS

# Initialize DOCUMENTS globally when the module loads
DOCUMENTS = load_documents_from_processed()


def search_documents(query: str, language: str) -> Dict[str, Any]:
    """
    Simulates RAG retrieval by performing an exact match search against the
    user input column loaded from the parquet files.
    """
    
    query_lower = query.strip().lower()
    
    # Simple retrieval: Find the first document where the loaded query matches the user's input
    for doc in DOCUMENTS:
        if doc["query"] == query_lower:
            print(f"--- [RAG Service]: Exact match found for query: {query} ---")
            return {
                "retrieved_context": doc["text"],
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
    Since the parquet data contains the 'golden answer,' we just return the retrieved text.
    """
    context = search_result['retrieved_context']
    source = search_result['source']
    lang = search_result['language']
    
    if "No relevant context found" not in context:
        # Return the 'golden answer' directly, appending the source
        return f"{context} (Source: {source})"
        
    # Condition 3: Fallback (No Context)
    if lang == "ms":
        return "Saya tidak dapat mencari maklumat yang berkaitan dengan soalan anda dalam dokumen yang disediakan. (Sumber: N/A)"
    else:
        return "I could not find relevant information for your query in the provided documents. (Source: N/A)"