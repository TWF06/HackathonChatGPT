import json
from pathlib import Path
from typing import Dict, Any

PROCESSED_JSON_PATH = Path("data/processed_docs.json")

def load_documents_from_processed():
    docs = []
    if PROCESSED_JSON_PATH.exists():
        with open(PROCESSED_JSON_PATH, "r", encoding="utf-8") as f:
            all_docs = json.load(f)
        # all_docs is a list of {id, chunks, source}
        for d in all_docs:
            for chunk in d.get("chunks", []):
                docs.append({
                    "id": chunk["id"],
                    "text": chunk["text"],
                    "source": chunk.get("source", d.get("source", "N/A"))
                })
    else:
        # fallback minimal doc if nothing processed yet
        docs = [
            {
                "id": "doc1",
                "text": "Malaysia is pushing digitalisation (digitalisasi) under the MyDIGITAL blueprint. Many organisations face issues (isu) managing SOP information (maklumat SOP).",
                "source": "/mnt/data/Embedded LLM Track.pdf"
            }
        ]
    return docs

# Use this function whenever you need current documents:
# Note: In a real system, this would not be a global variable but loaded on startup/demand.
DOCUMENTS = load_documents_from_processed()

def search_documents(query: str) -> Dict[str, Any]:
    query_words = query.lower().split()
    docs = load_documents_from_processed()  # reload latest processed docs
    
    # Filter out short, common words that don't help the keyword search
    meaningful_query_words = [w for w in query_words if len(w) > 2 and w not in ["the", "and", "but", "what", "how", "does", "about", "for", "dalam", "yang", "ini", "itu"]]

    for doc in docs:
        doc_text = doc["text"].lower()
        match_count = sum(1 for w in meaningful_query_words if w in doc_text)
        
        # Calculate a simulated "Relevance Score" (better than simple count)
        query_word_count = len(meaningful_query_words)
        # Score is the fraction of query words found, capped at 1.0
        relevance_score = min(1.0, match_count / max(1, query_word_count)) 

        # Only consider relevant if the score is above a threshold (Simulated Vector Search threshold)
        if relevance_score >= 0.5:
            return {
                "id": doc["id"],
                "text": doc["text"],
                "source": doc["source"],
                "relevance_score": relevance_score # Added simulated score for context
            }

    return {
        "id": "N/A",
        "text": "No relevant SOP found. Please try a different query.",
        "source": "N/A"
    }

def generate_answer(query: str, retrieved_result: Dict[str, Any], language: str) -> str:
    """
    Simulates the LLM generation step using the retrieved text and the query.
    (Step 1: Generation)
    """
    lang_code = language.upper()
    retrieved_text = retrieved_result["text"]
    retrieved_source = retrieved_result["source"]

    # Handle case where no relevant document was found
    if retrieved_text == "No relevant SOP found.":
         # Use the query language for the failure message
         if language == "ms":
              return f"[{lang_code}] **PENJANAAN LLM (Simulasi)**: Tiada maklumat SOP yang relevan ditemui untuk menjawab pertanyaan anda: '{query}'. Sila cuba pertanyaan lain."
         else: # Default to English
              return f"[{lang_code}] **LLM GENERATION (Simulated)**: No relevant SOP information was found to answer your query: '{query}'. Please try a different query."
    else:
         # Simulate a successful LLM generation based on retrieved context
         text_preview = retrieved_text[:150].replace('\n', ' ')
         
         if language == "ms":
              return (
                 f"[{lang_code}] **PENJANAAN LLM (Simulasi)**: "
                 f"Saya telah mendapatkan teks SOP berikut (Sumber: {retrieved_source}): '{text_preview}...'. "
                 f"LLM kini akan menggunakan teks ini untuk menjana jawapan kepada pertanyaan: '{query}'. "
                 f"Jawapan akhir akan dijana dalam bahasa {language.upper()}."
             )
         else: # Default to English
              return (
                 f"[{lang_code}] **LLM GENERATION (Simulated)**: "
                 f"I have retrieved the following SOP text (Source: {retrieved_source}): '{text_preview}...'. "
                 f"The LLM would now use this text to answer the query: '{query}'. "
                 f"The final answer would be generated in {language.upper()}."
             )