# ingest_pdf.py
import pdfplumber
import json
import os
from pathlib import Path
from typing import List

PROCESSED_DIR = Path("data")
PROCESSED_DIR.mkdir(exist_ok=True)

PROCESSED_JSON = PROCESSED_DIR / "processed_docs.json"

def extract_text_from_pdf(path: str) -> str:
    text_chunks: List[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            # simple normalization
            page_text = page_text.strip()
            if page_text:
                text_chunks.append(page_text)
    return "\n\n".join(text_chunks)

def chunk_text(text: str, chunk_size: int = 400, overlap: int = 50):
    """
    Very simple chunker: split text into chunks of about chunk_size words (not characters).
    Returns list of chunks.
    """
    words = text.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk_words = words[i:i+chunk_size]
        chunks.append(" ".join(chunk_words))
        i += chunk_size - overlap
    return chunks

def save_processed_doc(doc: dict):
    if PROCESSED_JSON.exists():
        with open(PROCESSED_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    # Avoid duplicates by id
    existing = [d for d in existing if d.get("id") != doc.get("id")]
    existing.append(doc)

    with open(PROCESSED_JSON, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

def ingest_pdf_path(path: str, doc_id: str = None, source: str = None):
    """
    Given a local PDF path, extract text, chunk it, and save as processed_doc entries.
    Returns list of chunk dicts.
    """
    if doc_id is None:
        doc_id = Path(path).stem
    if source is None:
        source = path

    text = extract_text_from_pdf(path)
    chunks = chunk_text(text)

    processed_chunks = []
    for idx, c in enumerate(chunks):
        processed_chunks.append({
            "id": f"{doc_id}_chunk{idx+1}",
            "doc_id": doc_id,
            "text": c,
            "source": source
        })

    save_processed_doc({"id": doc_id, "chunks": processed_chunks, "source": source})
    return processed_chunks

if __name__ == "__main__":
    # quick local test (will run if you call python ingest_pdf.py)
    sample = "/mnt/data/Embedded LLM Track.pdf"
    if Path(sample).exists():
        print("Found sample file at", sample)
        chunks = ingest_pdf_path(sample)
        print("Created", len(chunks), "chunks.")
    else:
        print("No sample file found at", sample)