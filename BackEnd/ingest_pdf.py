# --- ingest_pdf.py ---
def ingest_pdf_path(file_path: str, source: str = None) -> list:
    """Mocks the PDF ingestion and chunking process."""
    print(f"--- [INGESTION] Mocking PDF ingestion for: {file_path} ---")
    
    # Returns mock chunks (IDs are required by app.py)
    return [{"id": "chunk_a1"}, {"id": "chunk_a2"}]