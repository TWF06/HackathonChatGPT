# --- simple_logger.py ---
def log_query(query: str, answer: str, sources: list):
    """Mocks the logging function."""
    print(f"\n[LOG] Query: {query}")
    print(f"[LOG] Answer Snippet: {answer[:50]}...")
    print(f"[LOG] Sources: {sources}\n")