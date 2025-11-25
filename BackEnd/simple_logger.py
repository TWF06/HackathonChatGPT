# simple_logger.py

import logging
import json
import datetime
import sys 
from pathlib import Path

# --- CONFIGURATION ---
LOG_FILE = Path("query_logs.txt")

# Configure the root logger
logger = logging.getLogger('SmartFloodAssistant')
logger.setLevel(logging.INFO)

# Create a custom formatter to output JSON
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "extras": record.__dict__.get('extras', {})
        }
        return json.dumps(log_data)

# Ensure the log file can be opened and written to (using full path for safety)
try:
    # Use encoding="utf-8" for safety when writing to file
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8") 
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
except Exception as e:
    print(f"Warning: Could not set up file logger: {e}")

# Create a stream handler for console output
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

# Add the handlers to the logger
logger.addHandler(stream_handler)
# ---------------------

def log_query(query: str, answer: str, sources: list):
    """
    Logs query details in a structured JSON format to the log file.
    """
    
    logger.info(
        "User query processed successfully.",
        extra={
            'extras': {
                "query": query,
                "answer_preview": answer[:150] + "..." if len(answer) > 150 else answer,
                "sources": sources
            }
        }
    )