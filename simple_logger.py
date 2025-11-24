import json
from datetime import datetime

def log_query(query, answer, sources):
    log_entry = {
        "timestamp": str(datetime.now()),
        "query": query,
        "answer_preview": answer[:50] + "...",
        "sources": sources
    }

    with open("query_logs.txt", "a") as f:
        f.write(json.dumps(log_entry) + "\n")
