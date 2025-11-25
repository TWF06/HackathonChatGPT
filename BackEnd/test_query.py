# test_query.py

import requests
import json
from typing import Dict, Any

# --- Configuration ---\r\n
API_URL = "http://127.0.0.1:8000/api/query"

# --- PASTE YOUR EXACT QUERY FROM THE PARQUET FILE HERE ---
# Use one of the exact queries the prompt engineer provided (e.g., from get_test_queries.py)
TEST_QUERY = "Grandma in Segamat with a cat" 
LANGUAGE = "en" 
# NOTE: The default coordinates are near Cyberjaya/Putrajaya.
# You may want to change these if 'Segamat' is far away in your mock data.
LAT = 2.9234
LON = 101.6669
# ---------------------------------------------------------

request_body = {
    "query": TEST_QUERY,
    "language": LANGUAGE,
    "lat": LAT,
    "lon": LON
}

print(f"Sending query (lang={LANGUAGE}): '{TEST_QUERY}' at ({LAT:.4f}, {LON:.4f})")

try:
    response = requests.post(API_URL, json=request_body)
    response.raise_for_status()
    response_json: Dict[str, Any] = response.json()
    
    print("\n✅ API Query Successful!")
    
    print("\n--- RESPONSE ANSWER (RAG) ---")
    print(response_json.get('answer'))
    
    print("\n--- SOURCES ---")
    print(response_json.get('sources'))
    
    # === CRITICAL FIX: PRINT ACTIONS LIST ===
    print("\n--- PPS ACTIONS (RECOMMENDATIONS) ---")
    # Pretty print the actions list
    actions = response_json.get('actions', [])
    if actions:
        print(json.dumps(actions, indent=4))
    else:
        print("No actions found.")
    # ========================================

    print("\n---------------------------------------------------------")

    
except requests.exceptions.RequestException as e:
    print("\n❌ API Query Failed.")
    print(f"Error: Could not connect or received bad status code. Ensure uvicorn is running: {e}")