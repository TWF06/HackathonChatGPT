import requests
import json

# --- Configuration ---
API_URL = "http://127.0.0.1:8000/api/query"

# --- PASTE YOUR EXACT QUERY FROM THE PARQUET FILE HERE ---
# Use one of the exact queries the prompt engineer provided (e.g., from get_test_queries.py)
TEST_QUERY = "Grandma in Segamat with a cat" 
LANGUAGE = "en" 
# ---------------------------------------------------------

request_body = {
    "query": TEST_QUERY,
    "language": LANGUAGE
}

print(f"Sending query (lang={LANGUAGE}): '{TEST_QUERY}'")

try:
    response = requests.post(API_URL, json=request_body)
    response.raise_for_status()
    response_json = response.json()
    
    print("\n✅ API Query Successful!")
    print("\n--- RESPONSE ANSWER ---")
    print(response_json.get('answer'))
    print("\n--- SOURCES ---")
    print(response_json.get('sources'))
    print("\n-----------------------")

    
except requests.exceptions.RequestException as e:
    print("\n❌ API Query Failed.")
    print(f"Error: Could not connect or received bad status code. Ensure uvicorn is running: {e}")