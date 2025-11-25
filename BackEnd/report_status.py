import requests
import json
import time

# --- Configuration ---
API_URL = "http://127.0.0.1:8000/api/report_pps_status"
PPS_NAME = "Kolej Komuniti"
STATUS = "FULL" # Critical status to trigger the override
# ---------------------

report_data = {
    "pps_name": PPS_NAME,
    "status": STATUS,
    "reporter_id": f"OverrideTest_{int(time.time())}"
}

print(f"Attempting to report: {PPS_NAME} is {STATUS}")

try:
    # Send the POST request with JSON data
    response = requests.post(API_URL, json=report_data)
    
    # Check for success
    response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
    
    print("\n✅ API Report Successful!")
    print(f"Response: {response.json()}")
    print("-----------------------------------------------------------------------")
    print("Now proceed to Step 2: Query the Status to see the override take effect.")

except requests.exceptions.RequestException as e:
    print("\n❌ API Report Failed.")
    print(f"Error: Could not connect or received bad status code. Ensure uvicorn is running: {e}")