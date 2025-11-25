import json
import time
from pathlib import Path
from typing import List, Dict, Any

# --- CONFIGURATION DATA ---

# Mock data to simulate a database of relief centers (PPS)
MOCK_PPS_DATA = [
    {
        "name": "SK Gombak", 
        "lat": 3.210, 
        "lon": 101.620, 
        "capabilities": ["stairs_only", "no_pets"],
        "distance_km": 1
    },
    {
        "name": "Dewan Serbaguna", 
        "lat": 3.195, 
        "lon": 101.635, 
        "capabilities": ["ground_floor", "no_pets"],
        "distance_km": 2
    },
    {
        "name": "Kolej Komuniti", 
        "lat": 3.170, 
        "lon": 101.650, 
        "capabilities": ["oku_toilets", "designated_pet_area", "ground_floor"],
        "distance_km": 4
    }
]

# Path to the file created by the /api/report_pps_status endpoint in app.py
REPORTS_FILE = Path("pps_live_status.json") 
# ---------------------------


# --- HELPER FUNCTIONS FOR LIVE DATA (Feature E Integration) ---

def load_live_reports() -> List[Dict[str, Any]]:
    """Loads all crowdsourced reports from the status file."""
    try:
        # Open in read mode ('r')
        with REPORTS_FILE.open("r") as f:
            # Safely loads and returns list of reports
            return [r for r in json.load(f) if isinstance(r, dict) and 'timestamp' in r]
    except (FileNotFoundError, json.JSONDecodeError):
        # Return an empty list if the file doesn't exist or is corrupted
        return []

def get_latest_status(pps_name: str, reports: List[Dict[str, Any]]) -> Dict[str, Any] or None:
    """Finds the most recent report for a specific PPS."""
    # Filter reports for this PPS and sort by timestamp (latest first)
    relevant_reports = sorted([
        r for r in reports if r.get('pps_name') == pps_name
    ], key=lambda x: x.get('timestamp', 0), reverse=True)
    
    # Return the most recent report object
    return relevant_reports[0] if relevant_reports else None

# -------------------------------------------------------------


def run_actions(query: str, retrieved_context: str) -> List[Dict[str, Any]]:
    """
    Implements Feature A (Intelligent Routing) and uses Feature E (Helper Mode) data.
    """
    
    # 1. Identify user needs (The JamAI Logic - Input Analysis)
    needs = []
    # Check for keywords related to OKU/Warga Emas/Bedridden
    if any(k in query.lower() for k in ["grandmother", "bedridden", "oku", "warga emas"]):
        needs.append("requires_oku_access")
    # Check for keywords related to pets
    if any(k in query.lower() for k in ["cat", "dog", "pet", "haiwan"]):
        needs.append("requires_pet_area")

    actions = []
    
    live_reports = load_live_reports() # Load Helper Mode data

    # 2. Run the JamAI Logic (Chain-of-Thought)
    if needs:
        for pps in MOCK_PPS_DATA:
            status = "BEST_MATCH"
            reason = "Recommended: Closest shelter matching your needs."
            
            # --- 2a. Initial Filtering based on Vulnerability (Feature A) ---
            
            # Analyze PPS A: "SK Gombak" -> 2nd floor classrooms only.
            if "requires_oku_access" in needs and "stairs_only" in pps["capabilities"]:
                status = "NOT_SUITABLE"
                reason = "Rejection: Grandmother cannot climb stairs."
            
            # Analyze PPS B: "Dewan Serbaguna" -> Pet Policy: Strict "No Animals"
            elif "requires_pet_area" in needs and "no_pets" in pps["capabilities"]:
                status = "NOT_SUITABLE"
                reason = "Rejection: User has a cat/pet (Strict 'No Animals' policy)."
            
            # --- 2b. Override/Modify based on Live Status (Feature E) ---
            latest_report = get_latest_status(pps["name"], live_reports)

            if latest_report:
                report_status = latest_report['status']
                # Format time string only if the key exists
                report_time = time.strftime('%H:%M', time.localtime(latest_report.get('timestamp', time.time())))

                if report_status == "FULL":
                    # Critical Override: Crowdsourced data says it's full
                    status = "CRITICAL_ISSUE" 
                    reason = f"CRITICAL: Crowdsourced Report says PPS is FULL as of {report_time}. Avoid this location."
                elif report_status in ["NO_FOOD", "NEED_BLANKETS"]:
                    # Non-critical warning: Append warning to the existing reason
                    if status not in ["NOT_SUITABLE", "CRITICAL_ISSUE"]:
                         status = "WARNING"
                         reason += f" WARNING: Crowdsourced Report says '{report_status.replace('_', ' ')}' as of {report_time}."


            # 2c. Finalize reason if it remains the BEST_MATCH
            if status == "BEST_MATCH":
                 # Analyze PPS C: "Kolej Komuniti" -> Has OKU toilets + designated outdoor pet area.
                 if "oku_toilets" in pps["capabilities"]:
                    reason += " (Has OKU toilets)."
                 if "designated_pet_area" in pps["capabilities"]:
                    reason += " (Designated outdoor pet area)."


            # 3. Structure the output (The UX)
            actions.append({
                "type": "PPS_RECOMMENDATION",
                "name": pps["name"],
                "lat": pps["lat"],
                "lon": pps["lon"],
                "status": status, # BEST_MATCH, NOT_SUITABLE, CRITICAL_ISSUE, WARNING
                "reason": reason,
                "distance_km": pps["distance_km"]
            })
            
        # Sort results: BEST_MATCH first, then WARNING, then the rest. Within each, sort by distance.
        actions.sort(key=lambda x: (
            x['status'] != 'BEST_MATCH', 
            x['status'] == 'CRITICAL_ISSUE', 
            x['status'] == 'WARNING', 
            x['distance_km'] 
        ))
        
        return actions

    # 4. Fallback: If no specific needs are detected, return list of all PPS sorted by distance
    return [
        {
            "type": "PPS_NEAREST_FALLBACK", 
            "message": f"No specific needs detected for query: {query}. Showing closest PPS.", 
            "recommendations": sorted([
                {"name": pps["name"], "status": "NEAREST", "distance_km": pps["distance_km"], "lat": pps["lat"], "lon": pps["lon"]} 
                for pps in MOCK_PPS_DATA
            ], key=lambda x: x['distance_km'])
        }
    ]