import json
import time
from pathlib import Path
from typing import List, Dict, Any
from geopy.distance import geodesic
import re

# --- CONFIGURATION DATA ---

# Path to the file containing the mock data for PPS centers
PPS_DATA_FILE = Path("data/live_pps_data.json")

# Path to the file created by the /api/report_pps_status endpoint in app.py
REPORTS_FILE = Path("pps_live_status.json")

# NEW CONFIGURATION FOR CONSENSUS
REPORT_TIMEOUT_SEC = 6 * 60 * 60 # 6 hours
CRITICAL_CONSENSUS_THRESHOLD = 2 # Requires 2 reports for CRITICAL_ISSUE

# New: Status ranking for final sorting (lower number is higher priority)
STATUS_RANKING = {
    "BEST_MATCH": 0,    # Perfect match on capabilities
    "SUITABLE": 1,      # Missing some information, but no critical/negative reports
    "WARNING": 2,       # One critical live report
    "CRITICAL_ISSUE": 3, # Consensus of critical live reports
    "NOT_SUITABLE": 4   # Explicitly fails a required capability (e.g., no_pets)
}
# ---------------------------

# --- TRANSLATION MAP (New for Bilingual Reasons and Messages) ---
TRANSLATION_MAP = {
    "en": {
        "matched": "✓ Matched",
        "missing": "? Info Missing",
        "failed_prefix": "✗ Failed: No",
        "failed_suffix": "(Has '{neg_key}')",
        "no_report": "No recent critical/warning report found.",
        "critical_prefix": "🛑 CRITICAL LIVE REPORT:",
        "warning_prefix": "⚠️ WARNING LIVE REPORT:",
        "fallback_msg": "No specific needs detected for query: {query}. Showing closest PPS."
    },
    "ms": {
        "matched": "✓ Dipadankan",
        "missing": "? Maklumat Tiada",
        "failed_prefix": "✗ Gagal: Tiada",
        "failed_suffix": "(Ada '{neg_key}')",
        "no_report": "Tiada laporan kritikal/amaran ditemui.",
        "critical_prefix": "🛑 LAPORAN KRITIKAL LANGSUNG:",
        "warning_prefix": "⚠️ LAPORAN AMARAN LANGSUNG:",
        "fallback_msg": "Tiada keperluan khusus dikesan untuk pertanyaan: {query}. Menunjukkan PPS terdekat."
    }
}
# -----------------------------

# --- GLOBALS ---
PPS_CENTERS: List[Dict[str, Any]] = []
LIVE_REPORTS_AGGREGATED: Dict[str, Dict[str, Any]] = {} 
# ---------------

# --- HELPER FUNCTIONS FOR LIVE DATA (Feature E Integration) ---

def refresh_live_reports_from_disk(reports_file: Path = REPORTS_FILE):
    """
    Loads all reports from disk, aggregates them, applies consensus logic, 
    and updates the global LIVE_REPORTS_AGGREGATED.
    """
    global LIVE_REPORTS_AGGREGATED
    LIVE_REPORTS_AGGREGATED = {}
    
    current_time = int(time.time())
    
    try:
        # Use encoding="utf-8" for safety when reading text files
        with reports_file.open("r", encoding="utf-8") as f:
            all_reports: List[Dict[str, Any]] = json.load(f)
        
        # 1. Filter out expired reports
        recent_reports = [
            report for report in all_reports
            if current_time - report.get("timestamp", 0) < REPORT_TIMEOUT_SEC
        ]
        
        # 2. Aggregate and apply Consensus Logic
        reports_by_pps: Dict[str, List[Dict[str, Any]]] = {}
        for report in recent_reports:
            pps_name = report.get("pps_name")
            if pps_name:
                if pps_name not in reports_by_pps:
                    reports_by_pps[pps_name] = []
                reports_by_pps[pps_name].append(report)
                
        # 3. Determine Final Status for each PPS
        for pps_name, reports in reports_by_pps.items():
            
            # Count critical reports (FULL or CRITICAL_ISSUE)
            critical_reports = [
                r for r in reports 
                if r.get("status", "").upper() in ("FULL", "CRITICAL_ISSUE")
            ]
            
            reports.sort(key=lambda r: r.get("timestamp", 0), reverse=True)
            latest_report = reports[0]

            if len(critical_reports) >= CRITICAL_CONSENSUS_THRESHOLD:
                final_status = "CRITICAL_ISSUE"
                reason = f"Consensus: {len(critical_reports)} recent reports confirm critical status."
            elif len(critical_reports) >= 1:
                final_status = "WARNING"
                reason = f"Warning: 1 user reported critical status. Needs verification."
            else:
                # If no critical reports, assume OK based on latest non-critical report
                final_status = latest_report.get("status", "OK")
                reason = "No recent critical reports."
            
            LIVE_REPORTS_AGGREGATED[pps_name] = {
                "final_status": final_status,
                "reason": reason,
                "latest_timestamp": latest_report.get("timestamp")
            }
        
    except (json.JSONDecodeError, FileNotFoundError):
        # This is expected on first run if pps_live_status.json doesn't exist
        pass


# --------------------------------------------------------------------------

def load_pps_data(pps_data_file: Path = PPS_DATA_FILE):
    """Loads all PPS centers data from the mock file."""
    global PPS_CENTERS
    try:
        # Use encoding="utf-8" for safety when reading text files
        with pps_data_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
            PPS_CENTERS = data.get("shelters", [])
        print(f"--- [Action Runner] Loaded {len(PPS_CENTERS)} PPS centers. ---")
    except (json.JSONDecodeError, FileNotFoundError, UnicodeDecodeError) as e:
        print(f"--- [Action Runner] Error loading PPS data: {e} ---")

# --- In action_runner.py ---

def extract_needs(query: str) -> Dict[str, bool]:
    # Detect specific needs from the query. Includes Malay keywords.
    needs = {
        "ground_floor": bool(re.search(r'wheelchair|oku|disabled access|ground|tingkat bawah|lantai bawah', query, re.IGNORECASE)),
        # ✅ CORRECTED LINE: Includes 'accessible toilet' to capture the user's need!
        "oku_toilets": bool(re.search(r'oku toilet|tandas oku|disabled toilet|accessible toilet', query, re.IGNORECASE)),
        "pet_area": bool(re.search(r'pet|cat|dog|animal|haiwan|kucing|anjing', query, re.IGNORECASE)),
        "family_room": bool(re.search(r'family|babies|nursing|newborn|keluarga|bayi|menyusu', query, re.IGNORECASE))
    }
    return needs

# CRITICAL FIX: Added 'language' to the signature (Done in app.py in previous turn)
def run_actions(query: str, lat: float, lon: float, language: str = "en") -> List[Dict[str, Any]]:
    """
    The main action function that recommends PPS centers.
    """
    
    # Ensure data is loaded
    if not PPS_CENTERS:
        load_pps_data()

    # 1. Select the correct translation dictionary
    lang_map = TRANSLATION_MAP.get(language.lower(), TRANSLATION_MAP["en"])
        
    # 2. Detect specific user needs from the query
    needs = extract_needs(query)
    needs_detected = any(needs.values())
    
    # 3. Calculate distance and initial filtering/scoring
    all_pps_with_distance = []
    for pps in PPS_CENTERS:
        # Check if latitude/longitude exist before calculating distance
        if pps.get('lat') is not None and pps.get('lon') is not None:
            distance_km = geodesic((lat, lon), (pps['lat'], pps['lon'])).kilometers
            pps['distance_km'] = round(distance_km, 2)
            all_pps_with_distance.append(pps)
        
    if needs_detected:
        
        # --- NEW CAPABILITY MAP FOR ROBUST MATCHING ---
        CAPABILITY_MAP = {
            "ground_floor": {"pos": "ground_floor", "neg": "stairs_only"},
            "oku_toilets": {"pos": "oku_toilets", "neg": "no_oku_toilets"}, 
            "pet_area": {"pos": "designated_pet_area", "neg": "no_pets"},
            "family_room": {"pos": "family_room", "neg": "no_family_room"} 
        }
        
        actions = []
        for pps in all_pps_with_distance:
            # Initial status is SUITABLE (for centers with missing info)
            status = "SUITABLE" 
            final_reason = []
            
            all_needs_met = True 
            is_explicitly_unsuitable = False

            capabilities = pps.get('capabilities', [])
            
            # List of required needs the center must meet
            required_needs = [need for need, required in needs.items() if required]

            for need in required_needs:
                map_entry = CAPABILITY_MAP.get(need)
                if not map_entry:
                    continue 

                pos_key = map_entry["pos"]
                neg_key = map_entry["neg"]
                
                # 1. Check for Positive Match
                if pos_key in capabilities:
                    final_reason.append(f"{lang_map['matched']}: {need}")
                
                # 2. Check for Negative Match (Explicit Exclusion)
                elif neg_key in capabilities: 
                    # Use translation for failed status
                    final_reason.append(f"{lang_map['failed_prefix']} {need} {lang_map['failed_suffix'].format(neg_key=neg_key)}")
                    is_explicitly_unsuitable = True
                    all_needs_met = False
                
                # 3. Check for Missing Information (Not a perfect match)
                else:
                    final_reason.append(f"{lang_map['missing']}: {need}")
                    all_needs_met = False
            
            # 2b. Determine Final Capability Status (CRITICAL FIX: logic for BEST_MATCH)
            if is_explicitly_unsuitable:
                status = "NOT_SUITABLE"
            # This is the fix: Only set BEST_MATCH if ALL required needs were met
            elif all_needs_met and required_needs: 
                status = "BEST_MATCH"
            # Otherwise, it remains the initial "SUITABLE"
            
            # 3. Apply Live Status Check (Overrides capability match if needed)
            live_report = LIVE_REPORTS_AGGREGATED.get(pps["name"])
            if live_report:
                report_status = live_report["final_status"]
                
                # Live status overrides BEST_MATCH/SUITABLE/OK if it's WARNING/CRITICAL
                if report_status == "CRITICAL_ISSUE":
                    status = "CRITICAL_ISSUE"
                    final_reason.insert(0, f"{lang_map['critical_prefix']} {live_report['reason']}")
                elif report_status == "WARNING":
                    # WARNING only downgrades BEST_MATCH/SUITABLE
                    if status not in ("CRITICAL_ISSUE", "NOT_SUITABLE"): 
                        status = "WARNING"
                        final_reason.insert(0, f"{lang_map['warning_prefix']} {live_report['reason']}")
            
            # Add default reason if no live report status was applied
            if status not in ("CRITICAL_ISSUE", "WARNING", "NOT_SUITABLE"):
                final_reason.append(lang_map['no_report'])


            # 4. Structure the output
            actions.append({
                "type": "PPS_RECOMMENDATION",
                "name": pps["name"],
                "lat": pps['lat'],
                "lon": pps['lon'],
                "status": status, 
                "reason": f"({' | '.join(final_reason)})".strip(),
                "distance_km": pps["distance_km"]
            })
            
        # 5. Sort results using the new STATUS_RANKING
        actions.sort(key=lambda x: (
            STATUS_RANKING.get(x['status'], 99), # Custom order
            x['distance_km'] # Secondary sort by distance
        ))
        
        return actions

    # 3. Fallback: If no specific needs are detected, return list of all PPS sorted by distance
    
    # Use translation for fallback message
    fallback_message = lang_map['fallback_msg'].format(query=query) 

    recommendations_list = sorted([
        {
            "name": pps["name"], 
            # Fallback must respect live status, but if OK, we just use "OK"
            "status": LIVE_REPORTS_AGGREGATED.get(pps["name"], {}).get('final_status', 'OK'), 
            "distance_km": pps["distance_km"]
        } for pps in all_pps_with_distance
    ], key=lambda x: (STATUS_RANKING.get(x['status'], 99), x['distance_km']))
    
    # Returning the final structure
    return [{
        "type": "PPS_NEAREST_FALLBACK", 
        "message": fallback_message, 
        "recommendations": recommendations_list
    }]


# --- INITIALIZATION ---
load_pps_data()
refresh_live_reports_from_disk()
# ----------------------