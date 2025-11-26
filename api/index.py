from flask import Flask, request, jsonify
from jamaibase import JamAI, types as p
from flask_cors import CORS 
import os
import sys
import re
from datetime import datetime

app = Flask(__name__)
CORS(app)

# 1. Configuration
PROJECT_ID = os.getenv("JAMAI_PROJECT_ID")
API_KEY = os.getenv("JAMAI_PAT")
TABLE_ID = os.getenv("ACTION_TABLE_ID")

# Initialize JamAI Client
jamai = JamAI(
    project_id=PROJECT_ID, 
    token=API_KEY 
)

# FIX 1: Root GET route (handles browser direct access)
@app.route('/api/', methods=['GET'])
def api_root():
    return jsonify({"status": "API is operational", "primary_endpoint": "/api/analyze (POST)"}), 200

# 💡 FIX 2: Only define the route WITHOUT a trailing slash. Flask handles the redirect.
@app.route('/api/analyze', methods=['POST'])
def analyze_route():
    try:
        data = request.json
        user_input = data.get('user_input')
        location_details = data.get('location_details')
        row_id_to_fetch = data.get('row_id')

        if not user_input and not row_id_to_fetch:
            return jsonify({"error": "User input or row_id is required"}), 400

        # =========================================================
        # MODE 2: FETCH STATUS (The polling request from the client)
        # =========================================================
        if row_id_to_fetch:
            print(f"DEBUG: Fetching Row ID: {row_id_to_fetch}", file=sys.stderr)
            
            row_response = jamai.table.get_table_row(
                p.TableType.ACTION,
                TABLE_ID,
                row_id_to_fetch
            )
            
            final_row = row_response.get("row")

            if final_row:
                
                if 'route_analysis' in final_row:
                    
                    # Check for the *actual content* of the required analysis fields
                    if final_row.get("route_analysis", {}).get("value") and final_row.get("selected_pps", {}).get("value"):
                        
                        # --- CLEANUP: Limit selected_pps to just the name ---
                        clean_pps = final_row["selected_pps"]["value"]
                        if len(clean_pps) > 50: 
                            match = re.search(r"(Shelter\s+\d+|[\w\s]+(Hall|Center|Centre|School|Club))", clean_pps, re.IGNORECASE)
                            if match:
                                clean_pps = match.group(0).strip()
                            else:
                                clean_pps = clean_pps.split('.')[0].split(',')[0].strip()

                        return jsonify({
                            "success": True,
                            "status": "complete",
                            "analysis": final_row["route_analysis"]["value"], 
                            "tags": final_row.get("decoded_tags", {}).get("value"),
                            "selected_pps": clean_pps
                        }), 200
            
            print(f"DEBUG: Row ID {row_id_to_fetch} still pending.", file=sys.stderr)
            return jsonify({
                "success": False, 
                "status": "pending", 
                "row_id": row_id_to_fetch
            }), 200

        # =========================================================
        # MODE 1: SUBMIT JOB
        # =========================================================
        else:
            print("DEBUG: Submitting new job...", file=sys.stderr)
            row_data = {
                "action": "find_safe_shelter",
                "user_input": user_input,
                "location_details": location_details,
                "created_at": datetime.now().isoformat()
            }
            
            add_request = p.MultiRowAddRequest(
                table_id=TABLE_ID,
                data=[row_data],
                stream=False,
            )

            completion = jamai.table.add_table_rows(
                table_type=p.TableType.ACTION,
                request=add_request,
            )
            
            # Robust extraction of row_id from submission
            row_id = None
            if hasattr(completion, "rows") and completion.rows:
                row_id = completion.rows[0].row_id
            elif isinstance(completion, dict) and "rows" in completion:
                rows = completion["rows"]
                if rows:
                    row_id = rows[0].get("row_id")

            if not row_id:
                return jsonify({"error": "Failed to submit job - no Row ID returned"}), 500

            print(f"DEBUG: Job Submitted. Row ID: {row_id}", file=sys.stderr)

            return jsonify({
                "success": True, 
                "status": "submitted", 
                "row_id": row_id
            }), 200

    except Exception as e:
        print(f"FATAL ERROR in analyze_route: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500