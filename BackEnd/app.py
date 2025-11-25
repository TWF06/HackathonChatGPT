from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import json
import time 
from typing import List, Dict, Any
import aiofiles 
import os 
import shutil # Needed for safe file handling

# --- Conceptual Imports from your RAG and Action Services ---
# NOTE: Ensure you have 'refresh_live_reports_from_disk' imported from action_runner
from rag_service import search_documents, get_simulated_llm_response, load_documents_from_processed
from action_runner import run_actions, refresh_live_reports_from_disk
from simple_logger import log_query 
from ingest_pdf import ingest_pdf_path 
# -----------------------------------------------------------

# =========================================================
# 1. APPLICATION SETUP
# =========================================================

app = FastAPI()

# Configuration for CORS 
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration Files
REPORTS_FILE = Path("pps_live_status.json") 
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


# Pydantic Models for API validation
class QueryBody(BaseModel):
    query: str
    language: str = "en"
    lat: float = 2.9234
    lon: float = 101.6669

class PPSStatusReport(BaseModel):
    pps_name: str
    status: str
    reporter_id: str


# =========================================================
# 2. CORE ENDPOINTS
# =========================================================

@app.post("/api/query")
def process_query(body: QueryBody):
    """
    Main endpoint for user queries. Runs RAG and Action Runner logic.
    """
    # 1. Refresh reports immediately before running actions (to ensure freshness)
    refresh_live_reports_from_disk()
    
    # 2. RAG Component
    # We pass the language to the RAG service as well
    search_result = search_documents(body.query, body.language)
    
    # 3. LLM Component (Simulated or Real)
    llm_answer = get_simulated_llm_response(search_result, prompt_templates={}) 
    
    # 4. Action Runner Component (For PPS Recommendation)
    # 🌟 CRITICAL FIX: Pass body.language to run_actions
    actions = run_actions(body.query, body.lat, body.lon, body.language)
    
    # 5. Logging
    log_query(body.query, llm_answer, [search_result.get('source', 'N/A')])
    
    # 6. Response
    return {
        "query": body.query,
        "answer": llm_answer,
        "sources": [search_result.get('source', 'N/A')],
        "actions": actions
    }


# =========================================================
# 3. DATA INGESTION & REPORTING ENDPOINTS (UPGRADED TO ASYNC)
# =========================================================

def run_pdf_ingestion_task(file_path: str):
    """Function to be run as a background task."""
    print(f"\n--- [BACKGROUND TASK] Starting ingestion for: {file_path} ---")
    
    # 1. Process the PDF and get chunks 
    ingest_pdf_path(file_path, source=file_path)
    
    # 2. Reload RAG documents after ingestion. 
    load_documents_from_processed()
    
    print(f"--- [BACKGROUND TASK] Ingestion finished for: {file_path}. RAG documents reloaded. ---")


@app.post("/api/upload_pdf")
async def upload_pdf(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    Handles PDF upload asynchronously and queues ingestion as a background task.
    """
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed.")

    # 1. Save the file asynchronously (using aiofiles)
    destination = UPLOAD_DIR / file.filename
    try:
        async with aiofiles.open(destination, 'wb') as out_file:
            # Read in chunks for safety, using the read() method of UploadFile
            while content := await file.read(): 
                await out_file.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save file: {e}")

    # 2. Queue the ingestion task
    background_tasks.add_task(run_pdf_ingestion_task, str(destination))
    
    return {
        "status": "processing", 
        "message": f"File '{file.filename}' uploaded successfully. Ingestion started in the background."
    }


@app.post("/api/report_pps_status")
async def report_pps_status(report: PPSStatusReport):
    """
    Accepts crowdsourced reports on PPS status (Helper Mode - Feature E).
    Saves the report to a JSON file and INSTANTLY updates the in-memory state.
    """
    
    # 1. Prepare the report data
    report_data = report.model_dump()
    report_data["timestamp"] = int(time.time())
    
    # 2. Read existing reports asynchronously
    existing_reports = []
    if os.path.exists(REPORTS_FILE):
        try:
            # Read with UTF-8 encoding for safety
            async with aiofiles.open(REPORTS_FILE, "r", encoding="utf-8") as f:
                content = await f.read()
            existing_reports = json.loads(content)
        except (json.JSONDecodeError, FileNotFoundError, UnicodeDecodeError):
            existing_reports = []
    
    # 3. Append the new report
    existing_reports.append(report_data)
    
    # 4. Save the updated list back to the file asynchronously
    try:
        # Write with UTF-8 encoding for safety
        async with aiofiles.open(REPORTS_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(existing_reports, indent=4))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save report file: {e}")
        
    # 5. Trigger the in-memory update immediately!
    refresh_live_reports_from_disk()

    return {"status": "ok", "message": "PPS status reported and live recommendations updated instantly."}


# =========================================================
# 4. STARTUP HOOK
# =========================================================

# Ensure RAG documents are loaded on startup.
load_documents_from_processed()