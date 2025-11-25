from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import shutil
from pathlib import Path
import json
import time 
from typing import List, Dict, Any

# --- Conceptual Imports from your RAG and Action Services ---
# NOTE: These functions are assumed to be available from the files you created.
from rag_service import search_documents, get_simulated_llm_response, load_documents_from_processed
from action_runner import run_actions
from simple_logger import log_query # Assuming this exists
from ingest_pdf import ingest_pdf_path # Assuming this exists
# -----------------------------------------------------------

# =========================================================
# 1. PROMPT TEMPLATES (Rumour Killer - Feature C)
# =========================================================

PROMPT_TEMPLATES = {
    "en": {
        "system": "You are a Smart Flood Mitigation Assistant. Answer the user's question ONLY based on the CONTEXT provided. DO NOT fabricate information. Always cite the Source. Maintain a helpful and professional tone.",
        "template": "CONTEXT: {context}\n\nUSER QUESTION: {query}\n\nANSWER:"
    },
    "ms": {
        "system": "Anda adalah Pembantu Mitigasi Banjir Pintar. Jawab soalan pengguna HANYA berdasarkan KONTEKS yang diberikan. JANGAN mereka-reka jawapan. Sentiasa nyatakan Sumber. Kekalkan nada yang membantu dan profesional.",
        "template": "KONTEKS: {context}\n\nPERTANYAAN PENGGUNA: {query}\n\nJAWAPAN:"
    }
}


app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all origins for simplicity in this development environment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    query: str
    language: str = "en"

class PPSStatusReport(BaseModel):
    """Data model for crowdsourced PPS status reports (Helper Mode - Feature E)."""
    pps_name: str
    status: str # e.g., "FULL", "NO_FOOD", "NEED_BLANKETS"
    reporter_id: str = "anonymous" # Simple ID/token

# --- Helper Mode Setup ---
REPORTS_FILE = Path("pps_live_status.json")
# Ensure the file exists
if not REPORTS_FILE.exists():
    with REPORTS_FILE.open("w") as f:
        json.dump([], f)
# -------------------------


@app.post("/api/query")
def query_api(request: QueryRequest):
    # 1. Retrieve from documents (Rumour Killer)
    result = search_documents(request.query, request.language)
    
    result['language'] = request.language 
    
    # 2. Generate the simulated LLM answer
    answer = get_simulated_llm_response(result, PROMPT_TEMPLATES)
    
    # 3. Generate action steps (Intelligent Routing/Smart PPS - Feature A)
    retrieved_context = result.get("retrieved_context", "")
    actions = run_actions(request.query, retrieved_context)

    # 4. Log activity
    sources_to_log = [result["source"]] if result.get("source") != "N/A" else []
    log_query(request.query, answer, sources_to_log)

    return {
        "answer": answer,
        "sources": sources_to_log,
        "actions": actions
    }

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@app.post("/api/upload")
async def upload_pdf(file: UploadFile = File(None), file_path: str = Form(None)):
    """
    Handles file upload and local path ingestion. (Rumour Killer Knowledge Base update)
    """

    # 1) Handle local path ingestion
    if file_path:
        if not Path(file_path).exists():
            raise HTTPException(status_code=400, detail=f"Local file not found: {file_path}")
        
        chunks = ingest_pdf_path(file_path)
        
        # CRITICAL FIX: Simply call the function that updates the RAG state.
        # Removed global/import lines that caused the SyntaxError.
        load_documents_from_processed() 
        
        return {"status": "ok", "method": "local_path", "processed_chunks": [c["id"] for c in chunks], "message": f"Successfully ingested file from local path: {file_path}. {len(chunks)} chunks processed."}


    # 2) Handle file upload
    if file is None:
        raise HTTPException(status_code=400, detail="No file or file_path provided.")

    # Save uploaded file
    destination = UPLOAD_DIR / file.filename
    try:
        with destination.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")
    finally:
        await file.close() # Close the UploadFile handle after reading

    # Call the ingestion logic
    chunks = ingest_pdf_path(str(destination), source=str(destination))
    
    # CRITICAL FIX: Simply call the function that updates the RAG state.
    # Removed global/import lines that caused the SyntaxError.
    load_documents_from_processed()
    
    return {"status": "ok", "method": "upload", "processed_chunks": [c["id"] for c in chunks], "message": f"File '{file.filename}' uploaded and ingested successfully. {len(chunks)} chunks processed."}


@app.post("/api/report_pps_status")
def report_pps_status(report: PPSStatusReport):
    """
    Accepts crowdsourced reports on PPS status (Helper Mode - Feature E).
    Saves the report to a JSON file.
    """
    
    # 1. Prepare the report data
    report_data = report.model_dump()
    report_data["timestamp"] = int(time.time()) # Add server timestamp
    
    # 2. Read existing reports
    try:
        with REPORTS_FILE.open("r") as f:
            existing_reports = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        existing_reports = []
    
    # 3. Append the new report
    existing_reports.append(report_data)
    
    # 4. Save the updated list back to the file
    try:
        with REPORTS_FILE.open("w") as f:
            json.dump(existing_reports, f, indent=4)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save report: {str(e)}")

    return {"status": "ok", "message": f"Status for {report.pps_name} reported successfully."}