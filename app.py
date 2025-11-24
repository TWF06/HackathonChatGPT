from fastapi import FastAPI
from pydantic import BaseModel
from rag_service import search_documents
from action_runner import run_actions
from simple_logger import log_query
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
import shutil
from pathlib import Path
import json

app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    language: str = "en"

@app.post("/api/query")
def query_api(request: QueryRequest):
    # 1. Retrieve from documents
    result = search_documents(request.query)

    # 2. Generate action steps (simple chain)
    actions = run_actions(request.query, result["text"])

    # 3. Compose final answer
    # UPDATED: Use the new generation function, passing the full result dictionary
    answer = f"[{request.language.upper()}] Based on SOP: {result['text']}"

    # 4. Log activity
    # Note: result["source"] is guaranteed to exist by search_documents
    log_query(request.query, answer, [result["source"]])

    return {
        "answer": answer,
        "sources": [result["source"]] if result["source"] != "N/A" else [],
        "actions": actions
    }

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@app.post("/api/upload")
async def upload_pdf(file: UploadFile = File(None), file_path: str = Form(None)):
    """
    Accepts either:
    - a file upload (form-data, field name 'file'), OR
    - a form field 'file_path' that points to a local file path (useful for quick testing).
    Returns: list of processed chunk ids and a success message.
    """

    # 1) If client provided a direct local path (quick test), use ingest_pdf directly
    if file_path:
        if not Path(file_path).exists():
            raise HTTPException(status_code=400, detail=f"Local file not found: {file_path}")
        # call ingest_pdf.ingest_pdf_path
        from ingest_pdf import ingest_pdf_path
        chunks = ingest_pdf_path(file_path)
        return {"status": "ok", "method": "local_path", "processed_chunks": [c["id"] for c in chunks]}

    # 2) If client uploaded a file via multipart form upload
    if file is None:
        raise HTTPException(status_code=400, detail="No file or file_path provided.")

    # Save uploaded file to uploads/
    destination = UPLOAD_DIR / file.filename
    with destination.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Now ingest it
    from ingest_pdf import ingest_pdf_path
    chunks = ingest_pdf_path(str(destination))
    return {"status": "ok", "method": "upload", "filename": file.filename, "processed_chunks": [c["id"] for c in chunks]}