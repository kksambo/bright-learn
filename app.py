from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

import shutil
import asyncio
import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import DB_CONFIG, UPLOAD_FOLDER
from etl import run_pipeline
from load import PostgreSQLLoader

app = FastAPI(title="BrightLearn ETL Dashboard")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create upload folder
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# BEST APPROACH: Create Jinja2 environment and templates properly
template_env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(['html', 'xml']),
    auto_reload=True,
    cache_size=400,  # Limit cache size to avoid memory issues
    extensions=['jinja2.ext.i18n']
)

# Pass ONLY the env parameter, NOT directory
templates = Jinja2Templates(env=template_env)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


def get_engine():
    return create_engine(
        f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


# Pipeline status tracking
pipeline = {
    "status": "Waiting...",
    "progress": 0,
    "logs": [],
    "rows": 0,
    "customers": 0,
    "stores": 0,
    "products": 0,
    "transactions": 0,
    "sales": 0,
    "time": 0
}


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page endpoint"""
    return templates.TemplateResponse(
        "index.html", 
        {"request": request}
    )


@app.get("/status")
async def status():
    """Get pipeline status"""
    return JSONResponse(pipeline)


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    """Upload and process CSV file"""
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    
    # Save uploaded file
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Run pipeline asynchronously
    asyncio.create_task(run_pipeline(file_path, pipeline, DB_CONFIG))
    
    return {"message": "Pipeline started", "filename": file.filename}


@app.post("/query")
async def execute_sql(query_data: dict):
    """Execute SQL query (SELECT only)"""
    try:
        query = query_data.get("query", "").strip()
        
        if not query:
            return JSONResponse(
                {"error": "Query cannot be empty"}, 
                status_code=400
            )
        
        # Security: Only allow SELECT queries
        query_lower = query.lower().strip()
        if not query_lower.startswith("select"):
            return JSONResponse(
                {"error": "Only SELECT queries are allowed"}, 
                status_code=400
            )
        
        engine = get_engine()
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = list(result.keys())
            rows = []
            for row in result.fetchall():
                row_data = [str(value) if value is not None else None for value in row]
                rows.append(row_data)
            
            return JSONResponse({
                "columns": columns,
                "rows": rows,
                "row_count": len(rows)
            })
    
    except SQLAlchemyError as e:
        return JSONResponse(
            {"error": f"Database error: {str(e)}"}, 
            status_code=400
        )
    except Exception as e:
        return JSONResponse(
            {"error": f"Error executing query: {str(e)}"}, 
            status_code=400
        )


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
