from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import sqlite3

import shutil
import asyncio
import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import DB_CONFIG, UPLOAD_FOLDER
from etl import run_pipeline
from load import SQLiteLoader

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

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


def get_db_connection():
    """Get SQLite database connection"""
    db_path = DB_CONFIG.get("database_path", "brightlearn_data.db")
    return sqlite3.connect(db_path)


def get_engine():
    """Get SQLAlchemy engine for SQLite"""
    db_path = DB_CONFIG.get("database_path", "brightlearn_data.db")
    return create_engine(f"sqlite:///{db_path}")


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
    """Home page with the full dashboard UI"""
    # [Your complete HTML content here - keep the same as before]
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BrightLearn ETL Control Center</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        /* [All your existing CSS styles - keep the same] */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #0a1628 0%, #1a2a4a 50%, #162d50 100%);
            color: #e2e8f0;
            min-height: 100vh;
        }

        /* ... rest of your CSS ... */
        /* [Keep all your existing CSS here - it's the same as before] */
    </style>
</head>
<body>
    <!-- [Your complete HTML content - keep the same as before] -->
    <header class="header">
        <div class="header-left">
            <div class="logo-icon">
                <i class="fas fa-bolt"></i>
            </div>
            <div>
                <div class="header-title">BrightLearn ETL</div>
                <div class="header-subtitle">Control Center</div>
            </div>
        </div>
        <div class="header-right">
            <div class="status-badge waiting" id="statusBadge">
                <i class="fas fa-circle" style="font-size: 10px; margin-right: 8px;"></i>
                <span id="statusText">Waiting</span>
            </div>
        </div>
    </header>

    <div class="container">
        <!-- Upload Section -->
        <div class="upload-section">
            <div class="upload-left">
                <div class="file-input-wrapper">
                    <label class="file-label" id="fileLabel" for="csv">
                        <i class="fas fa-cloud-upload-alt"></i>
                        <span id="fileLabelText">Choose CSV File</span>
                    </label>
                    <input type="file" id="csv" accept=".csv">
                    <span class="file-name" id="fileName">No file selected</span>
                </div>
                <button class="btn-primary" id="runBtn" onclick="uploadCSV()">
                    <i class="fas fa-play"></i>
                    <span id="btnText">Run Pipeline</span>
                    <i class="fas fa-spinner spinner"></i>
                </button>
            </div>
            <div class="execution-time">
                <i class="far fa-clock"></i>
                Execution: <span id="execTime">0.00s</span>
            </div>
        </div>

        <!-- Pipeline Steps -->
        <div class="pipeline-steps">
            <div class="step" id="stepExtract">
                <span class="step-number">STEP 1</span>
                <span class="step-icon">&#128195;</span>
                <h3>Extract</h3>
                <p>Reading CSV data</p>
            </div>
            <div class="step" id="stepTransform">
                <span class="step-number">STEP 2</span>
                <span class="step-icon">&#9881;</span>
                <h3>Transform</h3>
                <p>Cleaning and building dimensions</p>
            </div>
            <div class="step" id="stepLoad">
                <span class="step-number">STEP 3</span>
                <span class="step-icon">&#128451;</span>
                <h3>Load</h3>
                <p>Loading to SQLite</p>
            </div>
        </div>

        <!-- Progress -->
        <div class="progress-container">
            <div class="progress-header">
                <span class="progress-label">
                    <i class="fas fa-chart-line"></i> Pipeline Progress
                </span>
                <span class="progress-percentage" id="progressPercent">0%</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill"></div>
            </div>
        </div>

        <!-- Stats -->
        <div class="stats-grid">
            <div class="stat-card">
                <span class="stat-icon">&#128202;</span>
                <div class="stat-value" id="statRows">0</div>
                <div class="stat-label">Total Rows</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">&#128100;</span>
                <div class="stat-value" id="statCustomers">0</div>
                <div class="stat-label">Customers</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">&#127970;</span>
                <div class="stat-value" id="statStores">0</div>
                <div class="stat-label">Stores</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">&#128230;</span>
                <div class="stat-value" id="statProducts">0</div>
                <div class="stat-label">Products</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">&#128260;</span>
                <div class="stat-value" id="statTransactions">0</div>
                <div class="stat-label">Transactions</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">&#128176;</span>
                <div class="stat-value" id="statSales">0</div>
                <div class="stat-label">Sales Records</div>
            </div>
        </div>

        <!-- SQL Query Section -->
        <div class="sql-section">
            <div class="sql-header">
                <h3>
                    <i class="fas fa-database"></i> SQL Query Console
                </h3>
                <div class="query-status" id="queryStatus">
                    <span>Ready</span>
                </div>
            </div>
            <div class="sql-body">
                <div class="sql-input-group">
                    <textarea class="sql-textarea" id="sqlQuery" placeholder="Enter your SQL query here...&#10;Example: SELECT * FROM customers LIMIT 10">SELECT * FROM customers LIMIT 10</textarea>
                    <button class="btn-secondary" onclick="executeQuery()">
                        <i class="fas fa-play"></i> Execute
                    </button>
                </div>
                <div class="sql-results" id="sqlResults">
                    <div class="no-results">
                        <i class="fas fa-arrow-up" style="font-size: 24px; margin-bottom: 10px; display: block;"></i>
                        Enter a SQL query above and click Execute to see results
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let isRunning = false;

        document.getElementById('csv').addEventListener('change', function(e) {
            const file = e.target.files[0];
            if (file) {
                document.getElementById('fileName').textContent = file.name;
                document.getElementById('fileLabel').classList.add('has-file');
                document.getElementById('fileLabelText').textContent = 'Change File';
            }
        });

        async function uploadCSV() {
            if (isRunning) return;

            const file = document.getElementById('csv').files[0];
            if (!file) {
                alert('Please select a CSV file first');
                return;
            }

            isRunning = true;
            const btn = document.getElementById('runBtn');
            btn.classList.add('loading');
            btn.disabled = true;
            document.getElementById('btnText').textContent = 'Running...';

            const data = new FormData();
            data.append('file', file);

            try {
                await fetch('/upload', {
                    method: 'POST',
                    body: data
                });
            } catch (error) {
                console.error('Upload failed:', error);
                isRunning = false;
                btn.classList.remove('loading');
                btn.disabled = false;
                document.getElementById('btnText').textContent = 'Run Pipeline';
            }
        }

        async function executeQuery() {
            const query = document.getElementById('sqlQuery').value.trim();
            if (!query) {
                alert('Please enter a SQL query');
                return;
            }

            const statusDiv = document.getElementById('queryStatus');
            const resultsDiv = document.getElementById('sqlResults');

            statusDiv.innerHTML = '<span style="color: #ecc94b;"><i class="fas fa-spinner fa-spin"></i> Executing...</span>';
            resultsDiv.innerHTML = '<div class="no-results"><i class="fas fa-spinner fa-spin" style="font-size: 24px; margin-bottom: 10px; display: block;"></i>Executing query...</div>';

            try {
                const response = await fetch('/query', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ query: query })
                });

                const data = await response.json();

                if (data.error) {
                    statusDiv.innerHTML = `<span class="error"><i class="fas fa-times-circle"></i> Error</span>`;
                    resultsDiv.innerHTML = `<div class="error-message"><i class="fas fa-exclamation-triangle"></i> ${data.error}</div>`;
                    return;
                }

                if (data.columns && data.rows) {
                    statusDiv.innerHTML = `<span class="success"><i class="fas fa-check-circle"></i> ${data.row_count} rows returned</span>`;

                    if (data.rows.length === 0) {
                        resultsDiv.innerHTML = '<div class="no-results">No results found</div>';
                        return;
                    }

                    let html = '<table><thead><tr>';
                    data.columns.forEach(col => {
                        html += `<th>${col}</th>`;
                    });
                    html += '</tr></thead><tbody>';

                    data.rows.forEach(row => {
                        html += '<tr>';
                        row.forEach(cell => {
                            html += `<td>${cell !== null ? cell : 'NULL'}</td>`;
                        });
                        html += '</tr>';
                    });

                    html += '</tbody></table>';
                    resultsDiv.innerHTML = html;
                }
            } catch (error) {
                statusDiv.innerHTML = `<span class="error"><i class="fas fa-times-circle"></i> Error</span>`;
                resultsDiv.innerHTML = `<div class="error-message"><i class="fas fa-exclamation-triangle"></i> Failed to execute query: ${error.message}</div>`;
            }
        }

        document.getElementById('sqlQuery').addEventListener('keydown', function(e) {
            if (e.ctrlKey && e.key === 'Enter') {
                e.preventDefault();
                executeQuery();
            }
        });

        setInterval(async () => {
            try {
                const res = await fetch('/status');
                const d = await res.json();

                document.getElementById('progressFill').style.width = d.progress + '%';
                document.getElementById('progressPercent').textContent = d.progress + '%';

                document.getElementById('statRows').textContent = d.rows.toLocaleString();
                document.getElementById('statCustomers').textContent = d.customers.toLocaleString();
                document.getElementById('statStores').textContent = d.stores.toLocaleString();
                document.getElementById('statProducts').textContent = d.products.toLocaleString();
                document.getElementById('statTransactions').textContent = d.transactions.toLocaleString();
                document.getElementById('statSales').textContent = d.sales.toLocaleString();

                if (d.time > 0) {
                    document.getElementById('execTime').textContent = d.time.toFixed(2) + 's';
                }

                const badge = document.getElementById('statusBadge');
                const statusText = document.getElementById('statusText');
                badge.className = 'status-badge';

                if (d.status === 'Extract') {
                    badge.classList.add('active');
                    statusText.textContent = 'Extracting...';
                } else if (d.status === 'Transform') {
                    badge.classList.add('active');
                    statusText.textContent = 'Transforming...';
                } else if (d.status === 'Load') {
                    badge.classList.add('active');
                    statusText.textContent = 'Loading...';
                } else if (d.status === 'Completed') {
                    badge.classList.add('completed');
                    statusText.textContent = 'Completed';
                    isRunning = false;
                    document.getElementById('runBtn').classList.remove('loading');
                    document.getElementById('runBtn').disabled = false;
                    document.getElementById('btnText').textContent = 'Run Pipeline';
                } else {
                    badge.classList.add('waiting');
                    statusText.textContent = 'Waiting';
                }

                const steps = ['Extract', 'Transform', 'Load'];
                steps.forEach((step, index) => {
                    const el = document.getElementById(['stepExtract', 'stepTransform', 'stepLoad'][index]);
                    el.classList.remove('active', 'completed');

                    if (d.status === step || (d.status === 'Completed' && index < 3)) {
                        if (d.status === 'Completed') {
                            el.classList.add('completed');
                        } else {
                            el.classList.add('active');
                        }
                    }

                    if (d.status !== 'Completed') {
                        const statusOrder = ['Extract', 'Transform', 'Load'];
                        if (statusOrder.indexOf(d.status) > index) {
                            el.classList.add('completed');
                        }
                    }
                });

            } catch (error) {
                console.error('Status fetch failed:', error);
            }
        }, 1000);
    </script>
</body>
</html>"""
    return HTMLResponse(content=html_content)


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
    """Execute SQL query on SQLite database (SELECT only)"""
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
        
        # Use SQLite connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            
            # Convert rows to list of lists
            result_rows = []
            for row in rows:
                result_rows.append([str(value) if value is not None else None for value in row])
            
            conn.close()
            
            return JSONResponse({
                "columns": columns,
                "rows": result_rows,
                "row_count": len(result_rows)
            })
            
        except sqlite3.Error as e:
            conn.close()
            return JSONResponse(
                {"error": f"SQLite error: {str(e)}"}, 
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
