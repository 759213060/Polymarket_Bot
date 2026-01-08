import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from pathlib import Path
from order_manager import OrderManager

app = FastAPI()
manager_ref = None
_BASE_DIR = Path(__file__).resolve().parent
_DASHBOARD_HTML_PATH = _BASE_DIR / "dashboard.html"

@app.get("/")
async def get_dashboard():
    if _DASHBOARD_HTML_PATH.exists():
        return FileResponse(_DASHBOARD_HTML_PATH, media_type="text/html; charset=utf-8")
    return HTMLResponse("<html><body><h3>dashboard.html 不存在</h3></body></html>", status_code=500)

@app.get("/api/stats")
async def get_stats():
    if manager_ref:
        return manager_ref.get_dashboard_stats()
    return {"error": "Manager not initialized"}

def start_server(manager: OrderManager, port: int = 8848):
    global manager_ref
    manager_ref = manager
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="error")
