import os
import threading
import time

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from allocation_app import run_refresh

app = FastAPI(title="INEOS Allocation Tool")
templates = Jinja2Templates(directory="templates")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMPLATE_PATH = os.path.join(BASE_DIR, "allocation_template.html")
MASTER_PATH = os.path.join(DATA_DIR, "master.xlsb")
DECRYPTED_PATH = os.path.join(DATA_DIR, "master_decrypted.xlsb")
OUTPUT_PATH = os.path.join(DATA_DIR, "allocation.html")

os.makedirs(DATA_DIR, exist_ok=True)

status = {
    "state": "idle",
    "last_refresh": None,
    "error": None,
}


def _do_refresh():
    try:
        status["state"] = "processing"
        status["error"] = None
        run_refresh(MASTER_PATH, TEMPLATE_PATH, OUTPUT_PATH, DECRYPTED_PATH)
        status["state"] = "ready"
        status["last_refresh"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    except Exception as e:
        status["state"] = "error"
        status["error"] = str(e)
        print(f"Refresh error: {e}")


@app.get("/", response_class=HTMLResponse)
async def allocation():
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(
        content="""
        <html><head><meta http-equiv="refresh" content="0;url=/upload"></head>
        <body>No allocation data yet. Redirecting to upload...</body></html>
        """
    )


@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    return templates.TemplateResponse("upload.html", {
        "request": request,
        "status": status,
    })


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    os.makedirs(DATA_DIR, exist_ok=True)
    contents = await file.read()
    with open(MASTER_PATH, "wb") as f:
        f.write(contents)

    thread = threading.Thread(target=_do_refresh, daemon=True)
    thread.start()

    return RedirectResponse(url="/upload?processing=1", status_code=303)


@app.get("/status")
async def get_status():
    return JSONResponse(status)
