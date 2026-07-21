import logging
import os

from fastapi import FastAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.config import settings
from app.routers import connection, scan, patterns
from app.ws import scan_progress

app = FastAPI(title="KeyView", version="1.0.0")

app.include_router(connection.router)
app.include_router(scan.router)
app.include_router(patterns.router)
app.include_router(scan_progress.router)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


static_dir = os.path.abspath(settings.static_dir)
if os.path.isdir(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = os.path.join(static_dir, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(static_dir, "index.html"))
