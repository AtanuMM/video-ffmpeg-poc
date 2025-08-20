# video_main.py
import os, tempfile, uuid, shutil, time
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from starlette.background import BackgroundTask

from services.ffmpeg_service import parse_ops_query, process_video

app = FastAPI(title="FFmpeg POC")

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

@app.post("/video/process")
async def process_video_endpoint(
    file: UploadFile = File(...),
    op: Optional[List[str]] = Query(None),
    ops: Optional[List[str]] = Query(None),
):
    t0 = time.perf_counter()
    ops_list = parse_ops_query((op or []) + (ops or []))
    tmpdir = tempfile.mkdtemp(prefix="ffx-")
    in_path = os.path.join(tmpdir, f"in-{uuid.uuid4().hex}")

    # stream upload to disk (no big read() in RAM)
    with open(in_path, "wb") as out:
        while True:
            chunk = await file.read(1024 * 1024)  # 1MB
            if not chunk: break
            out.write(chunk)
    t1 = time.perf_counter()

    try:
        out_path, media_type = process_video(in_path, ops_list, tmpdir)
        t2 = time.perf_counter()
        print(f"[ffx] upload={t1-t0:.2f}s, process={t2-t1:.2f}s, total={t2-t0:.2f}s")

        def _cleanup(): shutil.rmtree(tmpdir, ignore_errors=True)
        return FileResponse(out_path, media_type=media_type,
                            filename=os.path.basename(out_path),
                            background=BackgroundTask(_cleanup))
    except Exception as e:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(500, str(e))
