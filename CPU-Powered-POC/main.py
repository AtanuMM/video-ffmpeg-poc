# video_main.py
import os, tempfile, uuid, shutil, time
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Query, HTTPException 
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from starlette.background import BackgroundTask

from services.ffmpeg_service import parse_ops_query, process_video
from services.s3_utils import make_s3_key, upload_file, presign_get_url, public_url, should_return_public_url

app = FastAPI(title="FFmpeg POC")

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

# @app.post("/video/process")
# async def process_video_endpoint(
#     file: UploadFile = File(...),
#     op: Optional[List[str]] = Query(None),
#     ops: Optional[List[str]] = Query(None),
# ):
#     t0 = time.perf_counter()
#     ops_list = parse_ops_query((op or []) + (ops or []))
#     tmpdir = tempfile.mkdtemp(prefix="ffx-")
#     in_path = os.path.join(tmpdir, f"in-{uuid.uuid4().hex}")

#     # stream upload to disk (no big read() in RAM)
#     with open(in_path, "wb") as out:
#         while True:
#             chunk = await file.read(1024 * 1024)  # 1MB
#             if not chunk: break
#             out.write(chunk)
#     t1 = time.perf_counter()

#     try:
#         out_path, media_type = process_video(in_path, ops_list, tmpdir)
#         t2 = time.perf_counter()
#         print(f"[ffx] upload={t1-t0:.2f}s, process={t2-t1:.2f}s, total={t2-t0:.2f}s")

#         def _cleanup(): shutil.rmtree(tmpdir, ignore_errors=True)
#         return FileResponse(out_path, media_type=media_type,
#                             filename=os.path.basename(out_path),
#                             background=BackgroundTask(_cleanup))
#     except Exception as e:
#         shutil.rmtree(tmpdir, ignore_errors=True)
#         raise HTTPException(500, str(e))


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

    # stream upload to disk
    with open(in_path, "wb") as out:
        while True:
            chunk = await file.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            out.write(chunk)
    t1 = time.perf_counter()

    try:
        out_path, media_type = process_video(in_path, ops_list, tmpdir)
        t2 = time.perf_counter()
        print(f"[ffx] upload={t1-t0:.2f}s, process={t2-t1:.2f}s, total={t2-t0:.2f}s")

        # ---- S3 upload block ----
        ext = os.path.splitext(out_path)[1].lstrip(".") or "mp4"
        key = make_s3_key(f"{uuid.uuid4().hex}.{ext}")

        bucket = upload_file(
            out_path,
            key,
            content_type=media_type,
            cache_control="public,max-age=31536000,immutable",
            acl=(os.getenv("AWS_ACL", "").strip() or None),
        )

        url = public_url(key, bucket=bucket) if should_return_public_url() \
              else presign_get_url(key, bucket=bucket)
        # -------------------------

        # local cleanup since we return JSON, not a streaming file
        shutil.rmtree(tmpdir, ignore_errors=True)

        return JSONResponse({
            "bucket": bucket,
            "bucketKey": key,
            "url": url,
            "contentType": media_type,
            "ext": ext,
        })

    except Exception as e:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(500, str(e))