# services/ffmpeg_service.py
import os, shlex, subprocess, uuid, zipfile
from typing import List, Dict, Any, Tuple, Optional

FFMPEG = os.getenv("FFMPEG_BIN", "ffmpeg")

# Encoder + defaults (env-driven)
ENCODER         = os.getenv("VIDEO_ENCODER", "libx264")  # libx264 | h264_vaapi
DEFAULT_CRF     = os.getenv("VIDEO_CRF", "23")
DEFAULT_PRESET  = os.getenv("VIDEO_PRESET", "veryfast")

# Watermark
WM_ENABLED      = os.getenv("WATERMARK_ENABLED", "true").lower() == "true"
WM_TEXT         = os.getenv("WATERMARK_TEXT", "© Demo Watermark")
WM_IMAGE        = os.getenv("WATERMARK_IMAGE", "").strip()
WM_FONT         = os.getenv("WATERMARK_FONT", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
WM_FONTSIZE     = int(os.getenv("WATERMARK_POINTSIZE", "28"))
WM_OPACITY      = int(os.getenv("WATERMARK_OPACITY", "35"))   # 0..100
WM_INSET        = int(os.getenv("WATERMARK_INSET", "12"))

def parse_ops_query(op_params: List[str]) -> List[Dict[str, Any]]:
    ops: List[Dict[str, Any]] = []
    for raw in op_params or []:
        if not raw: continue
        name, args = (raw.split(":", 1) + [""])[:2]
        item: Dict[str, Any] = {"op": name.strip().lower()}
        if args:
            for part in args.split(","):
                if not part: continue
                if "=" in part:
                    k, v = part.split("=", 1)
                    v = v.strip()
                    if v.lower() in ("true","false"): v = (v.lower()=="true")
                    else:
                        try:
                            if v.isdigit(): v = int(v)
                            else: v = float(v) if "." in v else v
                        except: pass
                    item[k.strip()] = v
                else:
                    item[part.strip()] = True
        ops.append(item)
    return ops

def _base_cmd(in_path: str) -> List[str]:
    cmd = [FFMPEG, "-hide_banner", "-nostdin", "-y", "-loglevel", "error"]
    if ENCODER == "h264_vaapi":
        cmd += ["-vaapi_device", VAAPI_DEVICE]
    cmd += ["-i", in_path]
    return cmd

def _escape_drawtext(s: str) -> str:
    return s.replace("\\","\\\\").replace(":","\\:").replace("'","\\'")

def _choose_container_and_codecs(fmt: str) -> Tuple[str, List[str]]:
    fmt = (fmt or "mp4").lower()
    threads = ["-threads", "0"]  # use all cores
    if fmt in ("mp4","m4v","mov"):
        if ENCODER == "h264_vaapi":
            # VAAPI encode; quality via -qp (roughly like CRF)
            return ("mp4", threads + [
                "-c:v","h264_vaapi",
                "-b:v","0", "-qp", str(DEFAULT_CRF),
                "-c:a","aac",
                "-movflags","+faststart"
            ])
        else:
            return ("mp4", threads + [
                "-c:v","libx264",
                "-preset", DEFAULT_PRESET, "-crf", str(DEFAULT_CRF),
                "-c:a","aac",
                "-movflags","+faststart",
                "-pix_fmt","yuv420p"
            ])
    if fmt == "webm":
        return ("webm", threads + ["-c:v","libvpx-vp9","-row-mt","1","-c:a","libopus"])
    if fmt == "mkv":
        v = "h264_vaapi" if ENCODER=="h264_vaapi" else "libx264"
        args = ["-c:v", v]
        if v=="libx264": args += ["-preset", DEFAULT_PRESET, "-crf", str(DEFAULT_CRF)]
        elif v=="h264_vaapi": args += ["-b:v","0","-qp",str(DEFAULT_CRF)]
        return ("mkv", threads + args + ["-c:a","aac"])
    if fmt == "gif":
        return ("gif", threads + ["-an","-loop","0"])
    if fmt in ("hls","m3u8"):
        v = "h264_vaapi" if ENCODER=="h264_vaapi" else "libx264"
        args = ["-c:v", v]
        if v=="libx264": args += ["-preset", DEFAULT_PRESET, "-crf", str(DEFAULT_CRF)]
        elif v=="h264_vaapi": args += ["-b:v","0","-qp",str(DEFAULT_CRF)]
        return ("m3u8", threads + args + ["-c:a","aac","-f","hls","-hls_time","4","-hls_list_size","0"])
    return ("mp4", threads + ["-c:v","libx264","-preset",DEFAULT_PRESET,"-crf",str(DEFAULT_CRF),"-c:a","aac","-movflags","+faststart","-pix_fmt","yuv420p"])

def _apply_ops_and_watermark(cmd: List[str], ops: List[Dict[str,Any]], tmpdir: str) -> Tuple[List[str], Optional[str], Optional[Tuple[str,str,str]], bool, List[str]]:
    vf: List[str] = []
    complex_inputs: List[str] = []
    forced_ext: Optional[str] = None
    is_hls = False
    vcodec_args: List[str] = []
    acodec_args: List[str] = []
    used_scale = False

    for op in ops:
        name = op.get("op","")
        if name == "format":
            forced_ext = str(op.get("type","mp4")).lower()
            if forced_ext in ("hls","m3u8"): is_hls = True

        elif name in ("resize","scale"):
            w = op.get("width", -2)
            h = op.get("height", -2)
            vf.append(f"scale={w}:{h}")  # CPU scale; reliable and simple
            used_scale = True

        elif name == "fps":
            vf.append(f"fps={op.get('value',30)}")

        elif name == "crop":
            w = op.get("width","iw"); h = op.get("height","ih")
            x = op.get("x","(iw-ow)/2"); y = op.get("y","(ih-oh)/2")
            vf.append(f"crop={w}:{h}:{x}:{y}")

        elif name == "bitrate":
            if "video" in op: vcodec_args += ["-b:v", str(op["video"])]
            if "audio" in op: acodec_args += ["-b:a", str(op["audio"])]

        elif name == "crf":
            vcodec_args += (["-crf", str(op.get("value",23))] if ENCODER!="h264_vaapi" else ["-qp", str(op.get("value",23))])

        elif name == "preset" and ENCODER!="h264_vaapi":
            vcodec_args += ["-preset", str(op.get("value","veryfast"))]

        elif name == "audio" and op.get("remove", False):
            cmd += ["-an"]

        elif name == "rotate":
            deg = int(op.get("degrees",0))
            if deg % 90 == 0:
                if deg == 90: vf.append("transpose=1")
                elif deg == 180: vf.append("transpose=1,transpose=1")
                elif deg == 270: vf.append("transpose=2")
            else:
                rad = deg * 3.14159265 / 180.0
                vf.append(f"rotate={rad}:fillcolor=black")

        elif name in ("grayscale","monochrome"):
            vf.append("format=gray")

        elif name == "thumbnail":
            t = op.get("at", 1)
            cmd += ["-ss", str(t), "-frames:v", "1"]
            if not forced_ext: forced_ext = "png"

        elif name == "fast":
            # quick dev override
            if ENCODER == "h264_vaapi":
                vcodec_args += ["-qp","28"]
            else:
                vcodec_args += ["-preset","ultrafast","-crf","28"]

    # ---- Watermark (skip for max speed by setting WATERMARK_ENABLED=false) ----
    vf_or_complex = None
    if WM_ENABLED:
        alpha = max(0, min(100, WM_OPACITY)) / 100.0
        if WM_IMAGE and os.path.exists(WM_IMAGE):
            complex_inputs += ["-i", WM_IMAGE]
            chain = f"[1:v]format=rgba,colorchannelmixer=aa={alpha}[wm];[0:v][wm]overlay=W-w-{WM_INSET}:H-h-{WM_INSET}[vout]"
            vf_or_complex = ("-filter_complex", chain, "-map")
        else:
            txt = _escape_drawtext(WM_TEXT)
            fill = f"white@{alpha:.2f}"
            stroke = f"black@{alpha*0.6:.2f}"
            draw = f"drawtext=fontfile='{WM_FONT}':text='{txt}':fontsize={WM_FONTSIZE}:fontcolor={fill}:borderw=2:bordercolor={stroke}:x=w-tw-{WM_INSET}:y=h-th-{WM_INSET}"
            vf.append(draw)

    # VAAPI encode expects hw frames; after CPU filters, upload to GPU
    if ENCODER == "h264_vaapi":
        # If we used filter_complex (image overlay), we map it as [vout] and keep on CPU.
        # Add hwupload at the end of CPU -vf chain so encoder can consume it.
        if not vf_or_complex:
            vf.append("format=nv12,hwupload")

    # attach v/a codec overrides
    extra_codec_args = []
    if vcodec_args: extra_codec_args += vcodec_args
    if acodec_args: extra_codec_args += acodec_args

    return cmd, forced_ext, (vf_or_complex if vf_or_complex else None), is_hls, (["-vf", ",".join(vf)] if vf else []) + extra_codec_args + ([] if not vf_or_complex else ["[vout]"])
    
def _run(cmd: List[str]) -> None:
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"FFmpeg failed: {e}")

def _zip_dir(src_dir: str, zip_path: str):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, src_dir)
                z.write(full, rel)

def process_video(in_path: str, ops: List[Dict[str,Any]], tmpdir: str) -> Tuple[str, str]:
    base_cmd = _base_cmd(in_path)
    cmd, forced_ext, complex_tuple, is_hls, vf_and_codecs = _apply_ops_and_watermark(base_cmd, ops, tmpdir)

    # HLS path (writes dir then zips)
    if (forced_ext or "") in ("hls","m3u8") or is_hls:
        hls_dir = os.path.join(tmpdir, f"hls-{uuid.uuid4().hex[:8]}")
        os.makedirs(hls_dir, exist_ok=True)
        playlist = os.path.join(hls_dir, "index.m3u8")
        seg_tmpl = os.path.join(hls_dir, "seg_%04d.ts")
        container, codec_args = _choose_container_and_codecs("hls")

        final = cmd[:]
        if complex_tuple:
            # -filter_complex chain, then map video and optional audio
            final += [complex_tuple[0], complex_tuple[1], complex_tuple[2], "[vout]", "-map", "0:a?"]
        final += vf_and_codecs + codec_args + ["-hls_segment_filename", seg_tmpl, playlist]
        _run(final)

        zip_path = os.path.join(tmpdir, f"hls_{uuid.uuid4().hex[:8]}.zip")
        _zip_dir(hls_dir, zip_path)
        return zip_path, "application/zip"

    # Single file path
    out_ext = forced_ext or "mp4"
    container, codec_args = _choose_container_and_codecs(out_ext)
    out_path = os.path.join(tmpdir, f"out-{uuid.uuid4().hex}.{container}")

    final = cmd[:]
    if complex_tuple:
        final += [complex_tuple[0], complex_tuple[1], complex_tuple[2], "[vout]", "-map", "0:a?"]
    final += vf_and_codecs + codec_args + [out_path]
    _run(final)

    media_type = {
        "mp4": "video/mp4",
        "webm": "video/webm",
        "mkv": "video/x-matroska",
        "mov": "video/quicktime",
        "gif": "image/gif",
        "m3u8": "application/vnd.apple.mpegurl",
    }.get(container, "application/octet-stream")

    return out_path, media_type
