# services/ffmpeg_service.py
import os, shlex, subprocess, tempfile, uuid, json, zipfile
from typing import List, Dict, Any, Tuple, Optional

FFMPEG = os.getenv("FFMPEG_BIN", "ffmpeg")

# Default watermark config (similar names to image POC)
WM_TEXT       = os.getenv("WATERMARK_TEXT", "© Demo Watermark")
WM_IMAGE      = os.getenv("WATERMARK_IMAGE", "").strip()  # png with alpha if set
WM_FONT       = os.getenv("WATERMARK_FONT", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
WM_FONTSIZE   = int(os.getenv("WATERMARK_POINTSIZE", "28"))
WM_OPACITY    = int(os.getenv("WATERMARK_OPACITY", "35"))   # 0..100
WM_INSET      = int(os.getenv("WATERMARK_INSET", "12"))

# -------- parsing helpers (same ?op=... style) --------
def parse_ops_query(op_params: List[str]) -> List[Dict[str, Any]]:
    """
    Accepts repeated ?op=resize:width=1280,height=-1 style.
    Also supports ?op=quality:value=23 and bare ops like ?op=strip
    """
    ops: List[Dict[str, Any]] = []
    for raw in op_params:
        if not raw: 
            continue
        name, args = (raw.split(":", 1) + [""])[:2]
        item: Dict[str, Any] = {"op": name.strip()}
        if args:
            for part in args.split(","):
                if not part:
                    continue
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

# -------- ffmpeg command builders --------
def _base_cmd(in_path: str) -> List[str]:
    return [FFMPEG, "-hide_banner", "-nostdin", "-y", "-loglevel", "error", "-i", in_path]

def _choose_container_and_codecs(fmt: str) -> Tuple[str, List[str]]:
    fmt = (fmt or "mp4").lower()
    if fmt in ("mp4","m4v","mov"):
        return ("mp4", ["-c:v","libx264","-c:a","aac","-movflags","+faststart"])
    if fmt in ("webm",):
        return ("webm", ["-c:v","libvpx-vp9","-row-mt","1","-c:a","libopus"])
    if fmt in ("mkv",):
        return ("mkv", ["-c:v","libx264","-c:a","aac"])
    if fmt in ("gif",):
        return ("gif", ["-an","-loop","0"])
    if fmt in ("hls","m3u8"):
        # handled separately
        return ("m3u8", ["-c:v","libx264","-c:a","aac","-f","hls","-hls_time","4","-hls_list_size","0"])
    return ("mp4", ["-c:v","libx264","-c:a","aac","-movflags","+faststart"])

def _escape_drawtext(s: str) -> str:
    # Escape chars that break drawtext
    return s.replace("\\","\\\\").replace(":","\\:").replace("'","\\'")

def _apply_ops_and_watermark(cmd: List[str], ops: List[Dict[str,Any]], in_path: str,
                             tmpdir: str) -> Tuple[List[str], Optional[str], Optional[str], bool]:
    """
    Returns: (cmd, forced_ext, vf_or_complex, is_hls)
    """
    vf_filters: List[str] = []
    complex_inputs: List[str] = []  # for overlay/drawtext when we need extra -i
    forced_ext: Optional[str] = None
    is_hls = False

    vcodec_args: List[str] = []
    acodec_args: List[str] = []

    for op in ops:
        name = op.get("op","").lower()

        if name == "format":
            forced_ext = str(op.get("type","mp4")).lower()
            if forced_ext in ("hls","m3u8"): is_hls = True

        elif name == "scale" or name == "resize":
            w = op.get("width", -2)
            h = op.get("height", -2)
            # keep aspect if one is -1 or -2 (ffmpeg requires even numbers; use -2)
            vf_filters.append(f"scale={w}:{h}")

        elif name == "fps":
            fps = op.get("value", 30)
            vf_filters.append(f"fps={fps}")

        elif name == "crop":
            w = op.get("width","iw")
            h = op.get("height","ih")
            x = op.get("x","(iw-ow)/2")
            y = op.get("y","(ih-oh)/2")
            vf_filters.append(f"crop={w}:{h}:{x}:{y}")

        elif name == "bitrate":
            if "video" in op: vcodec_args += ["-b:v", str(op["video"])]
            if "audio" in op: acodec_args += ["-b:a", str(op["audio"])]

        elif name == "crf":
            v = op.get("value", 23)
            vcodec_args += ["-crf", str(v)]

        elif name == "preset":
            vcodec_args += ["-preset", str(op.get("value","veryfast"))]

        elif name == "audio":
            if op.get("remove", False):
                cmd += ["-an"]

        elif name == "rotate":
            deg = int(op.get("degrees",0))
            if deg % 90 == 0:
                # transpose uses 90 deg steps: 1=90R, 2=90L, etc. For arbitrary, use rotate filter.
                if deg == 90: vf_filters.append("transpose=1")
                elif deg == 180: vf_filters.append("transpose=1,transpose=1")
                elif deg == 270: vf_filters.append("transpose=2")
            else:
                rad = deg * 3.14159265 / 180.0
                vf_filters.append(f"rotate={rad}:fillcolor=black")

        elif name in ("grayscale","monochrome"):
            vf_filters.append("format=gray")

        elif name == "thumbnail":
            # single frame at time t (seconds); force png unless format overrides
            t = op.get("at", 1)
            cmd[0:0] = []  # no-op; keep input first
            cmd += ["-ss", str(t), "-frames:v", "1"]
            if not forced_ext:
                forced_ext = "png"

        # more ops can be added: denoise, eq/contrast, etc.

    # Watermark (always)
    overlay_expr = f"overlay=W-w-{WM_INSET}:H-h-{WM_INSET}"
    if WM_IMAGE and os.path.exists(WM_IMAGE):
        # alpha by opacity: use format=auto, colorchannelmixer to fade
        complex_inputs += ["-i", WM_IMAGE]
        alpha = max(0, min(100, WM_OPACITY)) / 100.0
        wm_chain = f"[1:v]format=rgba,colorchannelmixer=aa={alpha}[wm];[0:v][wm]{overlay_expr}[vout]"
        vf_or_complex = ("-filter_complex", wm_chain, "-map", "[vout]")
        # ensure we map audio if exists
        cmd += ["-map","0:a?"]
    else:
        # drawtext watermark
        alpha = max(0, min(100, WM_OPACITY)) / 100.0
        txt = _escape_drawtext(WM_TEXT)
        fill = f"white@{alpha:.2f}"
        stroke = f"black@{alpha*0.6:.2f}"
        # bottom-right using text size th/tw
        draw = f"drawtext=fontfile='{WM_FONT}':text='{txt}':fontsize={WM_FONTSIZE}:fontcolor={fill}:borderw=2:bordercolor={stroke}:x=w-tw-{WM_INSET}:y=h-th-{WM_INSET}"
        vf_filters.append(draw)
        vf_or_complex = None

    # Construct final command parts
    if complex_inputs:
        cmd = cmd[:1] + cmd[1:] + complex_inputs

    if vf_filters and not complex_inputs:
        cmd += ["-vf", ",".join(vf_filters)]

    # codecs if specified
    if vcodec_args: cmd += vcodec_args
    if acodec_args: cmd += acodec_args

    return cmd, forced_ext, (vf_or_complex[0], vf_or_complex[1], vf_or_complex[2]) if complex_inputs else None, is_hls

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
    """
    Returns (out_path, media_type)
    - Supports mp4/webm/mkv/gif + HLS (returned as .zip of playlist+segments)
    - Always applies a default watermark (image or text)
    """
    base_cmd = _base_cmd(in_path)
    cmd, forced_ext, complex_tuple, is_hls = _apply_ops_and_watermark(base_cmd, ops, in_path, tmpdir)

    # Output
    out_ext = forced_ext or "mp4"
    if is_hls:
        # write HLS into its own folder then zip it
        hls_dir = os.path.join(tmpdir, f"hls-{uuid.uuid4().hex[:8]}")
        os.makedirs(hls_dir, exist_ok=True)
        playlist = os.path.join(hls_dir, "index.m3u8")
        seg_tmpl = os.path.join(hls_dir, "seg_%04d.ts")

        # codecs/container for HLS
        _, hls_args = _choose_container_and_codecs("hls")
        # build command
        final = cmd[:]
        if complex_tuple:
            final += list(complex_tuple)  # -filter_complex ..., -map [vout]
        final += hls_args + ["-hls_segment_filename", seg_tmpl, playlist]
        _run(final)

        zip_path = os.path.join(tmpdir, f"hls_{uuid.uuid4().hex[:8]}.zip")
        _zip_dir(hls_dir, zip_path)
        return zip_path, "application/zip"

    # normal single-file outputs
    container, codec_args = _choose_container_and_codecs(out_ext)
    out_path = os.path.join(tmpdir, f"out-{uuid.uuid4().hex}.{container}")

    final = cmd[:]
    if complex_tuple:
        final += list(complex_tuple)
    final += codec_args + [out_path]

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
