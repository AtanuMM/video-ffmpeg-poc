import os
import uuid
from urllib.parse import quote
import boto3
from botocore.exceptions import ClientError

_AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
_S3_PREFIX = os.getenv("S3_PREFIX", "processed/").lstrip("/")
_PRESIGN_EXPIRES = int(os.getenv("S3_PRESIGN_EXPIRES", "900"))
_CREATE_BUCKET = os.getenv("S3_CREATE_BUCKET", "true").lower() in {"1", "true", "yes"}
_BLOCK_PUBLIC = os.getenv("S3_BLOCK_PUBLIC", "false").lower() in {"1", "true", "yes"}
_AWS_ACL = (os.getenv("AWS_ACL", "") or "").strip() or None
_PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

_s3 = boto3.client("s3", region_name=_AWS_REGION)
_sts = boto3.client("sts", region_name=_AWS_REGION)

def _default_bucket_name() -> str:
    try:
        acct = _sts.get_caller_identity()["Account"]
    except Exception:
        acct = uuid.uuid4().hex[:12]
    base = f"ffmpeg-poc-{acct}-{_AWS_REGION}".lower()
    return base[:63].rstrip("-")

def ensure_bucket(bucket: str | None) -> str:
    name = (bucket or os.getenv("S3_BUCKET") or _default_bucket_name()).lower()
    if not _CREATE_BUCKET:
        return name
    try:
        _s3.head_bucket(Bucket=name)
    except ClientError:
        kwargs = {"Bucket": name}
        if _AWS_REGION != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": _AWS_REGION}
        _s3.create_bucket(**kwargs)
    # Public access block per env
    _s3.put_public_access_block(
        Bucket=name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": _BLOCK_PUBLIC,
            "IgnorePublicAcls": _BLOCK_PUBLIC,
            "BlockPublicPolicy": _BLOCK_PUBLIC,
            "RestrictPublicBuckets": _BLOCK_PUBLIC,
        },
    )
    return name

def make_s3_key(filename: str) -> str:
    prefix = f"{_S3_PREFIX}/" if _S3_PREFIX and not _S3_PREFIX.endswith("/") else _S3_PREFIX
    return f"{prefix}{filename}".lstrip("/")

def upload_file(local_path: str, key: str, *, content_type: str, cache_control: str | None = None, acl: str | None = None, bucket: str | None = None) -> str:
    bkt = ensure_bucket(bucket)
    extra = {"ContentType": content_type}
    if cache_control:
        extra["CacheControl"] = cache_control
    if acl:
        extra["ACL"] = acl
    _s3.upload_file(local_path, bkt, key, ExtraArgs=extra)
    return bkt

def presign_get_url(key: str, *, expires: int | None = None, bucket: str | None = None) -> str:
    bkt = ensure_bucket(bucket)
    return _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bkt, "Key": key},
        ExpiresIn=expires or _PRESIGN_EXPIRES,
    )

def public_url(key: str, *, bucket: str | None = None) -> str:
    bkt = ensure_bucket(bucket)
    if _PUBLIC_BASE_URL:
        return f"{_PUBLIC_BASE_URL}/{quote(key)}"
    host = f"s3.{_AWS_REGION}.amazonaws.com"
    if _AWS_REGION == "us-east-1":
        host = "s3.amazonaws.com"
    return f"https://{bkt}.{host}/{quote(key)}"

def should_return_public_url() -> bool:
    return bool(_PUBLIC_BASE_URL) or ( (_AWS_ACL or "").lower() == "public-read" and not _BLOCK_PUBLIC )
