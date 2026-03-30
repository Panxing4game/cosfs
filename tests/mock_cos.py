"""In-memory mock of ``CosS3Client`` for unit-testing cosfs.

Every public method that ``cosfs/core.py`` calls on the COS SDK client is
replicated here with pure-Python logic backed by simple dicts and sets.
"""
# pylint: disable=invalid-name
# Parameter names (Bucket, Key, …) intentionally match the COS SDK's PascalCase API.

import io
import re
import uuid
from datetime import datetime, timezone

from qcloud_cos import CosServiceError


# ---------------------------------------------------------------------------
# Helper: construct CosServiceError
# ---------------------------------------------------------------------------
def make_cos_error(code, status_code=400, message="mock error"):
    """Build a ``CosServiceError`` the same way the real SDK does."""
    # CosServiceError.__init__(self, method, msg_dict, status_code)
    return CosServiceError(
        "MOCK",
        {
            "code": code,
            "message": message,
            "resource": "/",
            "requestid": "mock-request-id",
            "traceid": "mock-trace-id",
        },
        status_code,
    )


# ---------------------------------------------------------------------------
# FakeStreamBody: simulates qcloud_cos.StreamBody
# ---------------------------------------------------------------------------
class _FakeRawStream:
    """Minimal file-like wrapper returned by ``FakeStreamBody.get_raw_stream()``."""

    def __init__(self, data: bytes):
        self._stream = io.BytesIO(data)

    def read(self, amt=-1):
        return self._stream.read(amt)


class FakeStreamBody:
    """Drop-in replacement for ``qcloud_cos.StreamBody``.

    Supports the ``get_raw_stream().read()`` call chain used by cosfs.
    """

    def __init__(self, data: bytes):
        self._data = data

    def get_raw_stream(self):
        return _FakeRawStream(self._data)


# ---------------------------------------------------------------------------
# MockCosClient
# ---------------------------------------------------------------------------
class MockCosClient:
    """Pure-Python mock of ``qcloud_cos.CosS3Client``.

    Parameters
    ----------
    buckets : set[str] | None
        Pre-existing bucket names.
    objects : dict[(str, str), bytes] | None
        Pre-existing objects keyed by ``(bucket, key)``.
    """

    def __init__(self, buckets=None, objects=None):
        self._buckets: set = set(buckets or [])
        # (bucket, key) -> bytes
        self._objects: dict = dict(objects or {})
        # upload_id -> {"bucket": str, "key": str, "parts": {part_no: bytes}}
        self._pending_uploads: dict = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _require_bucket(self, bucket):
        if bucket not in self._buckets:
            raise make_cos_error("NoSuchBucket", 404, f"Bucket {bucket} does not exist")

    def _require_key(self, bucket, key):
        self._require_bucket(bucket)
        if (bucket, key) not in self._objects:
            raise make_cos_error("NoSuchKey", 404, f"{bucket}/{key} not found")

    @staticmethod
    def _now_str():
        return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

    @staticmethod
    def _etag():
        return f'"{uuid.uuid4().hex}"'

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------
    def get_object(self, Bucket, Key, **kwargs):
        self._require_key(Bucket, Key)
        data = self._objects[(Bucket, Key)]
        range_header = kwargs.get("Range")
        if range_header:
            m = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if m:
                start = int(m.group(1))
                end = int(m.group(2)) + 1 if m.group(2) else len(data)
                data = data[start:end]
        return {"Body": FakeStreamBody(data)}

    def head_object(self, Bucket, Key, **kwargs):
        self._require_key(Bucket, Key)
        data = self._objects[(Bucket, Key)]
        return {
            "Content-Length": str(len(data)),
            "ETag": self._etag(),
            "Last-Modified": self._now_str(),
            "Content-Type": "application/octet-stream",
        }

    def object_exists(self, Bucket, Key, **kwargs):
        self._require_bucket(Bucket)
        return (Bucket, Key) in self._objects

    # ------------------------------------------------------------------
    # List methods
    # ------------------------------------------------------------------
    def list_objects(self, Bucket, Prefix="", Delimiter="", Marker="", MaxKeys=1000, **kwargs):
        self._require_bucket(Bucket)

        # Gather all matching keys
        matching_keys = sorted(
            k for (b, k) in self._objects if b == Bucket and k.startswith(Prefix)
        )

        # Filter by marker
        if Marker:
            matching_keys = [k for k in matching_keys if k > Marker]

        contents = []
        common_prefixes = set()

        for key in matching_keys:
            if Delimiter:
                # Check if there's a delimiter after the prefix
                suffix = key[len(Prefix):]
                delim_pos = suffix.find(Delimiter)
                if delim_pos >= 0:
                    # This key belongs under a common prefix
                    cpfx = Prefix + suffix[: delim_pos + len(Delimiter)]
                    common_prefixes.add(cpfx)
                    continue

            data = self._objects[(Bucket, key)]
            contents.append({
                "Key": key,
                "Size": str(len(data)),
                "LastModified": self._now_str(),
                "ETag": self._etag(),
                "StorageClass": "STANDARD",
            })

        # Sort common prefixes and build response dicts
        cpfx_list = [{"Prefix": p} for p in sorted(common_prefixes)]

        # Apply MaxKeys pagination (MaxKeys applies to contents only in this mock)
        is_truncated = "false"
        next_marker = ""
        if MaxKeys is not None and len(contents) > MaxKeys:
            is_truncated = "true"
            contents = contents[:MaxKeys]
            next_marker = contents[-1]["Key"] if contents else ""

        result = {
            "IsTruncated": is_truncated,
            "NextMarker": next_marker,
        }
        if contents:
            result["Contents"] = contents
        if cpfx_list:
            result["CommonPrefixes"] = cpfx_list
        return result

    def list_buckets(self, **kwargs):
        buckets = [
            {"Name": b, "CreationDate": self._now_str()} for b in sorted(self._buckets)
        ]
        return {"Buckets": {"Bucket": buckets}}

    # ------------------------------------------------------------------
    # Write methods
    # ------------------------------------------------------------------
    def put_object(self, Bucket, Key, Body=b"", **kwargs):
        self._require_bucket(Bucket)
        if isinstance(Body, str):
            Body = Body.encode("utf-8")
        self._objects[(Bucket, Key)] = Body
        return {"ETag": self._etag()}

    def create_multipart_upload(self, Bucket, Key, **kwargs):
        self._require_bucket(Bucket)
        upload_id = uuid.uuid4().hex
        self._pending_uploads[upload_id] = {
            "bucket": Bucket,
            "key": Key,
            "parts": {},
        }
        return {"UploadId": upload_id}

    def upload_part(self, Bucket, Key, Body, PartNumber, UploadId, **kwargs):
        if UploadId not in self._pending_uploads:
            raise make_cos_error("NoSuchUpload", 404, f"Upload {UploadId} not found")
        if isinstance(Body, str):
            Body = Body.encode("utf-8")
        # Support Body being a BytesIO or similar
        if hasattr(Body, "read"):
            Body = Body.read()
        self._pending_uploads[UploadId]["parts"][PartNumber] = Body
        return {"ETag": self._etag()}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload, **kwargs):
        if UploadId not in self._pending_uploads:
            raise make_cos_error("NoSuchUpload", 404, f"Upload {UploadId} not found")
        upload = self._pending_uploads.pop(UploadId)
        # Assemble parts in order
        part_numbers = sorted(upload["parts"].keys())
        data = b"".join(upload["parts"][n] for n in part_numbers)
        self._objects[(Bucket, Key)] = data
        return {"ETag": self._etag()}

    def abort_multipart_upload(self, Bucket, Key, UploadId, **kwargs):
        self._pending_uploads.pop(UploadId, None)

    # ------------------------------------------------------------------
    # Delete methods
    # ------------------------------------------------------------------
    def delete_object(self, Bucket, Key, **kwargs):
        self._require_bucket(Bucket)
        self._objects.pop((Bucket, Key), None)
        return {}

    def delete_objects(self, Bucket, Delete, **kwargs):
        self._require_bucket(Bucket)
        for obj in Delete.get("Object", []):
            self._objects.pop((Bucket, obj["Key"]), None)
        return {}

    # ------------------------------------------------------------------
    # Bucket management
    # ------------------------------------------------------------------
    def create_bucket(self, Bucket, **kwargs):
        if Bucket in self._buckets:
            raise make_cos_error("BucketAlreadyOwnedByYou", 409, f"Bucket {Bucket} already exists")
        self._buckets.add(Bucket)

    def delete_bucket(self, Bucket, **kwargs):
        self._require_bucket(Bucket)
        # Check if bucket has objects
        has_objects = any(b == Bucket for (b, _) in self._objects)
        if has_objects:
            raise make_cos_error("BucketNotEmpty", 409, f"Bucket {Bucket} is not empty")
        self._buckets.discard(Bucket)

    # ------------------------------------------------------------------
    # File transfer helpers
    # ------------------------------------------------------------------
    def download_file(self, Bucket, Key, DestFilePath, **kwargs):
        self._require_key(Bucket, Key)
        data = self._objects[(Bucket, Key)]
        with open(DestFilePath, "wb") as f:
            f.write(data)

    def upload_file(self, Bucket, Key, LocalFilePath, **kwargs):
        self._require_bucket(Bucket)
        with open(LocalFilePath, "rb") as f:
            data = f.read()
        self._objects[(Bucket, Key)] = data
        return {"ETag": self._etag()}

    # ------------------------------------------------------------------
    # Copy
    # ------------------------------------------------------------------
    def copy(self, Bucket, Key, CopySource, **kwargs):
        src_bucket = CopySource["Bucket"]
        src_key = CopySource["Key"]
        self._require_key(src_bucket, src_key)
        self._require_bucket(Bucket)
        self._objects[(Bucket, Key)] = self._objects[(src_bucket, src_key)]
        return {"ETag": self._etag()}

    # ------------------------------------------------------------------
    # Presigned URL
    # ------------------------------------------------------------------
    def get_presigned_download_url(self, Bucket, Key, Expired=3600, **kwargs):
        return f"https://{Bucket}.cos.ap-guangzhou.myqcloud.com/{Key}?sign=mock&expired={Expired}"

    # ------------------------------------------------------------------
    # Append (used by COSFile in append mode)
    # ------------------------------------------------------------------
    def append_object(self, Bucket, Key, Position=0, Data=b"", **kwargs):
        self._require_bucket(Bucket)
        existing = self._objects.get((Bucket, Key), b"")
        if isinstance(Data, str):
            Data = Data.encode("utf-8")
        if hasattr(Data, "read"):
            Data = Data.read()
        # Pad with zeros if Position is beyond current length
        if Position > len(existing):
            existing = existing + b"\x00" * (Position - len(existing))
        self._objects[(Bucket, Key)] = existing[:Position] + Data
