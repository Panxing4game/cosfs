import bisect
import copy
import errno
import logging
import math
import os
import time
from configparser import ConfigParser
from os.path import expanduser
from typing import Dict, List, Optional, Tuple, Type

import yaml
from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractBufferedFile
from qcloud_cos import CosS3Client, CosConfig, CosServiceError

logger = logging.getLogger("cosfs")

# COS allows at most 10 000 parts per multipart upload.
COS_MAX_PARTS = 10_000

# Default part size: 50 MiB.
_DEFAULT_PART_BYTES = 50 * 2 ** 20


def _ensure_part_size(total_size, part_size=None, limit=COS_MAX_PARTS):
    """Return a part size that keeps the total number of parts within *limit*.

    When *part_size* is omitted the default 50 MiB is used.  If that would
    produce more chunks than *limit*, the part size is enlarged just enough
    to stay within the constraint.
    """
    if part_size is None:
        part_size = _DEFAULT_PART_BYTES
    if total_size <= part_size * limit:
        return part_size
    return int(math.ceil(total_size / limit))

# ---------------------------------------------------------------------------
# Error translation: COS error codes -> Python standard exceptions
# ---------------------------------------------------------------------------
COS_ERROR_CODE_TO_EXCEPTION: Dict[str, Type[Exception]] = {
    # Not found
    "NoSuchKey": FileNotFoundError,
    "NoSuchBucket": FileNotFoundError,
    "NoSuchUpload": FileNotFoundError,
    "404": FileNotFoundError,
    # Permission / auth
    "AccessDenied": PermissionError,
    "SignatureDoesNotMatch": PermissionError,
    "403": PermissionError,
    # Already exists
    "BucketAlreadyExists": FileExistsError,
    "BucketAlreadyOwnedByYou": FileExistsError,
}

# COS error codes that are safe to retry (rate-limiting, transient server errors)
COS_RETRYABLE_ERROR_CODES = {
    "SlowDown", "ServiceUnavailable", "InternalError",
    "RequestTimeout", "RequestTimeTooSkewed",
}


def translate_cos_error(error, message=None):
    """Map a ``CosServiceError`` to the appropriate Python builtin exception."""
    if not isinstance(error, CosServiceError):
        return error

    code = getattr(error, "get_error_code", lambda: None)()
    status_code = str(getattr(error, "get_status_code", lambda: None)())

    # Try error code first, then HTTP status code
    constructor = COS_ERROR_CODE_TO_EXCEPTION.get(code) or COS_ERROR_CODE_TO_EXCEPTION.get(status_code)
    if constructor:
        msg = message or getattr(error, "get_error_msg", lambda: str(error))()
        exc = constructor(msg)
    else:
        msg = message or str(error)
        exc = OSError(errno.EIO, msg)

    exc.__cause__ = error
    return exc


# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------
COS_RETRYABLE_EXCEPTIONS = (ConnectionError, TimeoutError, ConnectionResetError, BrokenPipeError)


def _call_cos(func, *args, retries=3, **kwargs):
    """Invoke a COS SDK method, retrying transient failures with backoff.

    Network-level errors and a known set of COS service error codes are
    retried up to *retries* times.  Permanent COS errors are translated
    into native Python exceptions and raised immediately.
    """
    err = None
    for attempt in range(retries):
        wait = min(2 ** attempt * 0.5, 15)  # 0.5s, 1s, 2s, ... capped at 15s
        try:
            return func(*args, **kwargs)
        except COS_RETRYABLE_EXCEPTIONS as e:
            err = e
            logger.debug("Retryable network error (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(wait)
        except CosServiceError as e:
            err = e
            code = getattr(e, "get_error_code", lambda: None)()
            if code in COS_RETRYABLE_ERROR_CODES:
                logger.debug("Retryable COS error %s (attempt %d/%d): %s", code, attempt + 1, retries, e)
                time.sleep(wait)
            else:
                # Non-retryable COS error -> translate and raise immediately
                raise translate_cos_error(e) from e
        except (OSError, RuntimeError, TypeError, ValueError) as e:
            err = e
            logger.debug("Non-retryable error: %s", e)
            break

    # All retries exhausted
    if isinstance(err, CosServiceError):
        raise translate_cos_error(err) from err
    if err is not None:
        raise err
    raise RuntimeError("_call_cos: unexpected state")


# ---------------------------------------------------------------------------
# COSFileSystem
# ---------------------------------------------------------------------------
class COSFileSystem(AsyncFileSystem):
    protocol = "cosn"
    retries = 3

    def __init__(self, conf_path: Optional[str] = expanduser("~"), secret_id: Optional[str] = None,
                 secret_key: Optional[str] = None, token: Optional[str] = None, region: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)

        if secret_id:
            self.client = CosS3Client(CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token))
        # coscli config
        elif os.path.exists(conf_path + "/.cos.yaml"):
            with open(conf_path + "/.cos.yaml") as f:
                cli_config = yaml.load(f.read(), Loader=yaml.FullLoader)['cos']
                if len(cli_config['buckets']) == 0:
                    raise ValueError("no bucket config found, please check your coscli config file.")
                region = cli_config['buckets'][0]['region']
                self.client = CosS3Client(CosConfig(Region=region, SecretId=cli_config['base']['secretid'],
                                                    SecretKey=cli_config['base']['secretkey'],
                                                    Token=cli_config['base']['sessiontoken']))
        # coscmd config
        elif os.path.exists(conf_path + "/.cos.conf"):
            with open(conf_path + "/.cos.conf", 'r') as f:
                cp = ConfigParser()
                cp.read_file(f)
                if not cp.has_section('common'):
                    raise ValueError("[common] section couldn't be found, please check your coscmd config file.")
                secret_id = cp.get('common', 'secret_id', fallback=cp.get('common', 'access_id', fallback=None))
                region = cp.get('common', 'region')
                self.client = CosS3Client(CosConfig(Region=region, SecretId=secret_id,
                                                    SecretKey=cp.get('common', 'secret_key'),
                                                    Token=cp.get('common', 'token', fallback=None)))
        # env variables
        elif os.environ.get("TENCENTCLOUD_SECRETID"):
            self.client = CosS3Client(CosConfig(Region=os.environ.get("TENCENTCLOUD_REGION"),
                                                SecretId=os.environ.get("TENCENTCLOUD_SECRETID"),
                                                SecretKey=os.environ.get("TENCENTCLOUD_SECRETKEY"),
                                                Token=os.environ.get("TENCENTCLOUD_SESSIONTOKEN")))
        else:
            raise FileNotFoundError("No config file found, see: https://cloud.tencent.com/document/product/436/63144")

        self.region = region

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------
    def split_path(self, path: str) -> Tuple[str, str]:
        trail = path[len(path.rstrip("/")):]
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, ""
        bucket_name, obj_name = path.split("/", 1)
        obj_name += trail
        return bucket_name, obj_name

    def parse_path(self, path: str) -> dict:
        bucket, key = self.split_path(path)
        return {"Bucket": bucket, "Key": key}

    # ------------------------------------------------------------------
    # Core read methods
    # ------------------------------------------------------------------
    async def _cat_file(self, path, start=None, end=None, **kwargs):
        """Fetch the contents (or a byte-range slice) of a COS object."""
        bucket, key = self.split_path(path)
        kw = {}
        if start is not None or end is not None:
            # COS Range header uses inclusive end: bytes=start-(end-1)
            range_start = start or 0
            range_end = f"{end - 1}" if end is not None else ""
            kw["Range"] = f"bytes={range_start}-{range_end}"
        res = _call_cos(self.client.get_object, Bucket=bucket, Key=key, **kw, retries=self.retries)
        return res["Body"].get_raw_stream().read()

    async def _get_file(self, rpath, lpath, **kwargs):
        bucket, key = self.split_path(rpath)
        norm_lpath = lpath.rstrip("/")
        if lpath.endswith("/") or os.path.isdir(lpath):
            norm_lpath += "/" + key.split("/")[-1]
        _call_cos(self.client.download_file, Bucket=bucket, Key=key, DestFilePath=norm_lpath, retries=self.retries)

    # ------------------------------------------------------------------
    # Core write methods
    # ------------------------------------------------------------------
    async def _pipe_file(self, path, value, **kwargs):
        """Upload *value* (bytes) to *path* on COS.

        Objects smaller than ``min(5 GB, 2 × block_size)`` are sent in a
        single PUT request; larger ones use multipart upload.
        """
        bucket, key = self.split_path(path)
        block_size = kwargs.pop("block_size", self.blocksize or 5 * 2 ** 20)
        block_size = _ensure_part_size(len(value), block_size)

        self.invalidate_cache(self._parent(path))

        # Single PUT for small objects (COS caps a single PUT at 5 GB).
        if len(value) < min(5 * 2 ** 30, 2 * block_size):
            _call_cos(self.client.put_object, Bucket=bucket, Key=key, Body=value, retries=self.retries, **kwargs)
            return

        # Multipart upload for larger objects
        mpu = _call_cos(self.client.create_multipart_upload, Bucket=bucket, Key=key, retries=self.retries, **kwargs)
        upload_id = mpu["UploadId"]
        parts = []
        try:
            for i, off in enumerate(range(0, len(value), block_size)):
                part_number = i + 1
                data = value[off:off + block_size]
                out = _call_cos(
                    self.client.upload_part,
                    Bucket=bucket, Key=key, Body=data,
                    PartNumber=part_number, UploadId=upload_id,
                    retries=self.retries,
                )
                parts.append({"ETag": out["ETag"], "PartNumber": part_number})
            _call_cos(
                self.client.complete_multipart_upload,
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Part": parts},
                retries=self.retries,
            )
        except (CosServiceError, OSError, RuntimeError):
            # Clean up failed multipart upload
            try:
                self.client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except (CosServiceError, OSError):
                logger.warning("Failed to abort multipart upload %s for %s/%s", upload_id, bucket, key)
            raise

    async def _put_file(self, lpath, rpath):
        if rpath.endswith("/"):
            rpath += lpath.split("/")[-1]
        _call_cos(self.client.upload_file, **self.parse_path(rpath), LocalFilePath=lpath, retries=self.retries)

    # ------------------------------------------------------------------
    # Info / existence
    # ------------------------------------------------------------------
    async def _info(self, path, **kwargs):
        bucket, key = self.split_path(path)
        if key:
            # Try as a file first
            if not path.endswith("/"):
                try:
                    exists = _call_cos(self.client.object_exists, Bucket=bucket, Key=key, retries=self.retries)
                except (CosServiceError, OSError):
                    exists = False
                if exists:
                    out = _call_cos(self.client.head_object, Bucket=bucket, Key=key, retries=self.retries)
                    return {
                        "ETag": out["ETag"],
                        "Key": f"{bucket}/{key}",
                        "name": f"{bucket}/{key}",
                        "LastModified": out["Last-Modified"],
                        "Size": int(out["Content-Length"]),
                        "size": int(out["Content-Length"]),
                        "type": "file",
                        "StorageClass": "OBJECT",
                    }

            # Try as a directory prefix
            prefix = key.rstrip("/") + "/"
            resp = _call_cos(
                self.client.list_objects, Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1,
                retries=self.retries,
            )
            if resp.get("Contents") or resp.get("CommonPrefixes"):
                return {
                    "Key": f"{bucket}/{key}",
                    "name": f"{bucket}/{key}",
                    "type": "directory",
                    "Size": 0,
                    "size": 0,
                    "StorageClass": "DIRECTORY",
                }

            raise FileNotFoundError(path)
        else:
            # Bucket root
            if bucket:
                # Verify bucket exists by listing with maxkeys=0
                try:
                    _call_cos(self.client.list_objects, Bucket=bucket, MaxKeys=0, retries=self.retries)
                except FileNotFoundError:
                    raise FileNotFoundError(path)
                return {
                    "Key": bucket,
                    "name": bucket,
                    "type": "directory",
                    "Size": 0,
                    "size": 0,
                    "StorageClass": "BUCKET",
                }
            raise FileNotFoundError(path)

    async def _exists(self, path: str):
        try:
            await self._info(path)
            return True
        except FileNotFoundError:
            return False

    # ------------------------------------------------------------------
    # Directory listing (with pagination!)
    # ------------------------------------------------------------------
    def _paginated_list(self, bucket_name, list_prefix):
        """Fetch all objects and common prefixes under *list_prefix* with pagination."""
        all_contents = []
        all_prefixes = []
        marker = ""
        while True:
            resp = _call_cos(
                self.client.list_objects,
                Bucket=bucket_name, Prefix=list_prefix, Delimiter="/", Marker=marker,
                retries=self.retries,
            )
            all_contents.extend(resp.get("Contents", []))
            all_prefixes.extend(resp.get("CommonPrefixes", []))
            if resp.get("IsTruncated") == "true":
                marker = resp.get("NextMarker", "")
                if not marker and all_contents:
                    marker = all_contents[-1]["Key"]
            else:
                break
        return all_contents, all_prefixes

    @staticmethod
    def _obj_to_entry(bucket_name, obj):
        """Convert a COS list-objects item into an fsspec info dict."""
        key = obj["Key"]
        is_dir = key.endswith("/")
        entry = {
            "name": f"{bucket_name}/{key}",
            "Key": f"{bucket_name}/{key}",
            "type": "directory" if is_dir else "file",
            "size": 0 if is_dir else int(obj.get("Size", 0)),
            "Size": 0 if is_dir else int(obj.get("Size", 0)),
            "StorageClass": "DIRECTORY" if is_dir else obj.get("StorageClass", "OBJECT"),
        }
        if "LastModified" in obj:
            entry["LastModified"] = obj["LastModified"]
        if "ETag" in obj:
            entry["ETag"] = obj["ETag"]
        return entry

    async def _ls(self, path, detail=True, **kwargs):
        norm_path = self._strip_protocol(path).strip("/")
        if norm_path in self.dircache:
            out = copy.deepcopy(self.dircache[norm_path])
            if detail:
                return out
            return [o["name"] for o in out]

        bucket_name, prefix = self.split_path(path)
        if bucket_name:
            list_prefix = prefix + "/" if prefix != "" else ""
            all_contents, all_prefixes = self._paginated_list(bucket_name, list_prefix)

            info = [self._obj_to_entry(bucket_name, obj) for obj in all_contents]
            for obj in all_prefixes:
                pfx = obj["Prefix"]
                info.append({
                    "name": f"{bucket_name}/{pfx}".rstrip("/"),
                    "Key": f"{bucket_name}/{pfx}".rstrip("/"),
                    "type": "directory",
                    "size": 0,
                    "Size": 0,
                    "StorageClass": "DIRECTORY",
                })
        else:
            resp = _call_cos(self.client.list_buckets, retries=self.retries)
            info = [{
                "name": bucket["Name"],
                "Key": bucket["Name"],
                "type": "directory",
                "size": 0,
                "Size": 0,
                "StorageClass": "BUCKET",
                "CreateTime": bucket["CreationDate"],
            } for bucket in resp.get("Buckets", {}).get("Bucket", [])]

        self.dircache[norm_path] = info
        if detail:
            return info
        return [o["name"] for o in info]

    # ------------------------------------------------------------------
    # Recursive listing — single-stream flat listing (no Delimiter)
    # ------------------------------------------------------------------
    def _flat_list(self, bucket, search_prefix):
        """Return all file entries under *search_prefix* (non-recursive COS list)."""
        all_objects = []
        marker = ""
        while True:
            resp = _call_cos(
                self.client.list_objects,
                Bucket=bucket, Prefix=search_prefix, Marker=marker,
                retries=self.retries,
            )
            for obj in resp.get("Contents", []):
                obj_key = obj["Key"]
                # Ignore zero-byte keys ending with "/" (COS directory markers)
                if obj_key.endswith("/") and int(obj.get("Size", 0)) == 0:
                    continue
                entry = {
                    "name": f"{bucket}/{obj_key}",
                    "Key": f"{bucket}/{obj_key}",
                    "type": "file",
                    "size": int(obj.get("Size", 0)),
                    "Size": int(obj.get("Size", 0)),
                    "StorageClass": obj.get("StorageClass", "OBJECT"),
                }
                if "LastModified" in obj:
                    entry["LastModified"] = obj["LastModified"]
                if "ETag" in obj:
                    entry["ETag"] = obj["ETag"]
                all_objects.append(entry)

            if resp.get("IsTruncated") == "true":
                marker = resp.get("NextMarker", "")
                if not marker:
                    contents = resp.get("Contents", [])
                    if contents:
                        marker = contents[-1]["Key"]
            else:
                break
        return all_objects

    def _synthesize_dirs(self, bucket, all_objects, prefix):
        """Add synthetic directory entries and optionally update dircache."""
        thisdircache = {}
        dir_names = []  # kept sorted for fast duplicate checking

        for obj in all_objects:
            parts = obj["name"].split("/")
            # Walk up to collect every ancestor directory
            for i in range(1, len(parts)):
                dir_path = "/".join(parts[:i])
                if len(dir_path) <= len(bucket):
                    continue
                # Ordered insert + lookup avoids scanning the whole list
                idx = bisect.bisect_left(dir_names, dir_path)
                if idx < len(dir_names) and dir_names[idx] == dir_path:
                    continue  # already seen
                dir_names.insert(idx, dir_path)
                thisdircache[dir_path] = {
                    "name": dir_path,
                    "Key": dir_path,
                    "type": "directory",
                    "size": 0,
                    "Size": 0,
                    "StorageClass": "DIRECTORY",
                }

        # Merge file entries into the same dict
        for obj in all_objects:
            thisdircache[obj["name"]] = obj

        # Cache the discovered directories (skip when a prefix filter
        # is active because the listing is partial).
        if not prefix:
            self.dircache.update(
                {k: v for k, v in thisdircache.items() if v["type"] == "directory"}
            )

        return sorted(thisdircache.values(), key=lambda x: x["name"])

    async def _find(self, path, maxdepth=None, withdirs=False, detail=False, prefix="", **kwargs):
        if maxdepth is not None:
            return await super()._find(path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs)

        bucket, key = self.split_path(path)
        if not bucket:
            raise ValueError("Cannot recursively list all buckets")

        search_prefix = (key + "/" + prefix) if key else prefix
        all_objects = self._flat_list(bucket, search_prefix)

        if withdirs:
            all_objects = self._synthesize_dirs(bucket, all_objects, prefix)

        if detail:
            return {o["name"]: o for o in all_objects}
        return [o["name"] for o in all_objects]

    # ------------------------------------------------------------------
    # Delete operations
    # ------------------------------------------------------------------
    async def _rm_file(self, path, **kwargs):
        bucket, key = self.split_path(path)
        _call_cos(self.client.delete_object, Bucket=bucket, Key=key, retries=self.retries)
        self.invalidate_cache(self._parent(path))

    async def _rm(self, path, recursive=False, **kwargs):
        """Delete one or more objects, using COS batch-delete (max 1 000 per call)."""
        paths = await self._expand_path(path, recursive=recursive)

        # Separate files (have a key) from buckets (no key)
        files = [p for p in paths if self.split_path(p)[1]]
        dirs = [p for p in paths if not self.split_path(p)[1] and p not in files]

        # Group files by bucket for batch deletion
        by_bucket: Dict[str, List[str]] = {}
        for f in files:
            bucket, key = self.split_path(f)
            by_bucket.setdefault(bucket, []).append(key)

        for bucket, keys in by_bucket.items():
            # Chunk into batches of 1000 (COS limit)
            for i in range(0, len(keys), 1000):
                batch = keys[i:i + 1000]
                delete_spec = {
                    "Quiet": "true",
                    "Object": [{"Key": k} for k in batch],
                }
                _call_cos(
                    self.client.delete_objects,
                    Bucket=bucket, Delete=delete_spec,
                    retries=self.retries,
                )

        # Delete empty buckets
        for d in dirs:
            bucket, _ = self.split_path(d)
            try:
                _call_cos(self.client.delete_bucket, Bucket=bucket, retries=self.retries)
            except (FileNotFoundError, PermissionError, OSError) as e:
                logger.debug("Could not delete bucket %s: %s", bucket, e)

        # Invalidate caches
        for p in paths:
            self.invalidate_cache(p)
            self.invalidate_cache(self._parent(p))

    # ------------------------------------------------------------------
    # File open
    # ------------------------------------------------------------------
    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        return COSFile(self, path, mode, block_size, autocommit, cache_options=cache_options, **kwargs)

    # ------------------------------------------------------------------
    # Copy
    # ------------------------------------------------------------------
    async def _cp_file(self, path1, path2):
        _call_cos(
            self.client.copy,
            **self.parse_path(path2),
            CopySource={**self.parse_path(path1), "Region": self.region},
            retries=self.retries,
        )
        self.invalidate_cache(self._parent(path2))

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------
    async def _mkdir(self, path, create_parents=True, **kwargs):
        """Create a COS bucket at *path*.

        Sub-paths within a bucket are a no-op — COS uses key prefixes
        rather than actual directories.
        """
        bucket, key = self.split_path(path)
        if not bucket:
            raise ValueError("Cannot create root directory")

        if key:
            # Sub-path inside a bucket: no-op for object storage
            return

        # Create bucket
        try:
            _call_cos(self.client.create_bucket, Bucket=bucket, retries=self.retries, **kwargs)
            self.invalidate_cache("")
        except FileExistsError:
            if not create_parents:
                raise

    async def _makedirs(self, path, exist_ok=False):
        try:
            await self._mkdir(path, create_parents=True)
        except FileExistsError:
            if not exist_ok:
                raise

    async def _rmdir(self, path):
        """Delete an empty bucket."""
        bucket, key = self.split_path(path)
        if key:
            # Can't rmdir a sub-path; use rm(recursive=True) instead
            return

        if not bucket:
            raise ValueError("Cannot remove root")

        try:
            _call_cos(self.client.delete_bucket, Bucket=bucket, retries=self.retries)
        except OSError as e:
            # _call_cos translates COS errors; check if it was BucketNotEmpty
            cause = e.__cause__
            if isinstance(cause, CosServiceError):
                code = getattr(cause, "get_error_code", lambda: None)()
                if code == "BucketNotEmpty":
                    raise OSError(errno.ENOTEMPTY, f"Bucket {bucket} is not empty") from cause
            raise

        self.invalidate_cache(path)
        self.invalidate_cache("")

    # ------------------------------------------------------------------
    # Touch
    # ------------------------------------------------------------------
    async def _touch(self, path, truncate=True, **kwargs):
        """Write a zero-byte object, optionally overwriting an existing one."""
        if not truncate and await self._exists(path):
            # COS doesn't support updating mtime without rewriting the object
            return

        bucket, key = self.split_path(path)
        if not key:
            raise ValueError("Cannot touch a bucket")

        _call_cos(self.client.put_object, Bucket=bucket, Key=key, Body=b"", retries=self.retries)
        self.invalidate_cache(self._parent(path))

    # ------------------------------------------------------------------
    # Timestamps
    # ------------------------------------------------------------------
    def created(self, path):
        """COS objects do not have a creation timestamp."""
        return None

    def modified(self, path):
        """Return the last modified time of an object."""
        info = self.info(path)
        return info.get("LastModified")

    # ------------------------------------------------------------------
    # Presigned URL
    # ------------------------------------------------------------------
    def sign(self, path, expiration=3600, **kwargs):
        """Return a temporary URL that grants unauthenticated download access.

        Parameters
        ----------
        path : str
            The cosn:// path to sign.
        expiration : int
            How long the URL remains valid, in seconds (default 3600).
        """
        bucket, key = self.split_path(path)
        return self.client.get_presigned_download_url(
            Bucket=bucket, Key=key, Expired=expiration
        )

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------
    def invalidate_cache(self, path=None):
        """Drop cached directory listings for *path* and every parent up to root."""
        if path is None:
            self.dircache.clear()
            return

        norm_path = self._strip_protocol(path).strip("/")
        self.dircache.pop(norm_path, None)
        # Also invalidate all parent directories up to root
        while "/" in norm_path:
            norm_path = norm_path.rsplit("/", 1)[0]
            self.dircache.pop(norm_path, None)
        # Invalidate root
        self.dircache.pop("", None)

    # ------------------------------------------------------------------
    # Low-level helpers (kept for backward compatibility with COSFile)
    # ------------------------------------------------------------------
    def fetch_object(self, path: str, start: int, end: int) -> bytes:
        res = _call_cos(self.client.get_object, **self.parse_path(path), Range=f"bytes={start}-{end}",
                        retries=self.retries)
        return res["Body"].get_raw_stream().read()

    def append_object(self, path: str, value: bytes, location: Optional[int] = None):
        if location is None:
            location = self.info(path)["size"]
        _call_cos(self.client.append_object, **self.parse_path(path), Position=location, Data=value,
                  retries=self.retries)

    def initiate_multipart_upload(self, path: str):
        return _call_cos(self.client.create_multipart_upload, **self.parse_path(path), retries=self.retries)

    def upload_part(self, path: str, body, upload_id, part_number: int):
        return _call_cos(self.client.upload_part, **self.parse_path(path), Body=body,
                         PartNumber=part_number, UploadId=upload_id, retries=self.retries)

    def complete_multipart_upload(self, path: str, upload_id, parts: list):
        _call_cos(self.client.complete_multipart_upload, **self.parse_path(path), UploadId=upload_id,
                  MultipartUpload={"Part": parts}, retries=self.retries)

    def abort_multipart_upload(self, path: str, upload_id: str):
        """Abort an in-progress multipart upload."""
        try:
            _call_cos(self.client.abort_multipart_upload, **self.parse_path(path), UploadId=upload_id,
                      retries=self.retries)
        except (CosServiceError, OSError):
            logger.warning("Failed to abort multipart upload %s for %s", upload_id, path)


# ---------------------------------------------------------------------------
# COSFile — buffered file implementation
# ---------------------------------------------------------------------------
class COSFile(AbstractBufferedFile):

    def _fetch_range(self, start, end):
        start = max(start, 0)
        end = min(self.size, end)
        if start >= end or start >= self.size:
            return b""
        return self.fs.fetch_object(self.path, start, end)

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload.

        Parameters
        ----------
        final : bool
            If True, this is the last block; complete the file if autocommit is True.
        """
        if "a" in self.mode:
            self.fs.append_object(self.path, self.buffer.getvalue(), self.offset)
        else:
            part_number = len(self.parts) + 1
            self.parts.append({
                **self.fs.upload_part(self.path, self.buffer.getvalue(), self.upload_id, part_number),
                "PartNumber": part_number,
            })
            if final and self.autocommit:
                self.commit()
        return True

    def commit(self):
        """Finalise the multipart upload and refresh the parent listing cache."""
        self.fs.complete_multipart_upload(self.path, self.upload_id, self.parts)
        self.fs.invalidate_cache(self.fs._parent(self.path))

    def discard(self):
        """Cancel a write that has not been committed.

        Aborts the in-flight multipart upload so that partially-written
        parts do not linger on the server and incur storage charges.
        """
        if hasattr(self, "upload_id") and self.upload_id:
            self.fs.abort_multipart_upload(self.path, self.upload_id)
            self.upload_id = None
        self.buffer = None

    def _initiate_upload(self):
        """Prepare the remote side for writing.

        NOTE: appendable objects in COS cannot be copied afterwards.
        """
        if "a" in self.mode:
            if self.fs.exists(self.path):
                self.offset = self.fs.info(self.path)["size"]
            else:
                self.offset = 0
        else:
            self.parts = []
            self.upload_id = self.fs.initiate_multipart_upload(self.path)["UploadId"]
            if self.fs.exists(self.path):
                self.fs.rm_file(self.path)
