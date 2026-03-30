"""Tests for miscellaneous operations: cp_file, mkdir, sign, timestamps,
invalidate_cache, error translation, retry logic, path parsing."""

import errno
from unittest.mock import patch

import pytest

from cosfs.core import (
    translate_cos_error, _call_cos,
    _ensure_part_size, COS_MAX_PARTS,
)
from tests.conftest import TEST_BUCKET
from tests.mock_cos import make_cos_error


# ======================================================================
# _ensure_part_size
# ======================================================================

class TestEnsurePartSize:

    def test_default_chunksize_small_file(self):
        """Small file should use default 50 MiB chunk."""
        cs = _ensure_part_size(100 * 2 ** 20)
        assert cs == 50 * 2 ** 20

    def test_auto_increase_large_file(self):
        """File >500 GiB should auto-increase chunk size to stay within part limit."""
        filesize = 600 * 2 ** 30  # 600 GiB
        cs = _ensure_part_size(filesize)
        assert cs > 50 * 2 ** 20
        assert filesize / cs <= COS_MAX_PARTS

    def test_explicit_chunksize_respected(self):
        """Explicit chunksize should be kept when it's sufficient."""
        cs = _ensure_part_size(100 * 2 ** 20, part_size=10 * 2 ** 20)
        assert cs == 10 * 2 ** 20

    def test_explicit_chunksize_too_small(self):
        """Explicit chunksize should be increased when file is too large for it."""
        filesize = 200 * 2 ** 30  # 200 GiB
        tiny_chunk = 1 * 2 ** 20  # 1 MiB → would need 200k parts
        cs = _ensure_part_size(filesize, part_size=tiny_chunk)
        assert cs > tiny_chunk
        assert filesize / cs <= COS_MAX_PARTS


# ======================================================================
# _cp_file
# ======================================================================

class TestCpFile:

    def test_cp_file(self, fs):
        src = f"{TEST_BUCKET}/file1.txt"
        dst = f"{TEST_BUCKET}/file1_copy.txt"
        fs.cp_file(src, dst)
        assert fs.cat_file(dst) == fs.cat_file(src)

    def test_cp_file_across_dirs(self, fs):
        src = f"{TEST_BUCKET}/file1.txt"
        dst = f"{TEST_BUCKET}/data/file1_copy.txt"
        fs.cp_file(src, dst)
        assert fs.cat_file(dst) == b"hello, world!"


# ======================================================================
# _mkdir / _makedirs
# ======================================================================

class TestMkdir:

    def test_mkdir_bucket(self, empty_fs):
        new_bucket = "new-bucket-1250000000"
        empty_fs.mkdir(new_bucket)
        assert new_bucket in empty_fs.client._buckets

    def test_mkdir_subpath_noop(self, fs):
        """mkdir on a sub-path within a bucket is a no-op."""
        # Should not raise or create anything special
        fs.mkdir(f"{TEST_BUCKET}/some/deep/path")

    def test_makedirs_exist_ok(self, fs):
        """makedirs with exist_ok=True should not raise on existing bucket."""
        fs.makedirs(TEST_BUCKET, exist_ok=True)

    def test_makedirs_not_exist_ok(self, fs):
        """makedirs with exist_ok=False on existing bucket.

        NOTE: Currently ``_makedirs`` passes ``create_parents=True`` to
        ``_mkdir``, which swallows the ``FileExistsError`` internally.
        As a result, ``exist_ok=False`` does **not** raise for buckets.
        This test documents the current behaviour.
        """
        # Should NOT raise due to the create_parents=True in _mkdir
        fs.makedirs(TEST_BUCKET, exist_ok=False)


# ======================================================================
# sign
# ======================================================================

class TestSign:

    def test_sign(self, fs):
        url = fs.sign(f"{TEST_BUCKET}/file1.txt")
        assert isinstance(url, str)
        assert TEST_BUCKET in url
        assert "file1.txt" in url

    def test_sign_custom_expiration(self, fs):
        url = fs.sign(f"{TEST_BUCKET}/file1.txt", expiration=7200)
        assert "expired=7200" in url


# ======================================================================
# Timestamps
# ======================================================================

class TestTimestamps:

    def test_modified(self, fs):
        result = fs.modified(f"{TEST_BUCKET}/file1.txt")
        assert result is not None
        assert isinstance(result, str)

    def test_created_returns_none(self, fs):
        result = fs.created(f"{TEST_BUCKET}/file1.txt")
        assert result is None


# ======================================================================
# invalidate_cache
# ======================================================================

class TestInvalidateCache:

    def test_invalidate_cache_path(self, fs):
        """Invalidating a path should clear it and all ancestors."""
        # Populate caches
        fs.ls(TEST_BUCKET)
        fs.ls(f"{TEST_BUCKET}/data")

        fs.invalidate_cache(f"{TEST_BUCKET}/data/a.csv")

        # The file's parent and all ancestors should be cleared
        assert f"{TEST_BUCKET}/data" not in fs.dircache
        assert TEST_BUCKET not in fs.dircache
        assert "" not in fs.dircache

    def test_invalidate_cache_all(self, fs):
        """path=None should clear entire cache."""
        fs.ls(TEST_BUCKET)
        fs.ls(f"{TEST_BUCKET}/data")

        fs.invalidate_cache()
        assert len(fs.dircache) == 0

    def test_invalidate_cache_already_empty(self, fs):
        """Should not raise when cache is already empty."""
        fs.invalidate_cache("nonexistent/path")


# ======================================================================
# Error translation
# ======================================================================

class TestErrorTranslation:

    def test_not_found(self):
        err = make_cos_error("NoSuchKey", 404, "key not found")
        result = translate_cos_error(err)
        assert isinstance(result, FileNotFoundError)

    def test_no_such_bucket(self):
        err = make_cos_error("NoSuchBucket", 404, "bucket not found")
        result = translate_cos_error(err)
        assert isinstance(result, FileNotFoundError)

    def test_permission(self):
        err = make_cos_error("AccessDenied", 403, "access denied")
        result = translate_cos_error(err)
        assert isinstance(result, PermissionError)

    def test_unknown_error(self):
        err = make_cos_error("SomeRandomError", 500, "something broke")
        result = translate_cos_error(err)
        assert isinstance(result, OSError)
        assert result.errno == errno.EIO

    def test_non_cos_error_passthrough(self):
        """Non-CosServiceError should be returned as-is."""
        err = ValueError("not a cos error")
        assert translate_cos_error(err) is err

    def test_cause_chain(self):
        """Translated exception should have __cause__ set."""
        err = make_cos_error("NoSuchKey", 404, "gone")
        result = translate_cos_error(err)
        assert result.__cause__ is err


# ======================================================================
# Retry logic
# ======================================================================

class TestRetry:

    def test_retry_on_network_error(self):
        """ConnectionError should be retried."""
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("network blip")
            return "success"

        with patch("cosfs.core.time.sleep"):  # skip actual sleep
            result = _call_cos(flaky, retries=3)
        assert result == "success"
        assert call_count == 3

    def test_retry_on_retryable_cos_error(self):
        """SlowDown COS error should be retried."""
        call_count = 0

        def throttled():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise make_cos_error("SlowDown", 503, "slow down")
            return "ok"

        with patch("cosfs.core.time.sleep"):
            result = _call_cos(throttled, retries=3)
        assert result == "ok"
        assert call_count == 2

    def test_no_retry_on_non_retryable_error(self):
        """NoSuchKey should NOT be retried — raise immediately."""
        call_count = 0

        def not_found():
            nonlocal call_count
            call_count += 1
            raise make_cos_error("NoSuchKey", 404, "not found")

        with pytest.raises(FileNotFoundError):
            _call_cos(not_found, retries=3)
        assert call_count == 1

    def test_retries_exhausted(self):
        """After exhausting retries, the last error should be raised."""
        def always_fail():
            raise ConnectionError("persistent failure")

        with patch("cosfs.core.time.sleep"):
            with pytest.raises(ConnectionError):
                _call_cos(always_fail, retries=3)

    def test_retries_exhausted_cos_error(self):
        """After exhausting retries on retryable COS error, it should be translated."""
        def always_slow():
            raise make_cos_error("SlowDown", 503, "always slow")

        with patch("cosfs.core.time.sleep"):
            with pytest.raises(OSError):
                _call_cos(always_slow, retries=2)


# ======================================================================
# Path parsing
# ======================================================================

class TestSplitPath:

    def test_split_path_basic(self, fs):
        bucket, key = fs.split_path("mybucket/some/key.txt")
        assert bucket == "mybucket"
        assert key == "some/key.txt"

    def test_split_path_bucket_only(self, fs):
        bucket, key = fs.split_path("mybucket")
        assert bucket == "mybucket"
        assert key == ""

    def test_split_path_with_protocol(self, fs):
        bucket, key = fs.split_path("cosn://mybucket/some/key.txt")
        assert bucket == "mybucket"
        assert key == "some/key.txt"

    def test_split_path_with_protocol_bucket_only(self, fs):
        bucket, key = fs.split_path("cosn://mybucket")
        assert bucket == "mybucket"
        assert key == ""

    def test_split_path_leading_slash(self, fs):
        bucket, key = fs.split_path("/mybucket/key.txt")
        assert bucket == "mybucket"
        assert key == "key.txt"

    def test_split_path_trailing_slash(self, fs):
        """Trailing slash should be preserved on the key (directory placeholders)."""
        bucket, key = fs.split_path("cosn://mybucket/dir/")
        assert bucket == "mybucket"
        assert key == "dir/"

    def test_split_path_trailing_slash_nested(self, fs):
        """Trailing slash preserved for nested paths."""
        bucket, key = fs.split_path("cosn://mybucket/a/b/")
        assert bucket == "mybucket"
        assert key == "a/b/"
