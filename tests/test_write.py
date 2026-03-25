"""Tests for write operations: pipe_file, put_file, touch, open write/append."""

import pytest

from tests.conftest import TEST_BUCKET


# ======================================================================
# _pipe_file
# ======================================================================

class TestPipeFile:

    def test_pipe_file_small(self, fs):
        """Small file goes through put_object path."""
        data = b"small payload"
        fs.pipe_file(f"{TEST_BUCKET}/written.bin", data)
        assert fs.cat_file(f"{TEST_BUCKET}/written.bin") == data

    def test_pipe_file_roundtrip(self, fs):
        """Write then read back."""
        data = b"x" * 1024
        path = f"{TEST_BUCKET}/roundtrip.dat"
        fs.pipe_file(path, data)
        assert fs.cat_file(path) == data

    def test_pipe_file_large(self, empty_fs):
        """Large file goes through multipart upload path.

        We use a tiny blocksize to force multipart even with small data.
        """
        from tests.conftest import _make_fs
        from tests.mock_cos import MockCosClient

        client = MockCosClient(buckets={TEST_BUCKET})
        test_fs = _make_fs(client)
        test_fs.blocksize = 10  # 10 bytes → force multipart

        data = b"A" * 30  # 30 bytes → 3 parts with blocksize=10
        # 2 * blocksize = 20, data=30 > 20 → multipart
        test_fs.pipe_file(f"{TEST_BUCKET}/big.dat", data, block_size=10)
        assert test_fs.cat_file(f"{TEST_BUCKET}/big.dat") == data

    def test_pipe_file_multipart_abort_on_error(self, empty_fs):
        """Multipart upload failure triggers abort."""
        from tests.conftest import _make_fs
        from tests.mock_cos import MockCosClient

        client = MockCosClient(buckets={TEST_BUCKET})
        test_fs = _make_fs(client)
        test_fs.blocksize = 10

        # Patch upload_part to fail after first call
        call_count = 0
        original_upload_part = client.upload_part

        def failing_upload_part(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise RuntimeError("simulated upload failure")
            return original_upload_part(**kwargs)

        client.upload_part = failing_upload_part

        with pytest.raises(RuntimeError, match="simulated upload failure"):
            test_fs.pipe_file(f"{TEST_BUCKET}/fail.dat", b"A" * 30, block_size=10)

        # Verify the multipart upload was aborted (no pending uploads remain)
        assert len(client._pending_uploads) == 0


# ======================================================================
# _touch
# ======================================================================

class TestTouch:

    def test_touch_creates_empty_file(self, fs):
        path = f"{TEST_BUCKET}/touched.txt"
        fs.touch(path)
        assert fs.exists(path)
        assert fs.cat_file(path) == b""

    def test_touch_no_truncate_existing(self, fs):
        """truncate=False on an existing file raises NotImplementedError.

        NOTE: The async ``_touch`` in cosfs handles this correctly, but the
        base-class sync ``touch()`` from ``AbstractFileSystem`` takes
        precedence because ``_touch`` is not in fsspec's ``async_methods``
        list.  The base class raises ``NotImplementedError`` for the
        update-timestamp-only case.
        """
        path = f"{TEST_BUCKET}/file1.txt"
        with pytest.raises(NotImplementedError):
            fs.touch(path, truncate=False)

    def test_touch_truncate_existing(self, fs):
        """Default truncate=True should zero out the file."""
        path = f"{TEST_BUCKET}/file1.txt"
        fs.touch(path, truncate=True)
        assert fs.cat_file(path) == b""


# ======================================================================
# _put_file
# ======================================================================

class TestPutFile:

    def test_put_file(self, fs, tmp_path):
        local = tmp_path / "upload_me.txt"
        local.write_bytes(b"local file content")
        fs.put_file(str(local), f"{TEST_BUCKET}/uploaded.txt")
        assert fs.cat_file(f"{TEST_BUCKET}/uploaded.txt") == b"local file content"

    def test_put_file_trailing_slash(self, fs, tmp_path):
        """Trailing slash on rpath should use local filename."""
        local = tmp_path / "myfile.dat"
        local.write_bytes(b"data here")
        fs.put_file(str(local), f"{TEST_BUCKET}/dest/")
        assert fs.cat_file(f"{TEST_BUCKET}/dest/myfile.dat") == b"data here"


# ======================================================================
# COSFile: open write / append / commit / discard
# ======================================================================

class TestOpenWrite:

    def test_open_write(self, fs):
        """Write via fs.open() in 'wb' mode."""
        path = f"{TEST_BUCKET}/written_via_open.txt"
        with fs.open(path, "wb") as f:
            f.write(b"hello from open")
        assert fs.cat_file(path) == b"hello from open"

    def test_open_append(self, fs):
        """Append to an existing file via fs.open() in 'ab' mode."""
        path = f"{TEST_BUCKET}/file1.txt"
        original = fs.cat_file(path)
        with fs.open(path, "ab") as f:
            f.write(b" appended")
        result = fs.cat_file(path)
        assert result == original + b" appended"

    def test_open_append_new_file(self, fs):
        """Append to a file that doesn't exist yet."""
        path = f"{TEST_BUCKET}/brand_new_append.txt"
        with fs.open(path, "ab") as f:
            f.write(b"first chunk")
        assert fs.cat_file(path) == b"first chunk"


class TestCOSFile:

    def test_cosfile_discard(self, fs):
        """Discard should abort the multipart upload."""
        path = f"{TEST_BUCKET}/discard_test.bin"
        f = fs.open(path, "wb", block_size=8)

        # _initiate_upload is called lazily during the first flush.
        # Write enough data to trigger a flush (> block_size).
        f.write(b"A" * 16)
        f.flush()

        # Now the upload should have been initiated
        assert hasattr(f, "upload_id") and f.upload_id is not None
        upload_id = f.upload_id
        assert upload_id in fs.client._pending_uploads

        # Discard
        f.discard()
        assert upload_id not in fs.client._pending_uploads
        assert f.buffer is None
