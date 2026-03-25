"""Tests for delete operations: rm_file, rm (bulk), rmdir."""

import asyncio

import pytest

from tests.conftest import TEST_BUCKET
from tests.mock_cos import MockCosClient
from tests.conftest import _make_fs


# ======================================================================
# _rm_file
# ======================================================================

class TestRmFile:

    def test_rm_file(self, fs):
        path = f"{TEST_BUCKET}/file1.txt"
        assert fs.exists(path)
        fs.rm_file(path)
        assert not fs.exists(path)

    def test_rm_file_cache_invalidation(self, fs):
        """After rm, the parent directory cache should be invalidated."""
        parent = TEST_BUCKET
        # Populate cache
        fs.ls(parent)
        assert fs._strip_protocol(parent).strip("/") in fs.dircache

        fs.rm_file(f"{TEST_BUCKET}/file1.txt")
        # Cache should be cleared for parent
        assert fs._strip_protocol(parent).strip("/") not in fs.dircache


# ======================================================================
# _rm (recursive / batch)
# ======================================================================

class TestRm:

    def test_rm_recursive(self, fs):
        """Recursively delete a directory prefix."""
        # Ensure files exist
        assert fs.exists(f"{TEST_BUCKET}/data/a.csv")
        assert fs.exists(f"{TEST_BUCKET}/data/b.csv")

        fs.rm(f"{TEST_BUCKET}/data", recursive=True)

        assert not fs.exists(f"{TEST_BUCKET}/data/a.csv")
        assert not fs.exists(f"{TEST_BUCKET}/data/b.csv")
        assert not fs.exists(f"{TEST_BUCKET}/data/sub/deep.json")

    def test_rm_bulk_batching(self):
        """Verify that >1000 files are deleted in batches."""
        # Create 1500 objects
        objs = {
            (TEST_BUCKET, f"bulk/file{i:04d}.txt"): b"x"
            for i in range(1500)
        }
        client = MockCosClient(buckets={TEST_BUCKET}, objects=objs)

        # Track calls to delete_objects
        delete_calls = []
        original_delete = client.delete_objects

        def tracking_delete(Bucket, Delete, **kwargs):
            delete_calls.append(len(Delete.get("Object", [])))
            return original_delete(Bucket=Bucket, Delete=Delete, **kwargs)

        client.delete_objects = tracking_delete
        test_fs = _make_fs(client)

        test_fs.rm(f"{TEST_BUCKET}/bulk", recursive=True)

        # Should have been called in batches of ≤1000
        assert all(n <= 1000 for n in delete_calls)
        # _expand_path returns 1500 files + the "bulk" directory path itself
        # (which has a non-empty key and is thus included in batch delete)
        total_deleted = sum(delete_calls)
        assert total_deleted >= 1500
        # At least 2 batches
        assert len(delete_calls) >= 2

    def test_rm_cache_invalidation(self, fs):
        """rm should clear caches for all affected paths."""
        # Populate caches
        fs.ls(TEST_BUCKET)
        fs.ls(f"{TEST_BUCKET}/data")

        fs.rm(f"{TEST_BUCKET}/data/a.csv")

        # Both caches should be invalidated
        norm = fs._strip_protocol(f"{TEST_BUCKET}/data").strip("/")
        assert norm not in fs.dircache


# ======================================================================
# _rmdir
# ======================================================================

class TestRmdir:

    def test_rmdir_empty_bucket(self):
        """The async _rmdir should successfully delete an empty bucket.

        NOTE: The sync ``rmdir()`` is a no-op in the base
        ``AbstractFileSystem``, so we test ``_rmdir`` directly via asyncio.
        """
        client = MockCosClient(buckets={TEST_BUCKET, "empty-bucket-1250000000"})
        test_fs = _make_fs(client)

        asyncio.get_event_loop().run_until_complete(
            test_fs._rmdir("empty-bucket-1250000000")
        )
        assert "empty-bucket-1250000000" not in client._buckets

    def test_rmdir_nonempty_bucket(self, fs):
        """Non-empty bucket should raise OSError from _rmdir."""
        with pytest.raises(OSError):
            asyncio.get_event_loop().run_until_complete(
                fs._rmdir(TEST_BUCKET)
            )

    def test_rmdir_subpath_noop(self, fs):
        """rmdir on a sub-path is a no-op (COS has no real directories)."""
        # Should not raise
        asyncio.get_event_loop().run_until_complete(
            fs._rmdir(f"{TEST_BUCKET}/data")
        )
