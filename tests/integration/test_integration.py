"""End-to-end integration tests against a real COS service.

These tests require valid COS credentials in environment variables.
They are skipped by default; run with ``pytest -m integration``.
"""

import pytest

pytestmark = pytest.mark.integration


# ======================================================================
# Basic round-trip
# ======================================================================

class TestCatPipeRoundtrip:

    def test_cat_pipe_roundtrip(self, cos_fs, test_root):
        path = f"{test_root}/roundtrip.txt"
        data = b"integration test data"
        cos_fs.pipe_file(path, data)
        assert cos_fs.cat_file(path) == data

    def test_cat_pipe_binary(self, cos_fs, test_root):
        path = f"{test_root}/binary.bin"
        data = bytes(range(256)) * 100
        cos_fs.pipe_file(path, data)
        assert cos_fs.cat_file(path) == data


# ======================================================================
# ls & find (basic)
# ======================================================================

class TestLsAndFind:

    def test_ls_and_find(self, cos_fs, test_root):
        # Create several files
        for name in ["ls_a.txt", "ls_b.txt", "ls_sub/c.txt"]:
            cos_fs.pipe_file(f"{test_root}/ls_test/{name}", b"data")

        # ls should show files and the sub-directory
        entries = cos_fs.ls(f"{test_root}/ls_test", detail=False)
        basenames = [e.split("/")[-1] for e in entries]
        assert "ls_a.txt" in basenames
        assert "ls_b.txt" in basenames
        assert "ls_sub" in basenames

        # find should return all files recursively
        found = cos_fs.find(f"{test_root}/ls_test")
        found_basenames = [f.split("/")[-1] for f in found]
        assert "ls_a.txt" in found_basenames
        assert "c.txt" in found_basenames


# ======================================================================
# rm recursive
# ======================================================================

class TestRmRecursive:

    def test_rm_recursive(self, cos_fs, test_root):
        prefix = f"{test_root}/rm_test"
        for i in range(5):
            cos_fs.pipe_file(f"{prefix}/file{i}.txt", b"delete me")

        assert len(cos_fs.find(prefix)) == 5
        cos_fs.rm(prefix, recursive=True)
        cos_fs.invalidate_cache()

        # After deletion, find should return empty (COS is strongly consistent
        # for DELETE, but ls may still see stale cache entries).
        remaining = cos_fs.find(prefix)
        assert len(remaining) == 0


# ======================================================================
# open write/read
# ======================================================================

class TestOpenWriteRead:

    def test_open_write_read(self, cos_fs, test_root):
        path = f"{test_root}/open_test.txt"
        with cos_fs.open(path, "wb") as f:
            f.write(b"written via open")
        with cos_fs.open(path, "rb") as f:
            assert f.read() == b"written via open"


# ======================================================================
# copy
# ======================================================================

class TestCopy:

    def test_copy(self, cos_fs, test_root):
        src = f"{test_root}/copy_src.txt"
        dst = f"{test_root}/copy_dst.txt"
        cos_fs.pipe_file(src, b"copy this")
        cos_fs.cp_file(src, dst)
        assert cos_fs.cat_file(dst) == b"copy this"


# ======================================================================
# Large directory (pagination)
# ======================================================================

class TestLargeDirectory:

    @pytest.mark.slow
    def test_large_directory(self, cos_fs, test_root):
        """Create >1000 files to verify pagination in ls/find."""
        prefix = f"{test_root}/large_dir"
        n = 1010
        for i in range(n):
            cos_fs.pipe_file(f"{prefix}/f{i:04d}.txt", b"x")

        found = cos_fs.find(prefix)
        assert len(found) == n


# ======================================================================
# NEW: find with prefix parameter
# ======================================================================

class TestFindWithPrefix:

    def test_find_prefix_filters(self, cos_fs, test_root):
        """prefix parameter should filter results server-side."""
        prefix = f"{test_root}/prefix_test"
        cos_fs.pipe_file(f"{prefix}/alpha_1.txt", b"a1")
        cos_fs.pipe_file(f"{prefix}/alpha_2.txt", b"a2")
        cos_fs.pipe_file(f"{prefix}/beta_1.txt", b"b1")
        cos_fs.pipe_file(f"{prefix}/beta_2.txt", b"b2")
        cos_fs.invalidate_cache()

        result = cos_fs.find(prefix, prefix="alpha")
        basenames = [r.split("/")[-1] for r in result]
        assert sorted(basenames) == ["alpha_1.txt", "alpha_2.txt"]

    def test_find_prefix_empty_returns_all(self, cos_fs, test_root):
        """Empty prefix should return all files (backward compat)."""
        prefix = f"{test_root}/prefix_test"
        cos_fs.invalidate_cache()

        result = cos_fs.find(prefix, prefix="")
        basenames = [r.split("/")[-1] for r in result]
        assert "alpha_1.txt" in basenames
        assert "beta_1.txt" in basenames
        assert len(result) >= 4


# ======================================================================
# NEW: find with withdirs (bisect dedup)
# ======================================================================

class TestFindWithDirs:

    def test_find_withdirs_synthetic_dirs(self, cos_fs, test_root):
        """withdirs=True should include synthetic directory entries."""
        prefix = f"{test_root}/withdirs_test"
        cos_fs.pipe_file(f"{prefix}/sub1/file1.txt", b"1")
        cos_fs.pipe_file(f"{prefix}/sub1/sub2/file2.txt", b"2")
        cos_fs.invalidate_cache()

        result = cos_fs.find(prefix, withdirs=True)
        # Should include synthetic directory entries
        dir_entries = [r for r in result if r.endswith("sub1") or r.endswith("sub2")]
        assert len(dir_entries) >= 2

        # No duplicates
        assert len(result) == len(set(result))

    def test_find_withdirs_detail(self, cos_fs, test_root):
        """withdirs=True with detail=True should return dict with dir entries."""
        prefix = f"{test_root}/withdirs_test"
        cos_fs.invalidate_cache()

        result = cos_fs.find(prefix, withdirs=True, detail=True)
        assert isinstance(result, dict)
        dir_entries = {k: v for k, v in result.items() if v["type"] == "directory"}
        assert len(dir_entries) > 0


# ======================================================================
# NEW: split_path trailing slash
# ======================================================================

class TestSplitPathTrailingSlash:

    def test_trailing_slash_preserved(self, cos_fs):
        """split_path should preserve trailing slash for directory placeholders."""
        bucket, key = cos_fs.split_path("cosn://some-bucket/some/dir/")
        assert key == "some/dir/"

    def test_no_trailing_slash(self, cos_fs):
        bucket, key = cos_fs.split_path("cosn://some-bucket/some/file.txt")
        assert key == "some/file.txt"


# ======================================================================
# NEW: multipart upload with auto part-size adjustment
# ======================================================================

class TestMultipartChunksize:

    def test_pipe_file_with_small_blocksize(self, cos_fs, test_root):
        """Force multipart path with tiny block_size and verify data integrity."""
        path = f"{test_root}/multipart_test.bin"
        block_size = 1 * 2 ** 20  # 1 MiB
        data = b"X" * (3 * 2 ** 20)  # 3 MiB > 2 * 1 MiB threshold
        cos_fs.pipe_file(path, data, block_size=block_size)

        result = cos_fs.cat_file(path)
        assert len(result) == len(data)
        assert result == data

    def test_pipe_file_range_read(self, cos_fs, test_root):
        """Verify range reads work on multipart-uploaded files."""
        path = f"{test_root}/multipart_range.bin"
        data = b"0123456789ABCDEF" * 1000
        cos_fs.pipe_file(path, data)

        result = cos_fs.cat_file(path, start=10, end=20)
        assert result == data[10:20]
