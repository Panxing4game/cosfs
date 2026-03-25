"""Tests for read operations: cat_file, get_file, info, exists, ls, find."""

import pytest

from tests.conftest import TEST_BUCKET


# ======================================================================
# _cat_file
# ======================================================================

class TestCatFile:

    def test_cat_file_full(self, fs):
        result = fs.cat_file(f"{TEST_BUCKET}/file1.txt")
        assert result == b"hello, world!"

    def test_cat_file_range(self, fs):
        # start=0, end=5  →  first 5 bytes
        result = fs.cat_file(f"{TEST_BUCKET}/file1.txt", start=0, end=5)
        assert result == b"hello"

    def test_cat_file_range_start_only(self, fs):
        result = fs.cat_file(f"{TEST_BUCKET}/file1.txt", start=7)
        assert result == b"world!"

    def test_cat_file_not_found(self, fs):
        with pytest.raises(FileNotFoundError):
            fs.cat_file(f"{TEST_BUCKET}/nonexistent.txt")


# ======================================================================
# _get_file
# ======================================================================

class TestGetFile:

    def test_get_file(self, fs, tmp_path):
        dest = str(tmp_path / "downloaded.txt")
        fs.get_file(f"{TEST_BUCKET}/file1.txt", dest)
        with open(dest, "rb") as f:
            assert f.read() == b"hello, world!"

    def test_get_file_to_directory(self, fs, tmp_path):
        dest = str(tmp_path) + "/"
        fs.get_file(f"{TEST_BUCKET}/file1.txt", dest)
        with open(str(tmp_path / "file1.txt"), "rb") as f:
            assert f.read() == b"hello, world!"


# ======================================================================
# _info
# ======================================================================

class TestInfo:

    def test_info_file(self, fs):
        info = fs.info(f"{TEST_BUCKET}/file1.txt")
        assert info["type"] == "file"
        assert info["size"] == 13
        assert info["name"] == f"{TEST_BUCKET}/file1.txt"

    def test_info_directory(self, fs):
        info = fs.info(f"{TEST_BUCKET}/data")
        assert info["type"] == "directory"
        assert info["name"] == f"{TEST_BUCKET}/data"

    def test_info_bucket_root(self, fs):
        info = fs.info(f"{TEST_BUCKET}")
        assert info["type"] == "directory"
        assert info["name"] == TEST_BUCKET

    def test_info_not_found(self, fs):
        with pytest.raises(FileNotFoundError):
            fs.info(f"{TEST_BUCKET}/no_such_file.bin")


# ======================================================================
# _exists
# ======================================================================

class TestExists:

    def test_exists_file(self, fs):
        assert fs.exists(f"{TEST_BUCKET}/file1.txt") is True

    def test_exists_directory(self, fs):
        assert fs.exists(f"{TEST_BUCKET}/data") is True

    def test_exists_not_found(self, fs):
        assert fs.exists(f"{TEST_BUCKET}/nope") is False

    def test_exists_bucket(self, fs):
        assert fs.exists(TEST_BUCKET) is True


# ======================================================================
# _ls
# ======================================================================

class TestLs:

    def test_ls_basic(self, fs):
        entries = fs.ls(TEST_BUCKET, detail=False)
        names = [e.split("/")[-1] for e in entries]
        assert "file1.txt" in names
        assert "data" in names

    def test_ls_detail(self, fs):
        entries = fs.ls(TEST_BUCKET, detail=True)
        assert isinstance(entries, list)
        assert all(isinstance(e, dict) for e in entries)
        file_entry = [e for e in entries if e["name"].endswith("file1.txt")]
        assert len(file_entry) == 1
        assert file_entry[0]["type"] == "file"

    def test_ls_subdir(self, fs):
        entries = fs.ls(f"{TEST_BUCKET}/data", detail=False)
        basenames = [e.split("/")[-1] for e in entries]
        assert "a.csv" in basenames
        assert "b.csv" in basenames
        assert "sub" in basenames

    def test_ls_pagination(self, fs):
        """Verify pagination works when MaxKeys is smaller than total objects.

        We seed a fresh filesystem with >2 objects and patch MaxKeys.
        """
        from tests.mock_cos import MockCosClient
        from tests.conftest import _make_fs

        objs = {
            (TEST_BUCKET, f"pg/item{i}.txt"): f"data{i}".encode()
            for i in range(5)
        }
        client = MockCosClient(buckets={TEST_BUCKET}, objects=objs)

        # Monkeypatch list_objects to use MaxKeys=2
        original_list = client.list_objects

        def small_page_list(Bucket, Prefix="", Delimiter="", Marker="", MaxKeys=1000, **kw):
            return original_list(Bucket=Bucket, Prefix=Prefix, Delimiter=Delimiter, Marker=Marker, MaxKeys=2, **kw)

        client.list_objects = small_page_list
        test_fs = _make_fs(client)

        entries = test_fs.ls(f"{TEST_BUCKET}/pg", detail=False)
        basenames = sorted(e.split("/")[-1] for e in entries)
        expected = sorted(f"item{i}.txt" for i in range(5))
        assert basenames == expected

    def test_ls_cache(self, fs):
        """Second call should hit dircache."""
        result1 = fs.ls(TEST_BUCKET, detail=False)
        result2 = fs.ls(TEST_BUCKET, detail=False)
        assert result1 == result2

    def test_ls_buckets(self, fs):
        """Listing root path should return bucket names."""
        entries = fs.ls("", detail=False)
        assert TEST_BUCKET in entries


# ======================================================================
# _find
# ======================================================================

class TestFind:

    def test_find_recursive(self, fs):
        result = fs.find(TEST_BUCKET)
        basenames = [r.split("/")[-1] for r in result]
        assert "file1.txt" in basenames
        assert "a.csv" in basenames
        assert "deep.json" in basenames

    def test_find_withdirs(self, fs):
        result = fs.find(TEST_BUCKET, withdirs=True)
        types = set()
        for r in result:
            info = fs.info(r)
            types.add(info["type"])
        assert "directory" in types
        assert "file" in types

    def test_find_skips_dir_placeholders(self, fs):
        """Zero-byte keys ending with '/' should be excluded from find results."""
        # Inject a directory placeholder
        fs.client._objects[(TEST_BUCKET, "data/sub/")] = b""

        result = fs.find(TEST_BUCKET)
        # The placeholder should NOT appear in find output
        assert f"{TEST_BUCKET}/data/sub/" not in result

    def test_find_maxdepth(self, fs):
        result = fs.find(TEST_BUCKET, maxdepth=2)
        # maxdepth=2 from bucket root:
        #   depth 1: file1.txt, data/ (dir)
        #   depth 2: data/a.csv, data/b.csv, data/sub/ (dir)
        # So data/sub/deep.json (depth 3) should NOT appear
        basenames = [r.split("/")[-1] for r in result]
        assert "file1.txt" in basenames
        assert "a.csv" in basenames
        assert "b.csv" in basenames
        assert "deep.json" not in basenames

    def test_find_prefix(self, fs):
        result = fs.find(f"{TEST_BUCKET}/data")
        basenames = [r.split("/")[-1] for r in result]
        assert "a.csv" in basenames
        assert "b.csv" in basenames
        assert "deep.json" in basenames
        assert "file1.txt" not in basenames

    def test_find_with_prefix_param(self):
        """The prefix parameter should filter results server-side."""
        from tests.mock_cos import MockCosClient
        from tests.conftest import _make_fs

        objs = {
            (TEST_BUCKET, "dir/alpha_1.txt"): b"a1",
            (TEST_BUCKET, "dir/alpha_2.txt"): b"a2",
            (TEST_BUCKET, "dir/beta_1.txt"): b"b1",
            (TEST_BUCKET, "dir/beta_2.txt"): b"b2",
        }
        client = MockCosClient(buckets={TEST_BUCKET}, objects=objs)
        test_fs = _make_fs(client)

        result = test_fs.find(f"{TEST_BUCKET}/dir", prefix="alpha")
        basenames = [r.split("/")[-1] for r in result]
        assert sorted(basenames) == ["alpha_1.txt", "alpha_2.txt"]

    def test_find_with_prefix_empty(self, fs):
        """Empty prefix should return all files (backward compat)."""
        result_default = fs.find(TEST_BUCKET)
        result_empty = fs.find(TEST_BUCKET, prefix="")
        assert sorted(result_default) == sorted(result_empty)

    def test_find_withdirs_no_duplicates(self):
        """withdirs=True should not produce duplicate directory entries."""
        from tests.mock_cos import MockCosClient
        from tests.conftest import _make_fs

        objs = {
            (TEST_BUCKET, "a/b/c1.txt"): b"1",
            (TEST_BUCKET, "a/b/c2.txt"): b"2",
            (TEST_BUCKET, "a/d/e.txt"): b"3",
        }
        client = MockCosClient(buckets={TEST_BUCKET}, objects=objs)
        test_fs = _make_fs(client)

        result = test_fs.find(TEST_BUCKET, withdirs=True)
        # No duplicates
        assert len(result) == len(set(result))
        # Should include synthetic dirs
        assert f"{TEST_BUCKET}/a" in result
        assert f"{TEST_BUCKET}/a/b" in result
        assert f"{TEST_BUCKET}/a/d" in result

    def test_find_withdirs_populates_dircache(self):
        """withdirs=True without prefix should populate dircache with dir entries."""
        from tests.mock_cos import MockCosClient
        from tests.conftest import _make_fs

        objs = {
            (TEST_BUCKET, "x/y/z.txt"): b"data",
        }
        client = MockCosClient(buckets={TEST_BUCKET}, objects=objs)
        test_fs = _make_fs(client)

        test_fs.find(TEST_BUCKET, withdirs=True)
        # dircache should have directory entries
        assert any(
            isinstance(v, dict) and v.get("type") == "directory"
            for v in test_fs.dircache.values()
        )

    def test_find_withdirs_with_prefix_no_dircache(self):
        """withdirs=True with prefix should NOT populate dircache."""
        from tests.mock_cos import MockCosClient
        from tests.conftest import _make_fs

        objs = {
            (TEST_BUCKET, "dir/alpha_1.txt"): b"a1",
            (TEST_BUCKET, "dir/beta_1.txt"): b"b1",
        }
        client = MockCosClient(buckets={TEST_BUCKET}, objects=objs)
        test_fs = _make_fs(client)

        test_fs.find(f"{TEST_BUCKET}/dir", withdirs=True, prefix="alpha")
        # dircache should not be populated when prefix is used
        assert len(test_fs.dircache) == 0
