"""Shared fixtures for cosfs unit tests."""

import os

import pytest
import fsspec.asyn
from fsspec.asyn import mirror_sync_methods

from cosfs.core import COSFileSystem
from tests.mock_cos import MockCosClient

TEST_BUCKET = "test-bucket-1250000000"


@pytest.fixture
def mock_client():
    """An empty ``MockCosClient`` with a single bucket."""
    return MockCosClient(buckets={TEST_BUCKET})


@pytest.fixture
def mock_client_with_data():
    """A ``MockCosClient`` pre-loaded with a realistic directory tree.

    Layout::

        test-bucket-1250000000/
            file1.txt          (13 bytes)
            data/
                a.csv          (20 bytes)
                b.csv          (20 bytes)
                sub/
                    deep.json  (28 bytes)
    """
    return MockCosClient(
        buckets={TEST_BUCKET},
        objects={
            (TEST_BUCKET, "file1.txt"): b"hello, world!",
            (TEST_BUCKET, "data/a.csv"): b"col1,col2\n1,2\n3,4\n",
            (TEST_BUCKET, "data/b.csv"): b"col1,col2\n5,6\n7,8\n",
            (TEST_BUCKET, "data/sub/deep.json"): b'{"key": "value", "n": 42}',
        },
    )


def _make_fs(client):
    """Build a ``COSFileSystem`` with an injected mock client, bypassing __init__."""
    fs = COSFileSystem.__new__(COSFileSystem)

    # fsspec internal bookkeeping (AbstractFileSystem + AsyncFileSystem)
    fs._intrans = False
    fs._transaction = None
    fs._invalidated_caches_in_transaction = []
    fs.dircache = {}
    fs.blocksize = 5 * 2 ** 20  # 5 MiB
    fs.region = "ap-guangzhou"
    fs.retries = 3
    fs._pid = os.getpid()
    fs.asynchronous = False
    fs._loop = fsspec.asyn.get_loop()
    fs.batch_size = None

    # Inject mock client
    fs.client = client

    # The metaclass __call__ normally calls mirror_sync_methods to create
    # sync wrappers (ls, cat_file, …) from the async _ls, _cat_file, …
    # Since we bypass __init__ via __new__, we must call it explicitly.
    mirror_sync_methods(fs)

    return fs


@pytest.fixture
def fs(mock_client_with_data):
    """A ``COSFileSystem`` backed by a pre-loaded ``MockCosClient``."""
    return _make_fs(mock_client_with_data)


@pytest.fixture
def empty_fs(mock_client):
    """A ``COSFileSystem`` backed by an empty ``MockCosClient``."""
    return _make_fs(mock_client)
