"""Fixtures for integration tests that connect to a real COS service.

Credentials are resolved from environment variables in this order:

    COS_SECRET_ID   / TENCENTCLOUD_SECRETID
    COS_SECRET_KEY  / TENCENTCLOUD_SECRETKEY
    COS_REGION      / TENCENTCLOUD_REGION      (default: ap-guangzhou)
    COS_TEST_BUCKET                             (default: mur-datalake-demo-1255655535)
"""

import os
import uuid

import pytest

from cosfs.core import COSFileSystem

DEFAULT_REGION = "ap-guangzhou"
DEFAULT_BUCKET = "mur-datalake-demo-1255655535"


def _get_env(name, fallback_name=None, default=None):
    """Read an env var with an optional fallback name and default value."""
    val = os.environ.get(name)
    if not val and fallback_name:
        val = os.environ.get(fallback_name)
    if not val:
        val = default
    if not val:
        pytest.skip(f"COS credentials not configured: {name} env var missing")
    return val


@pytest.fixture(scope="session")
def cos_config():
    """Read COS credentials from environment."""
    return {
        "secret_id": _get_env("COS_SECRET_ID", "TENCENTCLOUD_SECRETID"),
        "secret_key": _get_env("COS_SECRET_KEY", "TENCENTCLOUD_SECRETKEY"),
        "region": _get_env("COS_REGION", "TENCENTCLOUD_REGION", DEFAULT_REGION),
        "bucket": _get_env("COS_TEST_BUCKET", default=DEFAULT_BUCKET),
    }


@pytest.fixture(scope="session")
def cos_fs(cos_config):
    """A real ``COSFileSystem`` connected to COS."""
    return COSFileSystem(
        secret_id=cos_config["secret_id"],
        secret_key=cos_config["secret_key"],
        region=cos_config["region"],
    )


@pytest.fixture(scope="session")
def test_prefix(cos_config):
    """A unique prefix for this test session to isolate test data."""
    return f"tmp/cosfs-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def test_root(cos_config, test_prefix):
    """Full path: ``bucket/prefix``."""
    return f"{cos_config['bucket']}/{test_prefix}"


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_data(cos_fs, cos_config, test_prefix):
    """Clean up all test data after the session."""
    yield
    bucket = cos_config["bucket"]
    try:
        cos_fs.rm(f"{bucket}/{test_prefix}", recursive=True)
    except FileNotFoundError:
        pass
