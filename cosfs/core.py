import copy
import logging
import os
from configparser import ConfigParser
from os.path import expanduser
from typing import Optional, Tuple

import yaml
from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractBufferedFile
from qcloud_cos import CosS3Client, CosConfig

logger = logging.getLogger("cosfs")


class COSFileSystem(AsyncFileSystem):
    protocol = "cosn"

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

    def split_path(self, path: str) -> Tuple[str, str]:
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, ""
        bucket_name, obj_name = path.split("/", 1)
        return bucket_name, obj_name

    def parse_path(self, path: str) -> dict:
        bucket, key = self.split_path(path)
        return {"Bucket": bucket, "Key": key}

    async def _rm_file(self, path, **kwargs):
        bucket, key = self.split_path(path)
        self.client.delete_object(Bucket=bucket, Key=key)

    # async def _cat_file(self, path, start=None, end=None, **kwargs):
    #     pass

    async def _get_file(self, rpath, lpath, **kwargs):
        bucket, key = self.split_path(rpath)
        norm_lpath = lpath.rstrip("/")
        if lpath.endswith("/") or os.path.isdir(lpath):
            norm_lpath += "/" + key.split("/")[-1]
        self.client.download_file(Bucket=bucket, Key=key, DestFilePath=norm_lpath)

    async def _put_file(self, lpath, rpath):
        if rpath.endswith("/"):
            rpath += lpath.split("/")[-1]
        self.client.upload_file(**self.parse_path(rpath), LocalFilePath=lpath)

    async def _info(self, path, **kwargs):
        bucket, key = self.split_path(path)
        if not path.endswith("/") and self.client.object_exists(Bucket=bucket, Key=key):
            out = self.client.head_object(Bucket=bucket, Key=key)
            return {
                "ETag": out["ETag"],
                "Key": f"{bucket}/{key}",
                "name": f"{bucket}/{key}",
                "LastModified": out["Last-Modified"],
                "Size": int(out["Content-Length"]),
                "size": int(out["Content-Length"]),
                "type": "file",
                "StorageClass": "OBJECT"
            }

        elif len({'Contents', 'CommonPrefixes'} & self.client.list_objects(Bucket=bucket, Prefix=key.strip("/") + "/",
                                                                           Delimiter="/").keys()) > 0:
            return {
                "Key": path,
                "name": path,
                "type": "directory",
                "Size": 0,
                "size": 0,
                "StorageClass": "DIRECTORY"
            }

    async def _exists(self, path: str):
        return await self._info(path) is not None

    async def _ls(self, path, **kwargs):
        norm_path = path.strip("/")
        if norm_path in self.dircache:
            return copy.deepcopy(self.dircache[norm_path])
        bucket_name, prefix = self.split_path(path)
        if bucket_name:
            list_response = self.client.list_objects(Bucket=bucket_name, Prefix=prefix + "/" if prefix != '' else '',
                                                     Delimiter="/")
            info = [{**{
                "name": f"{bucket_name}/{obj.get('Key', obj.get('Prefix'))}",
                "Key": f"{bucket_name}/{obj.get('Key', obj.get('Prefix'))}",
                "type": "directory" if 'Prefix' in obj or obj['Key'].endswith("/") else "file",
                "size": 0 if 'Prefix' in obj else int(obj['Size']),
                "Size": 0 if 'Prefix' in obj else int(obj['Size']),
                "StorageClass": "DIRECTORY" if 'Prefix' in obj or obj['Key'].endswith("/") else "OBJECT"
            }, **({"LastModified": obj['LastModified']} if 'LastModified' in obj else {})}
                    for obj in list_response.get('Contents', []) + list_response.get('CommonPrefixes', [])]
        else:
            info = [{
                "name": bucket['Name'],
                "Key": bucket['Name'],
                "type": "directory",
                "size": 0,
                "Size": 0,
                "StorageClass": "BUCKET",
                "CreateTime": bucket['CreationDate'],
            } for bucket in self.client.list_buckets()['Buckets']['Bucket']]
        self.dircache[norm_path] = info
        return info

    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        return COSFile(self, path, mode, block_size, autocommit, cache_options=cache_options, **kwargs)

    async def _cp_file(self, path1, path2):
        self.client.copy(**self.parse_path(path2), CopySource={**self.parse_path(path1), **{"Region": self.region}})

    def created(self, path):
        pass

    def modified(self, path):
        pass

    def sign(self, path, expiration=100, **kwargs):
        pass

    def fetch_object(self, path: str, start: int, end: int) -> bytes:
        res = self.client.get_object(**{**self.parse_path(path), 'Range': f'bytes={start}-{end}'})
        return res['Body'].get_raw_stream().read()

    def append_object(self, path: str, value: bytes, location: Optional[int] = None):
        if location is None:
            location = self.info(path)['size']
        self.client.append_object(**{**self.parse_path(path)}, Position=location, Data=value)


class COSFile(AbstractBufferedFile):

    def _fetch_range(self, start, end):
        start = max(start, 0)
        end = min(self.size, end)
        if start >= end or start >= self.size:
            return b""
        return self.fs.fetch_object(self.path, start, end)

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload
        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        self.fs.append_object(self.path, self.buffer.getvalue(), self.offset)
        return True

    def _initiate_upload(self):
        """ Create remote file/upload """
        if "a" in self.mode:
            if self.fs.exists(self.path):
                self.offset = self.fs.info(self.path)["size"]
        elif "w" in self.mode:
            if self.fs.exists(self.path):
                self.fs.rm_file(self.path)
