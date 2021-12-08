import copy
import logging
import os
from os.path import expanduser
from configparser import ConfigParser
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
                self.client = CosS3Client(
                    CosConfig(Region=cli_config['buckets'][0]['region'], SecretId=cli_config['base']['secretid'],
                              SecretKey=cli_config['base']['secretkey'], Token=cli_config['base']['sessiontoken']))
        # coscmd config
        elif os.path.exists(conf_path + "/.cos.conf"):
            with open(conf_path + "/.cos.conf", 'r') as f:
                cp = ConfigParser()
                cp.read_file(f)
                if not cp.has_section('common'):
                    raise ValueError("[common] section couldn't be found, please check your coscmd config file.")
                secret_id = cp.get('common', 'secret_id', fallback=cp.get('common', 'access_id', fallback=None))
                self.client = CosS3Client(CosConfig(Region=cp.get('common', 'region'), SecretId=secret_id,
                                                    SecretKey=cp.get('common', 'secret_key'),
                                                    Token=cp.get('common', 'token', fallback=None)))
        else:
            raise FileNotFoundError("No config file found, see: https://cloud.tencent.com/document/product/436/63144")

    def split_path(self, path: str) -> Tuple[str, str]:
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, ""
        bucket_name, obj_name = path.split("/", 1)
        return bucket_name, obj_name

    async def _rm_file(self, path, **kwargs):
        pass

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        pass

    async def _get_file(self, rpath, lpath, **kwargs):
        pass

    async def _info(self, path, **kwargs):
        pass

    async def _ls(self, path, **kwargs):
        pass

    def ls(self, path, detail=True, **kwargs):
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
                "size": 0 if 'Prefix' in obj else obj['Size'],
                "Size": 0 if 'Prefix' in obj else obj['Size'],
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

    def cp_file(self, path1, path2, **kwargs):
        pass

    def created(self, path):
        pass

    def modified(self, path):
        pass

    def sign(self, path, expiration=100, **kwargs):
        pass


class COSFile(AbstractBufferedFile):

    def _fetch_range(self, start, end):
        pass


print(COSFileSystem().ls("cosn://mur-datalake-demo-1255655535/user_upload/weixin_drive/trend_drive/zuopin/zuopin/"))
print(COSFileSystem().ls("cosn://mur-datalake-demo-1255655535/user_upload/weixin_drive/trend_drive/zuopin/zuopin"))
print(COSFileSystem().ls("cosn://mur-datalake-demo-1255655535/"))
print(COSFileSystem().ls("cosn://mur-datalake-demo-1255655535"))
print(COSFileSystem().ls("cosn://"))
