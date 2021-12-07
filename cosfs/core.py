import logging
import os
from os.path import expanduser
from configparser import ConfigParser
from fsspec.asyn import AsyncFileSystem
from qcloud_cos import CosS3Client, CosConfig

logger = logging.getLogger("cosfs")


class COSFileSystem(AsyncFileSystem):
    def __init__(self, conf_path=expanduser("~")):
        super().__init__()
        if os.path.exists(conf_path + "/.cos.conf"):
            with open(conf_path + "/.cos.conf", 'r') as f:
                cp = ConfigParser()
                cp.read_file(f)
                if not cp.has_section('common'):
                    raise Exception("[common] section couldn't be found, please check your config file.")
                secret_id = cp.get('common', 'secret_id', fallback=cp.get('common', 'access_id', fallback=None))
                self.bucket = cp.get('common', 'bucket')
                self.client = CosS3Client(CosConfig(Region=cp.get('common', 'region'), SecretId=secret_id,
                                                    SecretKey=cp.get('common', 'secret_key'),
                                                    Token=cp.get('common', 'token', fallback=None)))
        elif os.path.exists(conf_path + "/.cos.yaml"):
            raise NotImplementedError("todo")
        else:
            raise FileNotFoundError("No config file found, see: https://cloud.tencent.com/document/product/436/63144")

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
        pass

    def cp_file(self, path1, path2, **kwargs):
        pass

    def created(self, path):
        pass

    def modified(self, path):
        pass

    def sign(self, path, expiration=100, **kwargs):
        pass


COSFileSystem()
