from setuptools import setup, find_packages

setup(
    name='cosfs',
    version='0.0.1',
    packages=find_packages(),
    entry_points={
        'fsspec.specs': [
            'cosn=cosfs.COSFileSystem'
        ]
    }
)
