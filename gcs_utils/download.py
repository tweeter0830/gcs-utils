from google.cloud import storage
from google.cloud.storage import bucket

import dataclasses
import pathlib
import logging

@dataclasses.dataclass
class _File:
    gcs_dir: pathlib.Path
    local_dir: pathlib.Path
    relative_path: pathlib.Path

    @classmethod
    def make_from_gcs_path(
        cls, gcs_dir: pathlib.Path, local_dir: pathlib.Path, gcs_path: pathlib.Path) -> '_File':
        s1 = gcs_path.as_posix()
        s2 = local_dir.as_posix()
        if s1.startswith(s2):
            s3 = s1[len(s2):] 
        return cls(gcs_dir, local_dir, pathlib.Path(s3))

    @classmethod
    def make_from_local_path(
        cls, gcs_dir: pathlib.Path, local_dir: pathlib.Path, local_path: pathlib.Path) -> '_File':
        s1 = local_path.as_posix()
        s2 = local_dir.as_posix()
        if s1.startswith(s2):
            s3 = s1[len(s2):] 
        return cls(gcs_dir, local_dir, pathlib.Path(s3))

    @property
    def local_path(self) -> pathlib.Path:
        return self.local_dir.joinpath(self.relative_path)

    @property
    def remote_path(self) -> pathlib.Path:
        return self.gcs_dir.joinpath(self.relative_path)


def directory(
        gcs_bucket: bucket.Bucket,
              gcs_dir: pathlib.Path,
              local_dir: pathlib.Path,
              ignore_existing_files: bool,
) -> None:
    """Downloads a gcs_directory to a local directory."""
    if not local_dir.is_dir():
        return ValueError(f'Expected directory, but got {local_dir=} instead')

    gcs_client: storage.Client = gcs_bucket.client
    blobs = gcs_client.list_blobs(bucket=gcs_bucket, prefix=gcs_dir.as_posix())

    if ignore_existing_files:
        glob = local_dir.glob('**/*')
        local_paths = [x for x in glob if x.is_file()]
        local_files = set([_File(gcs_dir, local_dir, x) for x in local_paths])
        remote_files = set([_File(gcs_dir, local_dir, pathlib.Path(x.name)) for x in blobs])
        files_to_download = remote_files - local_files
    else:
        files_to_download = [_File(gcs_dir, local_dir, pathlib.Path(x.name)) for x in blobs]
    logging.info(f'Downloading {len(files_to_download)} out of {len(remote_files)} found in GCS')
    for file in files_to_download:
        with open(file.local_path, 'wb') as f:
            gcs_bucket.blob(file.remote_path.as_posix()).download_to_file(f)