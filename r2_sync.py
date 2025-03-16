# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3",
# ]
# ///

from __future__ import annotations

import pathlib
import sys
import tomllib
import typing

import boto3

if typing.TYPE_CHECKING:
	from mypy_boto3_s3 import S3Client

VERBOSE = len(sys.argv) == 2 and sys.argv[1] == '-v'

def main() -> None:
	current_dir = pathlib.Path(__file__).parent
	with (current_dir / 'r2_sync.toml').open('rb') as f:
		config = tomllib.load(f)

	r2 = boto3.client('s3', endpoint_url=config['endpoint_url'],
			aws_access_key_id=config['aws_access_key_id'], aws_secret_access_key=config['aws_secret_access_key'])

	objs = frozenset(iter_objs(r2))
	log('got', len(objs), 'objects from R2')

	base = pathlib.Path('/mnt/data/immich/library/upload')
	for rel in iter_files(base):
		key = str(pathlib.Path('immich') / rel)
		if key not in objs:
			log('uploading', rel)
			with (base / rel).open('rb') as f:
				r2.upload_fileobj(f, 'backup', key)

	files = sorted(pathlib.Path('/mnt/data/immich/library/backups').iterdir())
	log('uploading DB backup', files[-1])
	with files[-1].open('rb') as f:
		r2.upload_fileobj(f, 'backup', 'immich/db-backup.sql.gz')

def log(*o: object) -> None:
	if VERBOSE:
		print(*o)

def iter_objs(r2: S3Client) -> typing.Iterator[str]:
	for page in r2.get_paginator('list_objects_v2').paginate(Bucket='backup', Prefix='immich/'):
		for obj in page['Contents']: # type: ignore[reportTypedDictNotRequiredAccess]
			yield obj['Key'] # type: ignore[reportTypedDictNotRequiredAccess]

def iter_files(base: pathlib.Path) -> typing.Iterator[pathlib.Path]:
	for path, _, files in base.walk():
		for file in files:
			yield path.relative_to(base) / file

if __name__ == '__main__':
	main()

