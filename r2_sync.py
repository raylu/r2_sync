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
	backup_immich(r2)
	backup_tree(r2, pathlib.Path('/mnt/data/music'), 'music/')

def backup_immich(r2: S3Client) -> None:
	backup_tree(r2, pathlib.Path('/mnt/data/immich/library/upload'), 'immich/')

	files = sorted(pathlib.Path('/mnt/data/immich/library/backups').iterdir())
	log('uploading DB backup', files[-1])
	with files[-1].open('rb') as f:
		r2.upload_fileobj(f, 'backup', 'immich/db-backup.sql.gz')

def backup_tree(r2: S3Client, base: pathlib.Path, r2_prefix: str) -> None:
	'''back up base to r2_prefix'''
	objs = frozenset(iter_objs(r2, r2_prefix))
	log('got', len(objs), 'objects from', r2_prefix, 'in R2')

	for rel in iter_files(base):
		key = str(pathlib.Path(r2_prefix) / rel)
		if key not in objs:
			log('uploading', rel)
			with (base / rel).open('rb') as f:
				r2.upload_fileobj(f, 'backup', key)

def log(*o: object) -> None:
	if VERBOSE:
		print(*o)

def iter_objs(r2: S3Client, prefix: str) -> typing.Iterator[str]:
	for page in r2.get_paginator('list_objects_v2').paginate(Bucket='backup', Prefix=prefix):
		for obj in page['Contents']: # type: ignore[reportTypedDictNotRequiredAccess]
			yield obj['Key'] # type: ignore[reportTypedDictNotRequiredAccess]

def iter_files(base: pathlib.Path) -> typing.Iterator[pathlib.Path]:
	for path, _, files in base.walk():
		for file in files:
			yield path.relative_to(base) / file

if __name__ == '__main__':
	main()

