# git-remote-s3

Use `s3://...` as a Git remote by storing versioned `git bundle` snapshots in S3.

## Features

- Works as a Git remote helper for `git clone`, `git fetch`, and `git push`
- Uses plain `s3://bucket/prefix` URLs
- Keeps timestamped snapshots under `snapshots/`
- Persists the latest snapshot pointer in `latest.json`
- Per-remote AWS profile, endpoint, and path-style configuration
- Compatible with S3-compatible services (OCI Object Storage, MinIO, etc.)
- Optimistic locking to detect concurrent pushes

## Installation

```bash
$ go install github.com/mattn/git-remote-s3/cmd/git-remote-s3@latest
```

## Usage

```bash
$ git clone s3://my-bucket/backups/my-repo
$ git remote add backup s3://my-bucket/backups/my-repo
$ git fetch backup
$ git push backup main
```

If the helper is installed as `git-remote-s3`, Git will invoke it automatically for `s3://...` URLs.

## Configuration

Per-remote settings via `git config`:

```bash
# Use a specific AWS profile
git config remote.backup.s3-profile work

# Use a custom S3-compatible endpoint
git config remote.backup.s3-endpoint http://localhost:9000

# Use path-style addressing (required by some S3-compatible services)
git config remote.backup.s3-path-style true
```

All settings are optional. Without them, the default AWS credential/config resolution from the AWS SDK for Go v2 is used.

### S3-compatible services

Example for OCI Object Storage:

```bash
git remote add origin s3://my-bucket/my-repo
git config remote.origin.s3-profile oci
git config remote.origin.s3-path-style true
```

If your `~/.aws/credentials` contains `endpoint_url` in the profile, it will be picked up automatically:

```ini
[oci]
aws_access_key_id = ...
aws_secret_access_key = ...
endpoint_url = https://xxx.compat.objectstorage.ap-tokyo-1.oraclecloud.com
```

Example for MinIO:

```bash
git remote add minio s3://my-bucket/my-repo
git config remote.minio.s3-endpoint http://localhost:9000
git config remote.minio.s3-path-style true
```

## Storage layout

The helper stores:

- `s3://.../snapshots/<timestamp>-<commit>.bundle` -- timestamped full bundle
- `s3://.../latest.json` -- pointer to the current snapshot

## Notes

- This implementation stores full snapshot bundles, not incremental packfile deltas.
- Push updates rebuild the remote snapshot from the previous snapshot plus the pushed refs.
- Deleting every ref on the remote is not supported.
- Concurrent pushes are detected via ETag-based optimistic locking.

## License

MIT
