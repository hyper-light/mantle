# mantle comparators

Comparison-only workspace on adacachy (Linux x86_64). Nothing here is linked into or shipped with mantle.
Every comparator is pinned by exact revision. Sources (shallow clones at the pinned tag) are in `src/<name>`;
the line references below are into those trees. Built/downloaded binaries are in `<name>/bin`.

Every S3 start script takes `<data-dir> <port>`, runs in the foreground on 127.0.0.1, access key
`AKIAIOSFODNN7EXAMPLE`, secret `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`, region `us-east-1`, single node,
and prints `... ready on 127.0.0.1:<port>` on stderr. `scripts/s3check.sh <name> <data-dir> <port>` is the smoke
check (create bucket, put 1 MiB, get, `cmp`, stop).

## Table

| Comparator | Version | Revision | License | Binary from (checksum) | Start |
|---|---|---|---|---|---|
| Garage | v2.5.0 | `586726edb0170bb46f604e84fba09e5659610649` | AGPL-3.0 | https://garagehq.deuxfleurs.fr/_releases/v2.5.0/x86_64-unknown-linux-musl/garage — no checksum is published; sha256 as downloaded `2ff667286217cd73ebcb1c03af993822365000441dfe31a80cb64c5189a9dea8` | `garage/start.sh DIR PORT` |
| SeaweedFS | 4.52 | `984eac6a790be89e2a11de175a7413e1e3e32dd3` (also printed by `weed version`) | Apache-2.0 | https://github.com/seaweedfs/seaweedfs/releases/download/4.52/linux_amd64.tar.gz — md5 `dea0ea56ee3439dc2e3fb485c235e58b` matches the published `.md5`; sha256 `8b538c22d47d0c3fb6792adaa6b0c735a967401270a53427d5a476dcaca9b7ae` matches GitHub's asset digest | `seaweedfs/start.sh DIR PORT` |
| RustFS | 1.0.4 | `46337942d55f3a8e6a97b147ca98d9ea8115555d` | Apache-2.0 | https://github.com/rustfs/rustfs/releases/download/1.0.4/rustfs-linux-x86_64-musl-v1.0.4.zip — sha256 `4924ad4b699b9f59943051efedae2d1eb6d4cac503a1b0384c4b25f7c2c31b5c` matches the release `SHA256SUMS` (kept in `dl/`) | `rustfs/start.sh DIR PORT` |
| MinIO | RELEASE.2025-10-15T17-29-55Z | `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a` | AGPL-3.0 | Built from source: dl.min.io answers **410 Gone** for every community binary. `scripts/build-minio.sh` (CGO off, `-tags kqueue -trimpath`, release ldflags from `buildscripts/gen-ldflags.go`), Go 1.27.2 with GOTOOLCHAIN=local (upstream pins go1.24.8). sha256 of `minio/bin/minio` `b50bfdaaa3793539b58ecc447ac364d7723a71dd9462ecd2d41f22f7ad1e74ad` (no upstream checksum left to compare against) | `minio/start.sh DIR PORT` |
| msquic secnetperf | v2.6.3 | `16ca4d8d785d34fb22285e7c652e477ac16475a5`; quictls submodule `ff36838bb69801cad56823159a036977bcbe5c75` (OpenSSL 3.1.7+quic) | MIT | Built from source: `scripts/build-msquic.sh` (cmake 4.4.4 + ninja from a scratch venv, `-DQUIC_TLS_LIB=quictls -DQUIC_BUILD_PERF=ON`, Release). sha256 `secnetperf` `262e8d0260708423d9150eef37542c546bc2741b0fa796e49ee46990a0903ddb`, `libmsquic.so.2.6.3` `e55ea3ff263550dc641ca25332f78024421ffa911f5bf2f63facdf5124c4837c` | server `msquic/secnetperf.sh -bind:127.0.0.1:4433`; client `msquic/secnetperf.sh -target:127.0.0.1 -port:4433 -exec:maxtput -upload:3s -ptput:1` |
| quinn perf | quinn-proto 0.11.18 (tag `quinn-proto-0.11.18`, same commit as `quinn-0.11.12`) | `eaec0db4bcb698f76df736d89938743c88a2ad5f` | MIT OR Apache-2.0 | Built from source: `scripts/build-quinn.sh` (`cargo build --locked --release -p perf --bin quinn-perf`, cargo 1.98.1). sha256 `a699009c7443f2313b5e2a2ecd2c07ea438898e99367b50ade876a902790ee2b` | server `quinn/bin/quinn-perf server --listen 127.0.0.1:4433`; client `quinn/bin/quinn-perf client --ip 127.0.0.1 --duration 3 --download-size 100M --upload-size 0 localhost:4433` |

All builds ran under `systemd-run --user --scope -p MemoryMax=16G -p MemorySwapMax=0` with `-j 8` / `-p=8`;
build intermediates (Go caches, cargo target + CARGO_HOME, cmake build dir, the venv) were deleted afterwards.

## Durability settings and what they do

### Garage — `data_fsync = true`, `metadata_fsync = true` (db_engine `lmdb`)
- `src/garage/src/block/manager.rs:800-802`: after writing a block to its temp file, `if mgr.data_fsync { f.sync_all() }`;
  then rename, and `:814-826` fsyncs the containing directory so the rename is durable.
- `src/garage/src/db/lmdb_adapter.rs:42-45`: LMDB opens with `NO_META_SYNC`, and adds `NO_SYNC` only when
  `metadata_fsync` is false; with it true every LMDB commit is synced (meta page sync deferred to the next commit).
- `src/garage/src/util/config.rs:19-24` documents both as "fsync after all metadata transactions / data block writes".
- start.sh ports: S3 `PORT`, RPC `PORT+1`, admin `PORT+2`. On first start it applies a one-node layout and imports
  the fixed key (`garage key import --yes`) with `--create-bucket`.

### SeaweedFS — filer storage rule `fs.configure -locationPrefix=/ -fsync=true -apply`
SeaweedFS has no global `-fsync` server flag in 4.52 (`weed benchmark -fsync` is a client flag); fsync is a
per-path filer.conf storage rule that start.sh applies via `weed shell` as soon as the filer answers.
- `src/seaweedfs/weed/operation/upload_content.go:221-222`: when the filer's AssignVolume answer carries
  `Fsync` (the matching rule's `fsync`), the S3 gateway's chunk upload URL gets `?fsync=true`.
- `src/seaweedfs/weed/topology/store_replicate.go:59-63,117`: the volume server reads `fsync=true` and calls
  `WriteVolumeNeedle(..., fsync)`.
- `src/seaweedfs/weed/storage/volume_write.go:270-298` (writeNeedle2): fsync writes go through the async batch
  worker that fsyncs the `.dat` once per batch (`:604 backend.Sync()`, group commit tuned by
  `-volume.fsyncBatchWait` 500µs / `fsyncMinBatchBytes` 1 MiB / `fsyncMinBatchRequests` 128) then syncs the `.idx`
  (`:655 syncIndex`); "Both paths only return once the .dat is on disk."
- `src/seaweedfs/weed/filer/filer.go:479-486` + `filer/leveldb2/leveldb2_store.go:136-141`: the same rule marks the
  entry write sync, so the filer's leveldb2 metadata store writes with `WriteOptions{Sync: true}`.
- start.sh ports: S3 `PORT`, master `+1`, volume `+2`, filer `+3`, gRPC `+11..+14`; Iceberg/Lance listeners and
  master telemetry off. Wait for the `ready on` line before writing: until the rule is applied, writes are not fsynced.
- Not stock: `-volume.minFreeSpace=4GiB` (env `SEAWEED_MIN_FREE`), because the default 1% threshold marked every
  volume read-only on this 99%-full disk. `-volume.preStopSeconds=0` for quicker stops.

### RustFS — `RUSTFS_DURABILITY_MODE=strict` (the default; set explicitly), `RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit`
- `src/rustfs/crates/ecstore/src/disk/local.rs:1250-1261` and `:1470-1530`: `strict` = "every commit point is
  fsynced: shard/part contents, xl.meta contents, rollback backups, and the directory entries of commit renames".
  (`relaxed` drops metadata/dir syncs, "aligned with MinIO's default"; `none` drops data syncs;
  `RUSTFS_DRIVE_SYNC_ENABLE=false` = legacy no-fsync-anywhere.)
- `src/rustfs/crates/ecstore/src/disk/local/commit.rs:570` (tmp xl.meta written synced when
  `syncs_commit_metadata`) and `:611-626` (shard files fdatasynced via `sync_dir_files_with_limiter` when
  `syncs_data_shards`), both joined before any commit rename in `rename_data`, i.e. before the PUT returns.
- `src/rustfs/crates/ecstore/src/bucket/durability.rs:15-80`: buckets may override the tier (`durability.json`);
  `inherit` keeps new buckets on the process-wide `strict`.
- start.sh: console off, logs to `DATA/logs`, one drive at `DATA/disk`.

### MinIO — default xl-single backend, plus `MINIO_FS_OSYNC=on`
- Default, no option: shard files go through `writeAllDirect` (O_DIRECT where possible) and end with
  `Fdatasync(w)` (`src/minio/cmd/xl-storage.go:2195`); xl.meta is written through `openFileSync` with
  `writeMode = O_DSYNC` (`xl-storage.go:1976`, `xl-storage_noatime_supported.go:33`), or O_DIRECT + fdatasync when
  larger than 4 KiB (`xl-storage.go:2252-2257`). The commit `renameAll` of xl.meta / data dir is **not** followed by
  a directory fsync.
- `MINIO_FS_OSYNC` (`internal/config/constants.go:49`, parsed at `cmd/common-main.go:745` into `globalFSOSync`,
  stored as `xlStorage.globalSync` at `xl-storage.go:228`): when on, `RenameData` defers `globalSync()`
  (`xl-storage.go:2576-2577`), which is `syscall.Sync()` (`os_unix.go:355-357`) — a whole-machine `sync(2)` before
  the PUT returns. That is the only knob that makes the rename durable; there is no MINIO_*_ODSYNC-style option.
  start.sh turns it on (override with `MINIO_FS_OSYNC=off`). Caution: each PUT flushes every filesystem on the host,
  including production's dirty pages; on adacachy prefer `off` (per-file fdatasync/O_DSYNC only) or another box.
- start.sh: console on `PORT+1`, browser and update checks off, one drive at `DATA/disk`.

## Smoke checks run (2026-11-04)
- Garage, SeaweedFS, RustFS: start.sh on a scratch dir under this workspace, create bucket, put 1 MiB, get, `cmp`
  identical, stop, scratch deleted.
- MinIO: on /home every PUT fails with `XMinioStorageFull`: `diskFillFraction = 0.99` is hard-coded
  (`cmd/globals.go:117`, checked in `object-api-utils.go:1295-1304`) and /home is >99% full. The round trip passed
  with the data dir on /dev/shm (tmpfs; a functional check only, fsync is meaningless there). Benchmarks need a
  filesystem under 99% used.
- secnetperf: loopback 3 s upload 18.4 Gbps, 3 s download 20.2 Gbps (`-exec:maxtput`).
- quinn-perf: loopback 3 s, 100 MiB bi-requests, avg download 12.5 Gb/s.

## Could not be done / caveats
- MinIO: no official binary (410 Gone; repo archived). Built from the tag with a newer Go than upstream used.
  Full durability (`MINIO_FS_OSYNC=on`) is a global sync(2), not a per-file fsync; see above. Its PUT path can't run
  on this disk as is (99% fill guard, no env override).
- Garage: the release site publishes no checksum or signature; only the downloaded hash is recorded.
- SeaweedFS: durability is a filer rule applied after start (not a flag); needs a non-default minFreeSpace here.
- msquic: the binary's RUNPATH points at the deleted build dir; `msquic/secnetperf.sh` sets LD_LIBRARY_PATH to
  `msquic/bin`. The unused `openssl` and `xdp-for-windows` submodule checkouts were deinitialised to save space
  (`git submodule update --init` restores them).
- No syscall-level confirmation (no strace/bpftrace here, perf tracepoints not readable); durability claims rest on
  the source lines above.
