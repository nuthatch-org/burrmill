# The burrmill binary against datafusion-cli (#343)

Measured 2026-11-17 on the ThinkPad (Linux 6.12, x86_64, rustc 1.95.0), both built from source
under the same two profiles and stripped. datafusion-cli is 55.2.0, the DataFusion Burrmill pins.

| Profile | burrmill | datafusion-cli | burrmill gzip -9 | datafusion-cli gzip -9 |
|---|---:|---:|---:|---:|
| release, stripped | 121.7 MB | 144.0 MB | 38.6 MB | 47.8 MB |
| `profile.cli`: LTO, one codegen unit, stripped | **80.8 MB** | 95.4 MB | **28.5 MB** | 34.5 MB |

About 15% smaller under either profile, and not a win worth much: datafusion-cli carries the S3,
GCS and Azure stores, Avro and every function DataFusion has, where burrmill reads local Parquet
and does the three things it exists for. Both are larger than the DuckDB CLI.

How: `cargo build --locked --profile cli -p burrmill-cli` and, for datafusion-cli,
`CARGO_PROFILE_RELEASE_LTO=true CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
CARGO_PROFILE_RELEASE_STRIP=symbols cargo install --locked datafusion-cli --version 55.2.0`;
without the variables for the release row, burrmill's built with `CARGO_PROFILE_RELEASE_STRIP=symbols`
and datafusion-cli's stripped with `strip`.

Both rows are glibc binaries. The static one, measured 2026-11-19:

| Profile | burrmill, x86_64 musl, static | gzip -9 |
|---|---:|---:|
| `profile.cli` | **78.6 MB** | **28.3 MB** |

2.2 MB under the glibc build at the same profile. Built on the Mac with
`cargo zigbuild --locked --profile cli -p burrmill-cli --target x86_64-unknown-linux-musl`, since the
only C in the graph is zstd-sys (through arrow-ipc) and zig is a musl C compiler, then run on the
ThinkPad (Debian 13, Linux 6.12): `ldd` reports no dynamic executable, and a `--strict
--deterministic --receipt` answer verified, with a file removed it was refused. datafusion-cli was
not built for musl. CI builds the static binary with musl-gcc and runs it (`yatr cli-musl`,
`crates/burrmill-cli/musl-smoke.sh`), at the release profile, to keep it buildable.
