# berry-installer

Install, update and repair engine of [Berry Launcher](https://github.com/A-Berry-Team/BerryLauncher):
resumable, verified downloads, game installs (HoYoverse *Sophon* and Kuro Games), archive
extraction, install state and Steam Linux Runtime management. Pure Rust, no external binaries
(`tar`, `unzip`, `7z`); the only git dependency is
[`berry-patcher`](https://github.com/A-Berry-Team/berry-patcher), pinned by commit.

Design notes and history: [`docs/INSTALLER.md`](docs/INSTALLER.md).

## Modules

| module | job |
| --- | --- |
| `download` | resumable (`Range`), retrying, SHA-256 / MD5 verified file download; `.part` files, cancel keeps them |
| `archive` | path-safe extraction of tar.{xz,gz,zst} and zip with progress and cancellation |
| `sophon` | HoYoverse Sophon engine: chunk manifests, reuse of what is already on disk, install / update / repair |
| `kuro` | Kuro Games (Wuthering Waves) engine: file index, parallel segmented downloads of big files, range patching, install / update / repair |
| `steamrt` | `SteamRuntimeManager`: install / ensure / check for updates / reinstall the Steam Linux Runtimes |
| `extras` | `ExtrasManager`: jadeite packages from Twintail manifests and [berry-unlock](https://github.com/A-Berry-Team/berry-unlock) from GitHub releases (`ensure_github`) |
| `game` / `state` | on-disk state of a game directory (`NotInstalled` / `Incomplete` / `Ready`) and marker files |
| `progress` | `CancelToken`, `Progress` / `Phase`, throttled `Reporter`, `JobContext` |
| `http` / `fsutil` / `hash` / `error` | shared `reqwest` client, free space, atomic writes, checksums, one error type |

## Kuro downloads

A Kuro release is a JSON index of `{dest, md5, size}` files. Install, update, resume and repair are
one operation: look at the disk, keep what matches, fetch the rest.

* Files of 32 MiB or more are split into 8 MiB segments fetched over up to 16 connections and
  written at their offset into a pre-sized `.part`. A ledger (`.segs`) lets an interrupted transfer
  resume segment by segment, even after a restart.
* Bodies are streamed to disk in 1 MiB writes, so memory use does not depend on file or region size.
* Every file is md5-checked before it is moved into place; a mismatch restarts that file once.
* Files with `chunkInfos` are patched in place by `Range`: only the differing regions are fetched.
* A server that ignores `Range` falls back to the sequential downloader automatically.
* A file that cannot be fetched makes the whole operation fail (`Incomplete`), never a finished
  install with holes.

## Usage

```rust
use berry_installer::{Http, JobContext};
use berry_installer::steamrt::{Edition, SteamRuntimeManager};

let http = Http::new("BerryLauncher/0.2.0")?;
let manager = SteamRuntimeManager::new(extras_dir.join("steamrt"), http);
let ctx = JobContext::new()
    .with_progress(|p| println!("{:?} {:.0}%", p.phase, p.fraction() * 100.0));

if !manager.state(Edition::Sniper).is_ready() {
    manager.ensure(Edition::Sniper, &ctx).await?;   // download, verify, extract, swap into place
}
let status = manager.check_update(Edition::Sniper, &ctx).await?;
if status.update_available {
    manager.update(Edition::Sniper, &ctx).await?;   // the old runtime stays usable until the swap
}
```

Kuro install:

```rust
use berry_installer::kuro::{KuroInstaller, KuroJob};

let job = KuroJob { index_url, base_url };
let outcome = KuroInstaller::new(http).install(&game_dir, &job, &ctx).await?;
```

## Command line helpers

```sh
cargo run --release --bin kuro-plan   -- <game_dir> <index_url> <base_url>
cargo run --release --bin sophon-plan -- <game_dir> <manifest_url> <chunk_base_url>
```

Both only read and hash: they report how much an install or update would really download.

## Tests

```sh
cargo test
```

The suite is offline and fast: hashing and verification, path-safe archive extraction (tar.gz and
zip, including hostile `..` entries), install state and markers, filesystem helpers, cancellation
and progress reporting. Nothing in it touches the network or the HoYoverse / Kuro servers.

## Using it from another crate

```toml
[dependencies]
berry-installer = { git = "https://github.com/A-Berry-Team/berry-installer", tag = "v0.2.0" }
```

Requires Rust 1.85 or newer (edition 2024). TLS is provided by `rustls`
(`reqwest` default), so neither OpenSSL nor `perl` is needed; a C compiler is required at build
time (`aws-lc-rs`, `liblzma` and `zstd` compile bundled C code).

## Origin and license

GPL-3.0-or-later. There is no fischl-rs code left in this crate: the engines are Berry Launcher
code. `LICENSE` is kept as an acknowledgement because the protocol knowledge (Sophon manifests,
Kuro indexes) and part of the design come from
[fischl-rs](https://github.com/TwintailTeam/fischl-rs) (TukanDev / TwintailTeam, GPL-3.0), and the
small generic toolbox ideas from
[anime-games-launcher](https://github.com/an-anime-team/anime-games-launcher).

## LDiff / HDiffPatch

HoYoverse LDiff patches are applied by [`berry-patcher`](https://github.com/A-Berry-Team/berry-patcher),
A-Berry-Team's hardened fork of TwintailTeam's `hdiffpatch-rs` (MIT, it reads HDIFF13, HDIFF19, SF20
and W26 patches). It is the crate's only git dependency, pinned by commit in `Cargo.toml`:

```toml
berry-patcher = { git = "https://github.com/A-Berry-Team/berry-patcher", rev = "<commit>",
                  default-features = false, features = ["codec-all", "hash-all"] }
```

Only the codecs and digests are enabled (KrDiff is not needed: Kuro updates go through HTTP Range
in the `kuro` module). To move to a newer patcher, change `rev`, run `cargo check -p berry-installer`
and, in the launcher repository, `build-aux/update-cargo-sources.sh` to refresh the offline Flatpak
sources.

What `berry-patcher` changes for Berry, compared with the old `hdiffpatch-rs` adapter:

* `ldiff::apply_hdiff` uses `HDiff::try_apply()` and reports the typed `berry_patcher::Error`
  (`InvalidPatch`, `SizeMismatch`, `ChecksumMismatch`, `UnsafePath`, ...) inside
  `InstallError::Archive`, instead of a bare `false` and a line on stderr.
* Single-file output is written to `<output>.berry-tmp` and renamed only after the patch and the
  digest it carries verified, so a failed patch never leaves a half-written file behind.
* A patch that names a checksum this build cannot compute is refused, never applied unverified.
* Header-driven allocations are bounded and decompressors cannot inflate past their declared size,
  so a corrupt patch bundle fails with an error instead of exhausting memory or panicking.
* `PatchOptions::auto()` lets it pipeline reading and decompression across threads.

The `ldiff` module downloads the selected `DiffManifest`, requests each version-specific patch
range from `<file_path>/<patch_id>` with HTTP Range, and persists the ranges under
`.berry-preload/ldiff`. Pre-download never modifies live game files. During a live update, Berry
reuses those cached ranges (or downloads any missing ones), applies them through `berry-patcher`,
verifies original and resulting MD5/size, and atomically replaces each game file. Manifest-declared
deletions are delayed until all selected patches have succeeded.
