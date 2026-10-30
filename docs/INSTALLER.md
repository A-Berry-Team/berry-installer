# berry-installer: design and roadmap

Goal: one engine, owned by this repository, that installs, updates, pre-downloads and repairs every
game the launcher offers (Genshin Impact, Honkai: Star Rail, Zenless Zone Zero, Honkai Impact 3rd,
Wuthering Waves) and manages the Steam Linux Runtimes, with a design where adding a game means
adding data and, at most, one small backend -- not touching the pipeline.

## Where the ideas come from

| | fischl-rs (Twintail) | anime-games-launcher (AGL) | berry-installer |
| --- | --- | --- | --- |
| Shape | one big library with a function per game/operation (`hoyo.rs` ~1800 lines) | tiny generic toolbox in Rust; game logic is Lua packages | generic toolbox **plus** typed backends |
| Downloads | parallel chunk downloader, retries, H2->H1 fallback | single-file `Range` resume, 416 handling | `Range` resume, retry in place, H1, verified, cancellable |
| Archives | extractor with many threads, 7z/zip/tar | shells out to `tar`/`unzip`/`7z` | pure Rust tar/zip (no host binaries: Flatpak friendly) |
| Integrity | md5 / crc64 per chunk | hash module, hashes in package lock files | `Checksum` + marker-based install state |
| Updates | Sophon manifests + hdiff patches | package-defined pipelines | per-backend (phase 3) |
| Cancel / progress | `AtomicBool` + 7-argument callback | interrupt flag + `ProgressReport` | `CancelToken` + one `Progress` struct |

What was taken from each: from fischl-rs the *knowledge* of the game servers (Sophon manifests, Kuro
indexes, Steam repo layout) and the staged-install idea; from AGL the *shape* -- small, generic,
testable pieces (downloader, archive, hash, progress) that backends are composed from, instead of
one monolith. (AGL has no Sophon or Steam runtime code of its own: its games are external Lua
packages, so there was nothing to copy there beyond the toolbox design.)

## Layers

```
 berry-downloads / berry-runners / berry-launcher        (callers, unchanged API)
            |
   managers:  steamrt (done) | game installer (phase 2-4)
            |
   backends:  sophon | kuro | yostar                     (phase 2-4, one module each)
            |
   toolbox:   download  archive  hash  state  progress  http  fsutil  error   (done)
```

Rules the toolbox enforces for every manager and backend:

1. **Marker last.** A directory is "ready" only when its marker file exists, and the marker is
   written after everything else. Anything else is `Incomplete` and is fixed by running the install
   again. This is how a game is classified as *ready to launch* vs *needs files*.
2. **Stage, then swap.** Runtimes are built in `<dir>.staging` and renamed into place; a failed
   update keeps the working version.
3. **Verify before trusting.** Downloads are renamed from `.part` only after the checksum matched.
4. **Everything is cancellable and reports `Progress`** through one `JobContext`; `Phase::Finalizing`
   is the only phase callers must not interrupt.
5. **One error type** (`InstallError`) with `is_cancelled()` / `is_transient()`.

## Phase 1 (this delivery): toolbox + Steam runtimes

* `SteamRuntimeManager` (`steamrt`): `state`, `latest_version`, `check_update`, `ensure`, `update`,
  `reinstall`. Same on-disk layout and `.berry-ready` marker as before, so existing installs are
  recognised (version read from `VERSIONS.txt`) and nothing is re-downloaded.
* `berry-runners::steamrt` keeps its public API and now calls the manager; `ensure_with(runtime,
  &JobContext)` is new and gives the UI progress + cancel for the runtime download.
* All fischl-rs code moved to `berry_installer::legacy` behind the `legacy` feature.

## Roadmap

### Phase 2 -- extras and game state (done)
* `extras::ExtrasManager` replaces fischl-rs' `Extras` (jadeite from Twintail manifests; BerryUnlock from the latest GitHub release of `A-Berry-Team/berry-unlock` via `ensure_github`). Same Twintail
  manifest data, new behaviour: a marker per package (`.berry-extra-<id>`) so the startup prefetch
  costs one small request instead of a download; an installed package keeps working when the
  manifest server is down; archives are unpacked with `archive` and merged into the (shared)
  destination without touching other files; manifest paths cannot escape the destination.
  Checksums (`zip_sha256`) are verified but only *warn* by default (`enforce_checksums(true)` makes
  them fatal): the field could not be checked against the live manifests from the dev sandbox, and
  a wrong guess must not break the FPS unlocker. Flip it once confirmed.
* `game::{GameState, inspect, ...}` is the single definition of a game directory's state:
  `NotInstalled | Incomplete(Unfinished) | Ready{version}`, plus the install marker
  (`.berry-installing`), staging directories and version file helpers that used to live in
  `berry-downloads::metadata`. `berry-downloads` keeps its public API (`is_fully_downloaded`,
  `has_incomplete_download`, `installed_version`) as thin wrappers and gains `game_state()`.
  Behaviour is unchanged: ready = executable present and no pending work; resumable = marker or
  staging directory present.
* Update / pre-download availability stays in `berry-downloads::sources` (it needs the network); it
  will move next to each backend in phases 3-4 and be reported alongside `GameState`.
* `legacy::download::Extras` and the Twintail manifest structs were deleted.

### Phase 3 -- Sophon (HoYoverse: Genshin, Star Rail, ZZZ, Honkai 3rd) -- implemented
* `sophon` module: manifest (zstd protobuf, hand-written prost structs, no build.rs) -> per-file
  chunk plan -> bounded parallel download (24 chunks, 4 files in flight) -> chunks written in
  place -> MD5 per chunk and per file. Install, update, resume and repair are one algorithm:
  hash every file region against the manifest, keep what matches, fetch only what differs.
* `preload` stores chunks in `.berry-preload/<slot>/` without touching the game; the next
  install/update consumes them. `plan` is a read-only report (`sophon-plan` binary).
* Shortcuts (never for repair): the stored manifest `.berry-sophon/<slot>.manifest.zst` and the
  download journal let unchanged files skip hashing. Only files the previous manifest listed are
  ever pruned.
* Failures are errors (`ChunksFailed`), never "completed"; no HEAD per chunk; one shared `Http`;
  cancellation aborts every task. This is what fixes the pause/resume slowdown.
* `berry-downloads` uses it for the `Sophon` backend. (a `BERRY_LEGACY_*` switch existed until phase 5.)
* **hdiffpatch-rs was removed in phase 5** after measuring. Chunk reuse replaces hdiff's main purpose (not
  re-downloading what is already there) but is weaker when data is inserted in a file (later
  chunks shift). Measure on real servers with
  `sophon-plan <game_dir> <manifest_url> <chunk_base_url>` against an older install; remove the
  dependency only if the saving is acceptable.

### Phase 4 -- Kuro (Wuthering Waves) -- implemented
* `kuro` module: `index.json` (`resource`: dest/md5/size, optional `chunkInfos`) -> per-file check
  -> download. Same rule as Sophon: look at the disk, keep what matches, fetch the rest.
  Install/update/resume/repair are one operation; repair trusts nothing.
* Whole files go to `downloading/<dest>` (resumable `.part`, md5) and are moved into place when
  verified. Files whose index has `chunkInfos` that tile the file are updated **in place by HTTP
  Range**: each region is hashed, only the differing ones are fetched (md5-checked). The index does
  not say whether `end` is inclusive, so the data decides (whichever convention tiles the file
  exactly); if neither does, a region hash is wrong, or the server ignores `Range`, that file falls
  back to a plain whole-file download.
* **Parallel, streamed transfers (`kuro/segments.rs`).** Files of 32 MiB or more (the multi-GB
  paks) are cut into 8 MiB segments fetched over up to 16 connections and written at their offset
  into a pre-sized `.part`; `<part>.segs` records finished segments, so a pause resumes segment by
  segment (a segment that broke midway resumes from the byte it reached). The whole file is md5
  checked before it is moved into place; a mismatch restarts that file once from scratch. A server
  that ignores `Range`, or a leftover sequential `.part`, sends the file to the plain downloader.
  Range patches stream to disk too (1 MiB writes, incremental md5), so memory no longer depends on
  region size, and progress moves continuously instead of per region. Small files are handled 12 at
  a time over a pooled HTTP/1.1 client (`TCP_NODELAY`, keep-alive, 32 idle sockets per host).
* **Whole-file verification has its own progress.** Once the last bytes are downloaded the big
  files are still md5-checked, at most 3 at a time (a dozen parallel passes make a hard drive seek
  and take far longer). While nothing is downloading, the ticker reports `Verifying` with
  bytes hashed / bytes to hash and the hashing rate, instead of a bar frozen at 100 % and 0 B/s.
  A file task that panics is recorded as a failure, so a run can never finish with a file missing.
* `.berry-kuro/index.json` = what the last successful run installed (trust shortcut and the only
  source of prune candidates); `downloading/journal-kuro` remembers verified files across a pause.
* A failed file is an error (`Incomplete`), never a finished install.
* `kuro-plan <game_dir> <index_url> <base_url>` measures the real saving without writing anything.
* Yostar: the launcher never used it (Honkai 3rd goes through Sophon), so no module was written.
  The old yostar code and `zesven` were deleted in phase 5.

### Phase 5 -- remove `legacy` -- done
Measured on a real Zenless Zone Zero update (`sophon-plan`): 59307 chunks in the manifest, 49992
already on disk, 9315 to fetch: **9.0 GiB instead of 57.4 GiB (84.2% saved)**, where the old path
re-downloaded every chunk. That settled the hdiffpatch question.
Deleted: `src/legacy/`, `proto/`, `build.rs`, the `legacy` feature, the `berry-installer` binary and
the dependencies `zesven`, `hdiffpatch-rs`, `reqwest-middleware/retry`, `sysinfo`, `crossbeam-deque`,
`crc64fast`, `futures-util`, `prost-build`, `protoc-bin-vendored`. The `BERRY_LEGACY_*` switches are
gone. `berry-installer` has no git dependencies.

## Adding a game (target design for phases 2-4)

A game is data plus a backend id:

```rust
GameSpec { id: "genshin-impact", backend: Backend::Sophon, ids: SophonIds { game_id, biz } }
```

A backend implements the small set of operations the launcher exposes
(`resolve -> Plan`, `execute(Plan, JobContext)`, `verify`), reusing `download`, `archive`, `hash`
and `state`. A new HoYoverse title is therefore one catalogue entry; a game on a new CDN is one new
backend module.
