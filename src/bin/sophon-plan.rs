//! Read-only measurement: how much would a Sophon install/update download for a game folder?
//!
//! ```text
//! sophon-plan <game_dir> <manifest_url> <chunk_base_url> [slot]
//! ```
//!
//! Nothing is written to `<game_dir>` (the manifest is fetched and files are only hashed).
//! Run it against a real, older installation and the new manifest to see how much of the
//! update the chunk-level reuse saves compared with downloading everything.

use berry_installer::sophon::{SophonInstaller, SophonJob};
use berry_installer::{Http, JobContext};
use std::path::PathBuf;

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: sophon-plan <game_dir> <manifest_url> <chunk_base_url> [slot]");
        std::process::exit(2);
    }
    let job = SophonJob {
        slot: args.get(3).cloned().unwrap_or_else(|| "game".into()),
        manifest_url: args[1].clone(),
        chunk_base: args[2].clone(),
    };
    let dir = PathBuf::from(&args[0]);
    let http = match Http::new("berry-installer/sophon-plan") {
        Ok(http) => http,
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
    };
    match SophonInstaller::new(http)
        .plan(&dir, &job, &JobContext::new())
        .await
    {
        Ok(o) => {
            println!(
                "files:            {} ({} trusted without hashing)",
                o.files, o.files_trusted
            );
            println!(
                "chunks:           {} ({} reusable on disk, {} missing)",
                o.chunks, o.chunks_reused, o.chunks_missing
            );
            println!("full download:    {:.1} MiB", mib(o.bytes_total));
            println!("to download:      {:.1} MiB", mib(o.bytes_missing));
            if o.bytes_total > 0 {
                println!(
                    "saved:            {:.1}%",
                    100.0 * (1.0 - o.bytes_missing as f64 / o.bytes_total as f64)
                );
            }
        }
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
    }
}
