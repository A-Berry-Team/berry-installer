//! Read-only measurement for Wuthering Waves: how much would an install/update download?
//!
//! ```text
//! kuro-plan <game_dir> <index_url> <base_url>
//! ```
//!
//! Files are only hashed, nothing is written to `<game_dir>`.

use berry_installer::kuro::{KuroInstaller, KuroJob};
use berry_installer::{Http, JobContext};
use std::path::PathBuf;

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: kuro-plan <game_dir> <index_url> <base_url>");
        std::process::exit(2);
    }
    let job = KuroJob {
        index_url: args[1].clone(),
        base_url: args[2].clone(),
    };
    let result = match Http::new("berry-installer/kuro-plan") {
        Ok(http) => {
            KuroInstaller::new(http)
                .plan(&PathBuf::from(&args[0]), &job, &JobContext::new())
                .await
        }
        Err(error) => Err(error),
    };
    match result {
        Ok(o) => {
            println!(
                "files:            {} ({} trusted without hashing)",
                o.files, o.files_trusted
            );
            println!("ranges reusable:  {}", o.regions_reused);
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
