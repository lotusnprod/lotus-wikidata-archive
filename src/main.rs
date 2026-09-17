use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use chrono::Utc;
use clap::Parser;
use lotus_wikidata_archive::{
    DEFAULT_QLEVER_ENDPOINT, QleverClient, deduplicate_by_inchi_key, dry_run_publish_archive,
    publish_archive, validate_smiles, write_archive,
};

/// Periodically archive and validate LOTUS SMILES sourced from Wikidata.
#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Directory in which completed archive files are retained.
    #[arg(long, default_value = "artifacts")]
    output_dir: PathBuf,

    /// QLever SPARQL endpoint serving the Wikidata dataset.
    #[arg(long, default_value = DEFAULT_QLEVER_ENDPOINT)]
    qlever_endpoint: String,

    /// Publish every completed archive to Zenodo using ZENODO_TOKEN.
    #[arg(long)]
    publish: bool,

    /// Validate the Zenodo metadata and upload plan without contacting Zenodo.
    #[arg(long, requires = "publish")]
    dry_run: bool,

    /// Creator name required by Zenodo metadata.
    #[arg(long, default_value = "The LOTUS Initiative")]
    creator: String,

    /// Repeat the pull after this many hours; omit for one archive run.
    #[arg(long)]
    period_hours: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.dry_run && !args.publish {
        bail!("--dry-run requires --publish");
    }

    let interval = args
        .period_hours
        .map(|hours| {
            if hours == 0 {
                bail!("--period-hours must be at least one");
            }
            Ok(Duration::from_secs(hours * 60 * 60))
        })
        .transpose()?;

    loop {
        run_once(&args).await?;
        let Some(interval) = interval else {
            return Ok(());
        };
        tokio::time::sleep(interval).await;
    }
}

async fn run_once(args: &Args) -> Result<()> {
    let fetched_at = Utc::now();
    let source_records = QleverClient::new(&args.qlever_endpoint)
        .fetch_lotus_smiles()
        .await?;
    let errors = validate_smiles(&source_records);
    let source_record_count = source_records.len();
    let records = deduplicate_by_inchi_key(source_records);
    let filename = format!(
        "lotus-wikidata-smiles-{}.tar.gz",
        fetched_at.format("%Y%m%dT%H%M%SZ")
    );
    let archive = write_archive(
        args.output_dir.join(filename),
        fetched_at,
        &args.qlever_endpoint,
        source_record_count,
        &records,
        &errors,
    )?;

    println!(
        "wrote {} InChIKey-deduplicated records and {} parse errors to {}",
        records.len(),
        errors.len(),
        archive.display()
    );
    if args.publish {
        let title = format!("LOTUS Wikidata SMILES — {}", fetched_at.date_naive());
        if args.dry_run {
            let plan =
                dry_run_publish_archive(&archive, &title, &args.creator, fetched_at.date_naive())?;
            println!("Zenodo dry run: would {plan}; no Zenodo request was made");
        } else {
            let record_id =
                publish_archive(&archive, &title, &args.creator, fetched_at.date_naive()).await?;
            println!("published Zenodo record {record_id}");
        }
    }
    Ok(())
}
