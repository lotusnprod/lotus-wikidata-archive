use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use chrono::Utc;
use clap::Parser;
use lotus_wikidata_archive::{
    DEFAULT_QLEVER_ENDPOINT, QleverClient, deduplicate_by_inchi_key, dry_run_publish_archive,
    publish_archive, validate_missing_inchi_key_smiles, validate_smiles, write_archive,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_ansi(false)
        .init();
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
    let client = QleverClient::new(&args.qlever_endpoint);
    let source_records = client.fetch_lotus_smiles().await?;
    let missing_inchi_key_records = client.fetch_missing_inchi_key_smiles().await?;
    let mut errors = validate_smiles(&source_records);
    errors.extend(validate_missing_inchi_key_smiles(
        &missing_inchi_key_records,
    ));
    let source_record_count = source_records.len();
    let records = deduplicate_by_inchi_key(source_records);
    let filename = format!(
        "lotus-wikidata-smiles-{}.tar.gz",
        fetched_at.format("%Y%m%dT%H%M%SZ")
    );
    let artifacts = write_archive(
        args.output_dir.join(filename),
        fetched_at,
        &args.qlever_endpoint,
        source_record_count,
        &missing_inchi_key_records,
        &records,
        &errors,
    )?;

    info!(
        records = records.len(),
        missing_inchi_key_records = missing_inchi_key_records.len(),
        parse_errors = errors.len(),
        archive = %artifacts.archive.display(),
        "archive written"
    );
    info!(manifest = %artifacts.manifest.display(), "standalone manifest written");
    info!(checksums = %artifacts.checksums.display(), "checksums written");
    if args.publish {
        let title = format!("LOTUS Wikidata SMILES — {}", fetched_at.date_naive());
        if args.dry_run {
            let plan = dry_run_publish_archive(
                &artifacts,
                &title,
                &args.creator,
                fetched_at.date_naive(),
            )?;
            info!(%plan, "Zenodo dry run; no Zenodo request was made");
        } else {
            let record_id =
                publish_archive(&artifacts, &title, &args.creator, fetched_at.date_naive()).await?;
            info!(record_id, "Zenodo record published");
        }
    }
    Ok(())
}
