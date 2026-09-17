//! Fetch, validate, package, and publish LOTUS SMILES sourced from Wikidata.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smiles_rs::Smiles;
use tar::{Builder, Header};
use zenodo_rs::{
    AccessRight, DepositMetadataUpdate, DepositionId, RelatedIdentifier, UploadSpec, UploadType,
    ZenodoClient,
};
/// QLever's public Wikidata SPARQL endpoint.
pub const DEFAULT_QLEVER_ENDPOINT: &str = "https://qlever.dev/api/wikidata";
/// Zenodo community identifier for the LOTUS Initiative.
pub const LOTUS_ZENODO_COMMUNITY: &str = "the-lotus-initiative";
/// Rows fetched per stable QLever page; explicit pagination avoids endpoint defaults.
pub const QLEVER_PAGE_SIZE: usize = 50_000;

/// The query intentionally retains canonical and isomeric values as separate rows.
///
/// A compound is recognized by a structure statement (`P233` or `P2017`) and
/// is included when it has a `P703` (found in taxon) statement. `P233` and
/// `P2017` are not interchangeable: canonical values describe connectivity while
/// isomeric values carry stereochemistry and isotopes. The source value is stored
/// verbatim and is never re-canonicalized.
pub const LOTUS_SMILES_QUERY: &str = r#"
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT ?compound ?inchiKey ?smiles ?smilesKind WHERE {
  ?compound wdt:P703 ?taxon ;
            wdt:P235 ?inchiKey .
  {
    ?compound wdt:P233 ?smiles .
    BIND("canonical" AS ?smilesKind)
  }
  UNION
  {
    ?compound wdt:P2017 ?smiles .
    BIND("isomeric" AS ?smilesKind)
  }
}
ORDER BY ?compound ?smilesKind ?smiles
"#;

/// Finds otherwise eligible source statements that lack an InChIKey.
///
/// These rows cannot be safely deduplicated and are reported separately.
pub const LOTUS_SMILES_WITHOUT_INCHIKEY_QUERY: &str = r#"
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT DISTINCT ?compound ?smiles ?smilesKind WHERE {
  ?compound wdt:P703 ?taxon .
  FILTER NOT EXISTS { ?compound wdt:P235 ?inchiKey }
  {
    ?compound wdt:P233 ?smiles .
    BIND("canonical" AS ?smilesKind)
  }
  UNION
  {
    ?compound wdt:P2017 ?smiles .
    BIND("isomeric" AS ?smilesKind)
  }
}
ORDER BY ?compound ?smilesKind ?smiles
"#;

/// The Wikidata property from which a SMILES value was obtained.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SmilesKind {
    /// Wikidata P233, canonical SMILES.
    Canonical,
    /// Wikidata P2017, isomeric SMILES.
    Isomeric,
}

impl SmilesKind {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "canonical" => Ok(Self::Canonical),
            "isomeric" => Ok(Self::Isomeric),
            _ => bail!("QLever returned unsupported SMILES kind {value:?}"),
        }
    }
}

/// One verbatim SMILES statement from a Wikidata chemical compound.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SmilesRecord {
    /// Standard InChIKey (Wikidata P235), used as the compound identity.
    pub inchi_key: String,
    /// Wikidata entity ID, e.g. `Q153`, supplying this source value.
    pub wikidata_id: String,
    /// Whether this is P233 or P2017.
    pub smiles_kind: SmilesKind,
    /// The unmodified Wikidata string value.
    pub smiles: String,
}

/// Source SMILES with P703 but no P235 InChIKey.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MissingInchiKeyRecord {
    /// Wikidata entity ID, e.g. `Q153`.
    pub wikidata_id: String,
    /// Whether this is P233 or P2017.
    pub smiles_kind: SmilesKind,
    /// The unmodified Wikidata string value.
    pub smiles: String,
}
/// A SMILES parser failure included beside the source archive.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ParseErrorRecord {
    /// Standard InChIKey identifying the deduplication group.
    pub inchi_key: String,
    /// Wikidata entity ID.
    pub wikidata_id: String,
    /// Property represented by the failed value.
    pub smiles_kind: SmilesKind,
    /// The source string that failed to parse.
    pub smiles: String,
    /// Error emitted by `smiles-rs`.
    pub error: String,
}

#[derive(Deserialize)]
struct SparqlResponse {
    results: SparqlResults,
}

#[derive(Deserialize)]
struct SparqlResults {
    bindings: Vec<SparqlBinding>,
}

#[derive(Deserialize)]
struct SparqlBinding {
    compound: SparqlValue,
    #[serde(rename = "inchiKey")]
    inchi_key: Option<SparqlValue>,
    smiles: SparqlValue,
    #[serde(rename = "smilesKind")]
    smiles_kind: SparqlValue,
}

#[derive(Deserialize)]
struct SparqlValue {
    value: String,
}

/// HTTP client for fetching the complete LOTUS SMILES projection from QLever.
#[derive(Clone, Debug)]
pub struct QleverClient {
    endpoint: String,
    client: Client,
}

impl QleverClient {
    /// Constructs a client for the supplied QLever SPARQL endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: Client::new(),
        }
    }

    /// Executes every stable page of [`LOTUS_SMILES_QUERY`] and returns the
    /// already ordered, duplicate-free source records.
    pub async fn fetch_lotus_smiles(&self) -> Result<Vec<SmilesRecord>> {
        decode_sparql_bindings(self.fetch_bindings(LOTUS_SMILES_QUERY).await?)
    }

    /// Retrieves source values that cannot be deduplicated because P235 is absent.
    pub async fn fetch_missing_inchi_key_smiles(&self) -> Result<Vec<MissingInchiKeyRecord>> {
        decode_missing_inchi_key_bindings(
            self.fetch_bindings(LOTUS_SMILES_WITHOUT_INCHIKEY_QUERY)
                .await?,
        )
    }

    async fn fetch_bindings(&self, query: &str) -> Result<Vec<SparqlBinding>> {
        let mut bindings = Vec::new();
        let mut offset = 0;

        loop {
            let page = self.fetch_page(query, offset).await?;
            let page_len = page.len();
            bindings.extend(page);

            if page_len < QLEVER_PAGE_SIZE {
                return Ok(bindings);
            }
            offset = offset
                .checked_add(QLEVER_PAGE_SIZE)
                .context("QLever page offset overflow")?;
        }
    }

    async fn fetch_page(&self, base_query: &str, offset: usize) -> Result<Vec<SparqlBinding>> {
        const MAX_RETRIES: u32 = 5;

        for attempt in 0..=MAX_RETRIES {
            let query = query_for_page(base_query, offset);
            let response = match self
                .client
                .get(&self.endpoint)
                .header("Accept", "application/sparql-results+json")
                .header("User-Agent", "lotus-wikidata-archive/0.1")
                .query(&[("query", query.as_str())])
                .send()
                .await
            {
                Ok(response) => response,
                Err(_error) if attempt < MAX_RETRIES => {
                    tokio::time::sleep(retry_delay(None, attempt)).await;
                    continue;
                }
                Err(error) => return Err(error).context("submit LOTUS query to QLever"),
            };

            if response.status().is_success() {
                return response
                    .json::<SparqlResponse>()
                    .await
                    .map(|response| response.results.bindings)
                    .context("decode QLever SPARQL JSON result");
            }

            let status = response.status();
            if (status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
                && attempt < MAX_RETRIES
            {
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok());
                tokio::time::sleep(retry_delay(retry_after, attempt)).await;
                continue;
            }

            bail!("QLever rejected LOTUS query page at offset {offset}: {status}");
        }

        unreachable!("retry loop always returns");
    }
}

fn retry_delay(retry_after: Option<u64>, attempt: u32) -> Duration {
    retry_after
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(2_u64.pow(attempt).min(32)))
}

fn query_for_page(base_query: &str, offset: usize) -> String {
    format!("{base_query}\nLIMIT {QLEVER_PAGE_SIZE}\nOFFSET {offset}")
}

fn decode_sparql_bindings(bindings: Vec<SparqlBinding>) -> Result<Vec<SmilesRecord>> {
    bindings
        .into_iter()
        .map(|binding| {
            let wikidata_id = wikidata_id(&binding.compound.value)?;
            let inchi_key = binding
                .inchi_key
                .context("QLever keyed result lacks an InChIKey")?
                .value;
            Ok(SmilesRecord {
                inchi_key,
                wikidata_id,
                smiles_kind: SmilesKind::parse(&binding.smiles_kind.value)?,
                smiles: binding.smiles.value,
            })
        })
        .collect()
}

fn decode_missing_inchi_key_bindings(
    bindings: Vec<SparqlBinding>,
) -> Result<Vec<MissingInchiKeyRecord>> {
    bindings
        .into_iter()
        .map(|binding| {
            if binding.inchi_key.is_some() {
                bail!("QLever missing-InChIKey result unexpectedly includes P235");
            }
            Ok(MissingInchiKeyRecord {
                wikidata_id: wikidata_id(&binding.compound.value)?,
                smiles_kind: SmilesKind::parse(&binding.smiles_kind.value)?,
                smiles: binding.smiles.value,
            })
        })
        .collect()
}

fn wikidata_id(uri: &str) -> Result<String> {
    uri.rsplit('/')
        .next()
        .filter(|id| id.starts_with('Q'))
        .context("QLever compound is not a Wikidata entity URI")
        .map(ToOwned::to_owned)
}

/// Validates each source value with the strict `smiles-rs` parser.
///
/// Invalid records remain in the data CSV; this report makes a failed parse
/// observable without discarding source data or silently changing chemistry.
pub fn validate_smiles(records: &[SmilesRecord]) -> Vec<ParseErrorRecord> {
    records
        .iter()
        .filter_map(|record| {
            record
                .smiles
                .parse::<Smiles>()
                .err()
                .map(|error| ParseErrorRecord {
                    inchi_key: record.inchi_key.clone(),
                    wikidata_id: record.wikidata_id.clone(),
                    smiles_kind: record.smiles_kind,
                    smiles: record.smiles.clone(),
                    error: error.to_string(),
                })
        })
        .collect()
}

/// Validates source values excluded solely because they lack an InChIKey.
pub fn validate_missing_inchi_key_smiles(
    records: &[MissingInchiKeyRecord],
) -> Vec<ParseErrorRecord> {
    records
        .iter()
        .filter_map(|record| {
            record
                .smiles
                .parse::<Smiles>()
                .err()
                .map(|error| ParseErrorRecord {
                    inchi_key: String::new(),
                    wikidata_id: record.wikidata_id.clone(),
                    smiles_kind: record.smiles_kind,
                    smiles: record.smiles.clone(),
                    error: error.to_string(),
                })
        })
        .collect()
}

/// Selects one record per InChIKey, preferring P2017 isomeric SMILES over P233.
///
/// Multiple values of the preferred property are resolved deterministically by
/// source SMILES then Wikidata ID, so repeated pulls yield the same archive.
pub fn deduplicate_by_inchi_key(records: Vec<SmilesRecord>) -> Vec<SmilesRecord> {
    let mut selected = BTreeMap::new();
    for record in records {
        let inchi_key = record.inchi_key.clone();
        match selected.entry(inchi_key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(record);
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if preferred_over(&record, entry.get()) =>
            {
                entry.insert(record);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    selected.into_values().collect()
}

fn preferred_over(candidate: &SmilesRecord, current: &SmilesRecord) -> bool {
    let candidate_priority = matches!(candidate.smiles_kind, SmilesKind::Isomeric);
    let current_priority = matches!(current.smiles_kind, SmilesKind::Isomeric);
    (candidate_priority && !current_priority)
        || (candidate_priority == current_priority
            && (candidate.smiles.as_str(), candidate.wikidata_id.as_str())
                < (current.smiles.as_str(), current.wikidata_id.as_str()))
}

#[derive(Serialize)]
struct Manifest<'a> {
    archive_format: &'static str,
    fetched_at: DateTime<Utc>,
    qlever_endpoint: &'a str,
    /// SPARQL query selecting source records with P235.
    query: &'a str,
    /// SPARQL query used to find entries excluded for missing P235.
    missing_inchi_key_query: &'static str,
    /// Maximum result rows requested by each stable query page.
    page_size: usize,
    /// Number of source values with an InChIKey before deduplication.
    source_records: usize,
    /// Number of source values excluded because P235 is absent.
    missing_inchi_key_records: usize,
    /// Number of selected records after InChIKey deduplication.
    records: usize,
    parse_errors: usize,
}

/// Paths emitted for one source snapshot.
///
/// The manifest and checksum file are standalone Zenodo uploads; the manifest
/// is also an archive member, so provenance and integrity remain inspectable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveArtifacts {
    /// Gzip-compressed source archive.
    pub archive: PathBuf,
    /// Standalone copy of the archive manifest.
    pub manifest: PathBuf,
    /// SHA-256 digests of the archive and standalone manifest.
    pub checksums: PathBuf,
}

/// Writes a gzip-compressed tar archive and a standalone manifest beside it.
///
/// The archive members have a fixed metadata timestamp so the only intended run-specific
/// value is `manifest.json`'s `fetched_at` field.
pub fn write_archive(
    output: impl AsRef<Path>,
    fetched_at: DateTime<Utc>,
    endpoint: &str,
    source_records: usize,
    missing_inchi_key_records: &[MissingInchiKeyRecord],
    records: &[SmilesRecord],
    parse_errors: &[ParseErrorRecord],
) -> Result<ArchiveArtifacts> {
    let output = output.as_ref();
    let parent = output
        .parent()
        .context("archive output path has no parent directory")?;
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;

    let records_csv = records_csv_bytes(records).context("encode LOTUS SMILES CSV")?;
    let missing_inchi_key_csv = missing_inchi_key_csv_bytes(missing_inchi_key_records)
        .context("encode missing InChIKey CSV")?;
    let errors_csv = parse_errors_csv_bytes(parse_errors).context("encode parser error CSV")?;
    let manifest = serde_json::to_vec_pretty(&Manifest {
        archive_format: "lotus-wikidata-smiles/v1",
        fetched_at,
        qlever_endpoint: endpoint,
        query: LOTUS_SMILES_QUERY,
        missing_inchi_key_query: LOTUS_SMILES_WITHOUT_INCHIKEY_QUERY,
        page_size: QLEVER_PAGE_SIZE,
        source_records,
        missing_inchi_key_records: missing_inchi_key_records.len(),
        records: records.len(),
        parse_errors: parse_errors.len(),
    })
    .context("encode archive manifest")?;

    let file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut archive = Builder::new(GzEncoder::new(file, Compression::default()));
    append_member(&mut archive, "lotus-wikidata-smiles.csv", &records_csv)?;
    append_member(&mut archive, "missing-inchikey.csv", &missing_inchi_key_csv)?;
    append_member(&mut archive, "smiles-parse-errors.csv", &errors_csv)?;
    append_member(&mut archive, "manifest.json", &manifest)?;
    archive
        .into_inner()
        .context("finish tar archive")?
        .finish()
        .context("finish gzip archive")?;

    let manifest_path = sibling_artifact_path(output, "manifest.json")?;
    std::fs::write(&manifest_path, &manifest)
        .with_context(|| format!("write {}", manifest_path.display()))?;
    let checksum_path = sibling_artifact_path(output, "SHA256SUMS")?;
    let archive_filename = output
        .file_name()
        .context("archive output path has no filename")?
        .to_string_lossy();
    let manifest_filename = manifest_path
        .file_name()
        .context("manifest output path has no filename")?
        .to_string_lossy();
    write_checksums(
        &checksum_path,
        &[
            (output, archive_filename.as_ref()),
            (&manifest_path, manifest_filename.as_ref()),
        ],
    )?;
    Ok(ArchiveArtifacts {
        archive: output.to_owned(),
        manifest: manifest_path,
        checksums: checksum_path,
    })
}

fn sibling_artifact_path(archive: &Path, suffix: &str) -> Result<PathBuf> {
    let filename = archive
        .file_name()
        .context("archive output path has no filename")?
        .to_string_lossy();
    let stem = filename.strip_suffix(".tar.gz").unwrap_or(&filename);
    Ok(archive.with_file_name(format!("{stem}.{suffix}")))
}

fn records_csv_bytes(records: &[SmilesRecord]) -> Result<Vec<u8>, csv::Error> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    writer.write_record(["inchi_key", "wikidata_id", "smiles_kind", "smiles"])?;
    for record in records {
        writer.serialize(record)?;
    }
    Ok(writer.into_inner().map_err(|error| error.into_error())?)
}

fn parse_errors_csv_bytes(records: &[ParseErrorRecord]) -> Result<Vec<u8>, csv::Error> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    writer.write_record(["inchi_key", "wikidata_id", "smiles_kind", "smiles", "error"])?;
    for record in records {
        writer.serialize(record)?;
    }
    Ok(writer.into_inner().map_err(|error| error.into_error())?)
}

fn missing_inchi_key_csv_bytes(records: &[MissingInchiKeyRecord]) -> Result<Vec<u8>, csv::Error> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    writer.write_record(["wikidata_id", "smiles_kind", "smiles"])?;
    for record in records {
        writer.serialize(record)?;
    }
    Ok(writer.into_inner().map_err(|error| error.into_error())?)
}

fn write_checksums(path: &Path, files: &[(&Path, &str)]) -> Result<()> {
    let mut output = String::new();
    for (file, label) in files {
        output.push_str(&sha256(file)?);
        output.push_str("  ");
        output.push_str(label);
        output.push('\n');
    }
    std::fs::write(path, output).with_context(|| format!("write {}", path.display()))
}

fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            return Ok(format!("{:x}", hasher.finalize()));
        }
        hasher.update(&buffer[..read]);
    }
}

fn append_member(
    archive: &mut Builder<GzEncoder<File>>,
    name: &str,
    contents: &[u8],
) -> Result<()> {
    let mut header = Header::new_gnu();
    header.set_size(contents.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_cksum();
    archive
        .append_data(&mut header, name, contents)
        .with_context(|| format!("append {name} to archive"))
}

/// Validates a planned Zenodo publication without making an HTTP request.
///
/// The returned text identifies whether an eventual publication would create
/// the first record or a new version in an existing record family.
pub fn dry_run_publish_archive(
    artifacts: &ArchiveArtifacts,
    title: &str,
    creator: &str,
    publication_date: chrono::NaiveDate,
) -> Result<String> {
    let source_code_url = source_code_url()?;
    let _metadata = zenodo_metadata(title, creator, publication_date, &source_code_url)?;
    let _files = archive_upload(artifacts)?;
    match root_deposition_id()? {
        Some(root_id) => Ok(format!(
            "publish a new version from deposition {}",
            root_id.0
        )),
        None => Ok("publish the initial Zenodo record".to_owned()),
    }
}

/// Publishes an archive to the LOTUS Zenodo community using `ZENODO_TOKEN`.
///
/// Set `ZENODO_ROOT_DEPOSITION_ID` to the first published deposition ID to
/// create a new version in that record family. Without it, this bootstraps a
/// new record; configure the emitted ID before the next scheduled run.
pub async fn publish_archive(
    artifacts: &ArchiveArtifacts,
    title: &str,
    creator: &str,
    publication_date: chrono::NaiveDate,
) -> Result<u64> {
    let client = ZenodoClient::from_env().context("read ZENODO_TOKEN")?;
    let source_code_url = source_code_url()?;
    let metadata = zenodo_metadata(title, creator, publication_date, &source_code_url)?;
    let files = archive_upload(artifacts)?;

    let publication = match root_deposition_id()? {
        Some(root_id) => {
            let draft = client
                .ensure_editable_draft(root_id)
                .await
                .context("create or reuse versioned Zenodo draft")?;
            client
                .publish_dataset(draft.id, &metadata, files)
                .await
                .context("publish versioned archive to Zenodo")?
        }
        None => client
            .create_and_publish_dataset(&metadata, files)
            .await
            .context("publish initial archive to Zenodo")?,
    };
    Ok(publication.record.id.0)
}

fn zenodo_metadata(
    title: &str,
    creator: &str,
    publication_date: chrono::NaiveDate,
    source_code_url: &str,
) -> Result<DepositMetadataUpdate> {
    DepositMetadataUpdate::builder()
        .title(title)
        .upload_type(UploadType::Dataset)
        .publication_date(publication_date)
        .description_html(
            "<p>InChIKey-deduplicated Wikidata SMILES for chemical structures with P703 (found in taxon), fetched from QLever. Isomeric P2017 values take precedence over canonical P233 values. The archive includes a smiles-rs parse-error report and query provenance.</p>",
        )
        .creator_named(creator)
        .access_right(AccessRight::Open)
        .license("cc-by-4.0")
        .keyword("LOTUS")
        .keyword("Wikidata")
        .keyword("SMILES")
        .related_identifier(
            RelatedIdentifier::builder()
                .identifier(source_code_url)
                .relation("isDerivedFrom")
                .scheme("url")
                .resource_type("software")
                .build()
                .context("build source-code related identifier")?,
        )
        .community_identifier(LOTUS_ZENODO_COMMUNITY)
        .build()
        .context("build Zenodo metadata")
}

fn source_code_url() -> Result<String> {
    if let Ok(url) = std::env::var("SOURCE_CODE_URL") {
        return validate_source_code_url(url);
    }

    let server = std::env::var("GITHUB_SERVER_URL");
    let repository = std::env::var("GITHUB_REPOSITORY");
    let revision = std::env::var("GITHUB_SHA");
    match (server, repository, revision) {
        (Ok(server), Ok(repository), Ok(revision)) => {
            validate_source_code_url(format!("{server}/{repository}/commit/{revision}"))
        }
        _ => bail!(
            "set SOURCE_CODE_URL to the exact immutable source revision before publishing locally"
        ),
    }
}

fn validate_source_code_url(url: String) -> Result<String> {
    let Some((_, revision)) = url.rsplit_once("/commit/") else {
        bail!("SOURCE_CODE_URL must identify a commit with a /commit/<sha> suffix");
    };
    if !url.starts_with("https://")
        || revision.len() != 40
        || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("SOURCE_CODE_URL must be an https URL ending in a full 40-character commit SHA");
    }
    Ok(url)
}

fn root_deposition_id() -> Result<Option<DepositionId>> {
    match std::env::var("ZENODO_ROOT_DEPOSITION_ID") {
        Ok(value) => {
            let id: u64 = value
                .parse()
                .context("parse ZENODO_ROOT_DEPOSITION_ID as a published deposition ID")?;
            if id == 0 {
                bail!("ZENODO_ROOT_DEPOSITION_ID must be a non-zero published deposition ID");
            }
            Ok(Some(DepositionId(id)))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).context("read ZENODO_ROOT_DEPOSITION_ID"),
    }
}

fn archive_upload(artifacts: &ArchiveArtifacts) -> Result<Vec<UploadSpec>> {
    let archive_name = artifacts
        .archive
        .file_name()
        .context("archive path does not have a filename")?
        .to_string_lossy()
        .into_owned();
    let manifest_name = artifacts
        .manifest
        .file_name()
        .context("manifest path does not have a filename")?
        .to_string_lossy()
        .into_owned();
    let checksum_name = artifacts
        .checksums
        .file_name()
        .context("checksum path does not have a filename")?
        .to_string_lossy()
        .into_owned();
    UploadSpec::from_named_paths([
        (archive_name, &artifacts.archive),
        (manifest_name, &artifacts.manifest),
        (checksum_name, &artifacts.checksums),
    ])
    .context("prepare Zenodo archive uploads")
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use chrono::TimeZone;
    use flate2::read::GzDecoder;
    use tempfile::tempdir;

    use super::*;
    fn record(key: &str, id: &str, kind: SmilesKind, smiles: &str) -> SmilesRecord {
        SmilesRecord {
            inchi_key: key.to_owned(),
            wikidata_id: id.to_owned(),
            smiles_kind: kind,
            smiles: smiles.to_owned(),
        }
    }

    #[test]
    fn validates_canonical_and_isomeric_source_values_without_rewriting_them() {
        let records = vec![
            record("KEY1", "Q1", SmilesKind::Canonical, "CCO"),
            record("KEY2", "Q2", SmilesKind::Isomeric, "C[C@H](O)C"),
            record("KEY3", "Q3", SmilesKind::Canonical, "C1CC"),
        ];

        let errors = validate_smiles(&records);

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].wikidata_id, "Q3");
        assert_eq!(errors[0].smiles, "C1CC");
        assert!(!errors[0].error.is_empty());
    }

    #[test]
    fn packages_source_errors_and_query_provenance() {
        let missing_inchi_key_records = vec![MissingInchiKeyRecord {
            wikidata_id: "Q2".to_owned(),
            smiles_kind: SmilesKind::Canonical,
            smiles: "O".to_owned(),
        }];
        let directory = tempdir().unwrap();
        let path = directory.path().join("lotus.tar.gz");
        let records = vec![record("KEY1", "Q1", SmilesKind::Canonical, "CCO")];
        let errors = validate_smiles(&records);

        let artifacts = write_archive(
            &path,
            Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap(),
            DEFAULT_QLEVER_ENDPOINT,
            records.len(),
            &missing_inchi_key_records,
            &records,
            &errors,
        )
        .unwrap();

        let mut archive =
            tar::Archive::new(GzDecoder::new(File::open(&artifacts.archive).unwrap()));
        let mut members = std::collections::BTreeMap::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            let mut contents = String::new();
            entry.read_to_string(&mut contents).unwrap();
            members.insert(name, contents);
        }
        assert!(members["lotus-wikidata-smiles.csv"].contains("KEY1,Q1,canonical,CCO"));
        assert_eq!(
            members["missing-inchikey.csv"],
            "wikidata_id,smiles_kind,smiles\nQ2,canonical,O\n"
        );
        assert_eq!(
            members["smiles-parse-errors.csv"],
            "inchi_key,wikidata_id,smiles_kind,smiles,error\n"
        );
        let manifest: serde_json::Value = serde_json::from_str(&members["manifest.json"]).unwrap();
        assert_eq!(
            std::fs::read_to_string(&artifacts.manifest).unwrap(),
            members["manifest.json"]
        );
        let uploads = archive_upload(&artifacts).unwrap();
        assert_eq!(uploads.len(), 3);
        assert_eq!(uploads[1].filename, "lotus.manifest.json");
        assert_eq!(uploads[2].filename, "lotus.SHA256SUMS");
        let checksums = std::fs::read_to_string(&artifacts.checksums).unwrap();
        assert!(checksums.contains("  lotus.tar.gz\n"));
        assert!(checksums.contains("  lotus.manifest.json\n"));
        assert!(checksums.starts_with(&sha256(&artifacts.archive).unwrap()));
        assert_eq!(manifest["query"], LOTUS_SMILES_QUERY);
        assert_eq!(manifest["page_size"], QLEVER_PAGE_SIZE);
        assert_eq!(manifest["source_records"], 1);
        assert_eq!(manifest["records"], 1);
        assert_eq!(manifest["parse_errors"], 0);
        assert_eq!(manifest["missing_inchi_key_records"], 1);
    }
    #[test]
    fn decodes_qlever_rows_and_deduplicates_by_inchi_key() {
        let bindings = vec![SparqlBinding {
            compound: SparqlValue {
                value: "http://www.wikidata.org/entity/Q153".into(),
            },
            inchi_key: Some(SparqlValue {
                value: "LFQSCWFLJHTTHZ-UHFFFAOYSA-N".into(),
            }),
            smiles: SparqlValue {
                value: "CCO".into(),
            },
            smiles_kind: SparqlValue {
                value: "canonical".into(),
            },
        }];
        let records = decode_sparql_bindings(bindings).unwrap();
        assert_eq!(
            records,
            vec![record(
                "LFQSCWFLJHTTHZ-UHFFFAOYSA-N",
                "Q153",
                SmilesKind::Canonical,
                "CCO"
            )]
        );
    }

    #[test]
    fn reports_and_validates_smiles_without_inchi_keys() {
        let missing = decode_missing_inchi_key_bindings(vec![SparqlBinding {
            compound: SparqlValue {
                value: "http://www.wikidata.org/entity/Q154".into(),
            },
            inchi_key: None,
            smiles: SparqlValue {
                value: "C1CC".into(),
            },
            smiles_kind: SparqlValue {
                value: "isomeric".into(),
            },
        }])
        .unwrap();

        assert_eq!(
            missing,
            vec![MissingInchiKeyRecord {
                wikidata_id: "Q154".to_owned(),
                smiles_kind: SmilesKind::Isomeric,
                smiles: "C1CC".to_owned(),
            }]
        );
        let errors = validate_missing_inchi_key_smiles(&missing);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].inchi_key, "");
        assert_eq!(errors[0].wikidata_id, "Q154");
    }

    #[test]
    fn deduplication_prefers_isomeric_smiles_over_canonical_smiles() {
        let selected = deduplicate_by_inchi_key(vec![
            record("KEY1", "Q1", SmilesKind::Canonical, "CCO"),
            record("KEY1", "Q2", SmilesKind::Isomeric, "C[C@H](O)C"),
            record("KEY2", "Q3", SmilesKind::Canonical, "N"),
        ]);

        assert_eq!(
            selected,
            vec![
                record("KEY1", "Q2", SmilesKind::Isomeric, "C[C@H](O)C"),
                record("KEY2", "Q3", SmilesKind::Canonical, "N"),
            ]
        );
    }

    #[test]
    fn publication_metadata_links_the_exact_source_revision() {
        let metadata = zenodo_metadata(
            "LOTUS Wikidata SMILES",
            "The LOTUS Initiative",
            Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0)
                .unwrap()
                .date_naive(),
            "https://github.com/oolonek/lotus-SMILES/commit/0123456789abcdef0123456789abcdef01234567",
        )
        .unwrap();

        assert_eq!(metadata.related_identifiers.len(), 1);
        assert_eq!(
            metadata.related_identifiers[0].identifier,
            "https://github.com/oolonek/lotus-SMILES/commit/0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(metadata.related_identifiers[0].relation, "isDerivedFrom");
    }

    #[test]
    fn rejects_non_immutable_source_code_urls() {
        assert!(
            validate_source_code_url("https://github.com/oolonek/lotus-SMILES".to_owned()).is_err()
        );
        assert!(
            validate_source_code_url(
                "https://github.com/oolonek/lotus-SMILES/commit/0123456789abcdef".to_owned()
            )
            .is_err()
        );
    }
}
