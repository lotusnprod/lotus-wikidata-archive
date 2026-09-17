//! Fetch, validate, package, and publish LOTUS SMILES sourced from Wikidata.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use smiles_rs::Smiles;
use tar::{Builder, Header};
use zenodo_rs::{
    AccessRight, DepositMetadataUpdate, DepositionId, UploadSpec, UploadType, ZenodoClient,
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
    inchi_key: SparqlValue,
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
        let mut records = Vec::new();
        let mut offset = 0;

        loop {
            let bindings = self.fetch_page(offset).await?;
            let page_len = bindings.len();
            records.extend(decode_sparql_bindings(bindings)?);

            if page_len < QLEVER_PAGE_SIZE {
                return Ok(records);
            }
            offset = offset
                .checked_add(QLEVER_PAGE_SIZE)
                .context("QLever page offset overflow")?;
        }
    }

    async fn fetch_page(&self, offset: usize) -> Result<Vec<SparqlBinding>> {
        const MAX_RETRIES: u32 = 5;

        for attempt in 0..=MAX_RETRIES {
            let query = query_for_page(offset);
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

fn query_for_page(offset: usize) -> String {
    format!("{LOTUS_SMILES_QUERY}\nLIMIT {QLEVER_PAGE_SIZE}\nOFFSET {offset}")
}

fn decode_sparql_bindings(bindings: Vec<SparqlBinding>) -> Result<Vec<SmilesRecord>> {
    bindings
        .into_iter()
        .map(|binding| {
            let wikidata_id = binding
                .compound
                .value
                .rsplit('/')
                .next()
                .filter(|id| id.starts_with('Q'))
                .context("QLever compound is not a Wikidata entity URI")?
                .to_owned();
            Ok(SmilesRecord {
                inchi_key: binding.inchi_key.value,
                wikidata_id,
                smiles_kind: SmilesKind::parse(&binding.smiles_kind.value)?,
                smiles: binding.smiles.value,
            })
        })
        .collect()
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
    /// SPARQL query before its stable `LIMIT`/`OFFSET` pagination suffix.
    query: &'static str,
    /// Maximum result rows requested by each stable query page.
    page_size: usize,
    /// Number of retrieved source values before InChIKey deduplication.
    source_records: usize,
    /// Number of selected records after InChIKey deduplication.
    records: usize,
    parse_errors: usize,
}

/// Writes a gzip-compressed tar archive containing data, parser errors, and provenance.
///
/// The archive members have a fixed metadata timestamp so the only intended run-specific
/// value is `manifest.json`'s `fetched_at` field.
pub fn write_archive(
    output: impl AsRef<Path>,
    fetched_at: DateTime<Utc>,
    endpoint: &str,
    source_records: usize,
    records: &[SmilesRecord],
    parse_errors: &[ParseErrorRecord],
) -> Result<PathBuf> {
    let output = output.as_ref();
    let parent = output
        .parent()
        .context("archive output path has no parent directory")?;
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;

    let records_csv = records_csv_bytes(records).context("encode LOTUS SMILES CSV")?;
    let errors_csv = parse_errors_csv_bytes(parse_errors).context("encode parser error CSV")?;
    let manifest = serde_json::to_vec_pretty(&Manifest {
        archive_format: "lotus-wikidata-smiles/v1",
        fetched_at,
        qlever_endpoint: endpoint,
        query: LOTUS_SMILES_QUERY,
        page_size: QLEVER_PAGE_SIZE,
        source_records,
        records: records.len(),
        parse_errors: parse_errors.len(),
    })
    .context("encode archive manifest")?;

    let file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut archive = Builder::new(GzEncoder::new(file, Compression::default()));
    append_member(&mut archive, "lotus-wikidata-smiles.csv", &records_csv)?;
    append_member(&mut archive, "smiles-parse-errors.csv", &errors_csv)?;
    append_member(&mut archive, "manifest.json", &manifest)?;
    archive.finish().context("finish archive")?;

    Ok(output.to_owned())
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
    archive: &Path,
    title: &str,
    creator: &str,
    publication_date: chrono::NaiveDate,
) -> Result<String> {
    let _metadata = zenodo_metadata(title, creator, publication_date)?;
    let _files = archive_upload(archive)?;
    match std::env::var("ZENODO_ROOT_DEPOSITION_ID") {
        Ok(root_id) => {
            root_id
                .parse::<u64>()
                .context("parse ZENODO_ROOT_DEPOSITION_ID as a deposition ID")?;
            Ok(format!("publish a new version from deposition {root_id}"))
        }
        Err(std::env::VarError::NotPresent) => Ok("publish the initial Zenodo record".to_owned()),
        Err(error) => Err(error).context("read ZENODO_ROOT_DEPOSITION_ID"),
    }
}

/// Publishes an archive to the LOTUS Zenodo community using `ZENODO_TOKEN`.
///
/// Set `ZENODO_ROOT_DEPOSITION_ID` to the first published deposition ID to
/// create a new version in that record family. Without it, this bootstraps a
/// new record; configure the emitted ID before the next scheduled run.
pub async fn publish_archive(
    archive: &Path,
    title: &str,
    creator: &str,
    publication_date: chrono::NaiveDate,
) -> Result<u64> {
    let client = ZenodoClient::from_env().context("read ZENODO_TOKEN")?;
    let metadata = zenodo_metadata(title, creator, publication_date)?;
    let files = archive_upload(archive)?;

    let publication = match std::env::var("ZENODO_ROOT_DEPOSITION_ID") {
        Ok(root_id) => {
            let root_id = root_id
                .parse()
                .context("parse ZENODO_ROOT_DEPOSITION_ID as a deposition ID")?;
            let draft = client
                .ensure_editable_draft(DepositionId(root_id))
                .await
                .context("create or reuse versioned Zenodo draft")?;
            client
                .publish_dataset(draft.id, &metadata, files)
                .await
                .context("publish versioned archive to Zenodo")?
        }
        Err(std::env::VarError::NotPresent) => client
            .create_and_publish_dataset(&metadata, files)
            .await
            .context("publish initial archive to Zenodo")?,
        Err(error) => return Err(error).context("read ZENODO_ROOT_DEPOSITION_ID"),
    };
    Ok(publication.record.id.0)
}

fn zenodo_metadata(
    title: &str,
    creator: &str,
    publication_date: chrono::NaiveDate,
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
        .community_identifier(LOTUS_ZENODO_COMMUNITY)
        .build()
        .context("build Zenodo metadata")
}

fn archive_upload(archive: &Path) -> Result<Vec<UploadSpec>> {
    let archive_name = archive
        .file_name()
        .context("archive path does not have a filename")?
        .to_string_lossy()
        .into_owned();
    UploadSpec::from_named_paths([(archive_name, archive)]).context("prepare Zenodo archive upload")
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
        let directory = tempdir().unwrap();
        let path = directory.path().join("lotus.tar.gz");
        let records = vec![record("KEY1", "Q1", SmilesKind::Canonical, "CCO")];
        let errors = validate_smiles(&records);

        write_archive(
            &path,
            Utc.with_ymd_and_hms(2026, 9, 17, 0, 0, 0).unwrap(),
            DEFAULT_QLEVER_ENDPOINT,
            records.len(),
            &records,
            &errors,
        )
        .unwrap();

        let mut archive = tar::Archive::new(GzDecoder::new(File::open(path).unwrap()));
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
            members["smiles-parse-errors.csv"],
            "inchi_key,wikidata_id,smiles_kind,smiles,error\n"
        );
        let manifest: serde_json::Value = serde_json::from_str(&members["manifest.json"]).unwrap();
        assert_eq!(manifest["query"], LOTUS_SMILES_QUERY);
        assert_eq!(manifest["page_size"], QLEVER_PAGE_SIZE);
        assert_eq!(manifest["source_records"], 1);
        assert_eq!(manifest["records"], 1);
        assert_eq!(manifest["parse_errors"], 0);
    }

    #[test]
    fn decodes_qlever_rows_and_deduplicates_by_inchi_key() {
        let bindings = vec![SparqlBinding {
            compound: SparqlValue {
                value: "http://www.wikidata.org/entity/Q153".into(),
            },
            inchi_key: SparqlValue {
                value: "LFQSCWFLJHTTHZ-UHFFFAOYSA-N".into(),
            },
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
}
