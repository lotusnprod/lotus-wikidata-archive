# LOTUS Wikidata SMILES archive

`lotus-wikidata-archive` periodically extracts, validates, packages, and publishes the Wikidata projection used for LOTUS.

## Inclusion, deduplication, and SMILES semantics

A source statement is selected when its Wikidata entity has:

- `P703` (*found in taxon*), the LOTUS inclusion criterion;
- `P235` (*InChIKey*), the stable compound identity used for deduplication; and
- `P233` (*canonical SMILES*) or `P2017` (*isomeric SMILES*).

The archive has one record per InChIKey. `P2017` always wins over `P233`, preserving available stereochemistry and isotope information. If no isomeric value exists, the canonical value is retained. If Wikidata supplies multiple values at the same priority, lexical SMILES then Wikidata entity ID resolves the tie deterministically.

Values are never re-rendered or re-canonicalized: canonicality is meaningful only relative to the source producer. Records without an InChIKey are deliberately excluded; assigning an ad-hoc identity would defeat reliable cross-item deduplication.

The query is submitted as SPARQL Results JSON to QLever's public Wikidata endpoint in stable `LIMIT`/`OFFSET` pages of 50,000 rows. The client honours `Retry-After` and retries transient server and rate-limit responses. This projection has no RDF graph to materialize, so it uses standard SPARQL result JSON rather than adding Sophia solely as a transport dependency.

## Validation and archive contents

Strict `smiles-rs` parsing runs on **all fetched source values before deduplication**. That makes an invalid lower-priority canonical value visible even if an isomeric value is selected for the same InChIKey.

Every run writes one `tar.gz` with deterministic member metadata and three members:

| Member | Contents |
| --- | --- |
| `lotus-wikidata-smiles.csv` | Selected one-per-InChIKey data: `inchi_key`, source `wikidata_id`, `smiles_kind`, and verbatim `smiles`. |
| `smiles-parse-errors.csv` | Every parser failure from the fetched source projection, including its InChIKey and parser message. |
| `manifest.json` | Fetch timestamp, QLever endpoint, base SPARQL query, page size, source count, selected count, and error count. |

Invalid input remains observable in the error report instead of being silently discarded. The report ships inside the same Zenodo artifact as the selected archive.

## Run

One archive run:

```bash
cargo run -- --output-dir artifacts
```

A long-running host process can repeat after a fixed interval:

```bash
cargo run --release -- --output-dir artifacts --period-hours 168
```

`--period-hours` must be at least one. The process stops on fetch, package, or publish failure rather than silently skipping a source snapshot.

## Zenodo publishing and record versioning

Publishing is opt-in and needs a production token with `deposit:write` and `deposit:actions`:

```bash
export ZENODO_TOKEN=... # never commit this value
cargo run --release -- --output-dir artifacts --publish --creator 'The LOTUS Initiative'
```

Dry-run the complete local publication plan without contacting Zenodo:

```bash
cargo run --release -- --output-dir artifacts --publish --dry-run --creator 'The LOTUS Initiative'
```

This fetches, validates, deduplicates, packages, and checks the exact Zenodo metadata and upload input. It never sends an HTTP request to Zenodo and never creates a draft.

Without `ZENODO_ROOT_DEPOSITION_ID`, that command creates the first Zenodo record and prints its version-specific record ID. Set that ID as `ZENODO_ROOT_DEPOSITION_ID` before the next publication:

```bash
export ZENODO_ROOT_DEPOSITION_ID=1234567
```

With the root ID set, the client asks `zenodo-rs` for the latest published version, creates its editable `newversion` draft, uploads the next archive, and publishes it. This preserves the Zenodo concept record and its version history rather than creating unrelated deposits. Every record is a public `CC-BY-4.0` Dataset submitted to the `the-lotus-initiative` community. Community acceptance may still require moderator approval.

## Scheduled GitHub Actions operation

`.github/workflows/publish.yml` runs every Monday at 03:17 UTC and also supports manual dispatch. Configure these repository values before enabling scheduled publication:
- Actions secret `ZENODO_TOKEN` — production Zenodo token.
- Actions variable `ZENODO_ROOT_DEPOSITION_ID` — the initial published record ID created during bootstrap.

The workflow refuses to publish without the root ID, preventing accidental creation of a second record family. It retains each generated archive as an Actions artifact for 90 days independently of Zenodo.

## Verification

```bash
cargo test
cargo fmt --check
cargo clippy -- -D warnings
```

Unit tests cover parser error retention, archive/provenance generation, QLever row decoding, and the observable one-per-InChIKey selection rule. A live QLever smoke run fetched 400,513 source statements, selected 227,409 unique InChIKeys, produced two parser errors, and confirmed that the error report is included. Production Zenodo publishing is not attempted by tests because it requires an authorized token and creates a permanent record version.
