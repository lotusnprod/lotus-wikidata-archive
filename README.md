# LOTUS Wikidata SMILES archive

[![Zenodo DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.22811236.svg)](https://doi.org/10.5281/zenodo.22811236)

`lotus-wikidata-archive` periodically extracts, validates, packages, and publishes the Wikidata projection used for LOTUS.

## Inclusion, deduplication, and SMILES semantics

A source statement is selected when its Wikidata entity has:

- `P703` (*found in taxon*), the LOTUS inclusion criterion;
- `P235` (*InChIKey*), the stable compound identity used for deduplication; and
- `P233` (*canonical SMILES*) or `P2017` (*isomeric SMILES*).

The archive has one record per InChIKey. `P2017` always wins over `P233`, preserving available stereochemistry and isotope information. If no isomeric value exists, the canonical value is retained. If Wikidata supplies multiple values at the same priority, lexical SMILES then Wikidata entity ID resolves the tie deterministically.

Records without an InChIKey are never assigned an ad-hoc identity. They are written to `missing-inchikey.csv` with a manifest count, making this curation gap observable while retaining safe InChIKey deduplication.

The query is submitted as SPARQL Results JSON to QLever's public Wikidata endpoint in stable `LIMIT`/`OFFSET` pages of 50,000 rows. The client honours `Retry-After` and retries transient server and rate-limit responses. This projection has no RDF graph to materialize, so it uses standard SPARQL result JSON rather than adding Sophia solely as a transport dependency.

## Validation and archive contents

Strict `smiles-rs` parsing runs on **all fetched source values before deduplication**. That makes an invalid lower-priority canonical value visible even if an isomeric value is selected for the same InChIKey.

Every run writes a gzip-compressed `tar.gz`, a same-named standalone
`.manifest.json`, and a same-named `.SHA256SUMS` file. The standalone manifest
and checksums are uploaded beside the archive to Zenodo for direct record-UI
inspection; the identical manifest remains inside the tarball for
self-contained archival.

The tarball has deterministic member metadata and four members:

| Member | Contents |
| --- | --- |
| `lotus-wikidata-smiles.csv` | Selected one-per-InChIKey data: `inchi_key`, source `wikidata_id`, `smiles_kind`, and verbatim `smiles`. |
| `missing-inchikey.csv` | P703 + SMILES source values that lack P235 and cannot be safely deduplicated. |
| `smiles-parse-errors.csv` | Every parser failure from the keyed and missing-InChIKey source projections. |
| `manifest.json` | Fetch timestamp, both source queries, page size, keyed/missing source counts, selected count, and error count. |
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

Publishing is opt-in and needs a production token with `deposit:write` and `deposit:actions`. Every published record receives a Zenodo related-resource link to the **exact immutable source revision** that generated it.

On GitHub Actions, this link is constructed from `GITHUB_SERVER_URL`, `GITHUB_REPOSITORY`, and `GITHUB_SHA`. For a local dry run or publication, set it explicitly:

```bash
export SOURCE_CODE_URL="https://github.com/<owner>/<repo>/commit/<full-commit-sha>"
export ZENODO_TOKEN=... # never commit this value
cargo run --release -- --output-dir artifacts --publish --creator 'The LOTUS Initiative'
```

Dry-run the complete local publication plan without contacting Zenodo:

```bash
export SOURCE_CODE_URL="https://github.com/<owner>/<repo>/commit/<full-commit-sha>"
cargo run --release -- --output-dir artifacts --publish --dry-run --creator 'The LOTUS Initiative'
```

This fetches, validates, deduplicates, packages, verifies checksums, and checks the exact Zenodo metadata/upload inputs. It never sends an HTTP request to Zenodo and never creates a draft.

Without `ZENODO_ROOT_DEPOSITION_ID`, a non-dry publication creates the first Zenodo record and prints its version-specific record ID. Set that concrete **published deposition ID** before the next publication:

```bash
export ZENODO_ROOT_DEPOSITION_ID=1234567
```

Do not use a concept DOI or its numeric suffix as `ZENODO_ROOT_DEPOSITION_ID`. With a valid root ID, the client asks `zenodo-rs` for the latest published version, creates its editable `newversion` draft, uploads the next archive, and publishes it. This preserves the Zenodo concept record and its version history rather than creating unrelated deposits. Every record is a public `CC-BY-4.0` Dataset submitted to the `the-lotus-initiative` community. Community acceptance may still require moderator approval.

## Scheduled GitHub Actions operation

`publish.yml` runs every Monday at 03:17 UTC and also supports manual dispatch. It serializes all production publications with the `lotus-zenodo-publish` concurrency group. Configure these repository values before enabling it:

- Actions secret `ZENODO_TOKEN` — production Zenodo token.
- Actions variable `ZENODO_ROOT_DEPOSITION_ID` — concrete published deposition ID, never a concept DOI suffix.

`verify.yml` runs a non-publishing full fetch/package/Zenodo-plan check every Saturday at 03:17 UTC and can also be manually dispatched. It requires no Zenodo secret. Both workflows retain the tarball, manifest, and SHA-256 checksums as Actions artifacts.

## Verification

```bash
cargo test
cargo fmt --check
cargo clippy -- -D warnings
```

Unit tests cover parser-error retention, archive/provenance generation, QLever row decoding, missing-InChIKey reporting, immutable source-revision links, and the observable one-per-InChIKey selection rule. A full QLever run on 2026-09-17 selected 227,409 unique InChIKeys, reported eight source values without InChIKeys, produced four parser errors, and wrote the archive, manifest, and checksums. Production Zenodo publishing is not attempted by tests because it requires an authorized token and creates a permanent record version.
