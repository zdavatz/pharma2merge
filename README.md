# pharma2merge

A Rust CLI tool for diffing and merging Swiss pharmaceutical data from [Swissmedic](https://www.swissmedic.ch/) and the [FOPH Spezialitätenliste (SL)](https://www.bag.admin.ch/).

It downloads, compares, and merges package registrations and price data, producing structured JSON and HTML diff reports with numeric change-flags compatible with the [Ruby ODDB OuwerkerkPlugin](https://github.com/zdavatz/oddb.org).

## Features

- **Download** current Swissmedic XLSX (converted to CSV) and FOPH SL NDJSON exports
- **Swissmedic diff** — compare two Swissmedic CSV snapshots, detecting added/deleted packages and field changes (name, owner, category, composition, indication, etc.)
- **FOPH diff** — compare two FOPH SL NDJSON exports, detecting new/deleted packages, SL entry changes, name changes, and retail/ex-factory price movements
- **Merge** — combine a FOPH price-change JSON and a Swissmedic-change JSON into a single `med-drugs-update` report
- **HTML output** — generate a styled HTML diff report with summary table, table of contents, and color-coded added/deleted/changed rows

## Data Sources

| Source | Format | URL |
|---|---|---|
| Swissmedic registered packages | XLSX | `zugelassene_packungen_ham.xlsx` from swissmedic.ch |
| FOPH Spezialitätenliste | NDJSON (FHIR Bundles) | `foph-sl-export-*.ndjson` from epl.bag.admin.ch |

## Numeric Change Flags

Flag codes match the Ruby `ODDB::OuwerkerkPlugin::NUMERIC_FLAGS`:

| Flag | Category |
|---:|---|
| 1 | new |
| 2 | sl_entry_delete |
| 3 | name_base |
| 4 | address (owner) |
| 5 | ikscat (Swissmedic category) |
| 6 | composition |
| 7 | indication |
| 8 | sequence (Handelsform) |
| 9 | expiry_date |
| 10 | sl_entry |
| 11 | price |
| 13 | price_rise |
| 14 | delete |
| 15 | price_cut |

## Requirements

- Rust 1.70+

## Build

```bash
cargo build --release
```

## Usage

### Download current data

```bash
pharma2merge --download
```

Downloads `csv/swissmedic_DD.MM.YYYY.csv` and `ndjson/sl_foph_DD.MM.YYYY.ndjson`.

### Swissmedic diff

```bash
pharma2merge --swissmedic-diff csv/swissmedic_07.01.2026.csv csv/swissmedic_06.02.2026.csv
```

Outputs `csv/diff_07.01.2026-06.02.2026.json`.

### FOPH / BAG price diff

```bash
pharma2merge --foph-diff ndjson/sl_foph_05.01.2026.ndjson ndjson/sl_foph_06.02.2026.ndjson
```

Outputs `ndjson/diff_05.01.2026-06.02.2026.json`.

Filter by category (prints GTINs only):

```bash
pharma2merge --foph-diff --retail_up ndjson/sl_foph_old.ndjson ndjson/sl_foph_new.ndjson
```

### Merge into final report

```bash
pharma2merge ndjson/diff_05.01.2026-06.02.2026.json csv/diff_07.01.2026-06.02.2026.json
```

Outputs `diff/med-drugs-update_DD.MM.YYYY.json`.

### Merge with HTML output

```bash
pharma2merge --html ndjson/diff_05.01.2026-06.02.2026.json csv/diff_07.01.2026-06.02.2026.json
```

Outputs both the JSON and an HTML report at `diff/med-drugs-update_DD.MM.YYYY.html`.

## Output Directories

| Directory | Contents |
|---|---|
| `csv/` | Swissmedic CSV snapshots and Swissmedic diff JSON |
| `ndjson/` | FOPH SL NDJSON exports and FOPH diff JSON |
| `diff/` | Merged `med-drugs-update` JSON and HTML reports |

## License

GPL-3.0 — see [LICENSE](LICENSE).
