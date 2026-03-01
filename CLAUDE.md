# CLAUDE.md

## Project Overview

pharma2merge is a Rust CLI tool that diffs and merges Swiss pharmaceutical registration data from Swissmedic and FOPH (BAG) Spezialitätenliste.

## Architecture

- `src/main.rs` — Entry point, CLI argument parsing, Swissmedic CSV diff logic, merge logic, HTML report generation, download helpers
- `src/foph_diff.rs` — FOPH NDJSON (FHIR Bundle) parsing, price extraction with date-based effective pricing, parallel diff computation using rayon

## Key Concepts

- **GTIN construction**: Built from Swissmedic registration number + pack code with EAN-13 checksum (`7680XXXXXYYYZ`)
- **Numeric flags**: Integer codes 1-16 matching Ruby `ODDB::OuwerkerkPlugin::NUMERIC_FLAGS` — used consistently across both Swissmedic and FOPH diffs
- **FOPH price evaluation**: Prices have `changeDate` fields; the tool picks the most recent price effective on or before the bundle's timestamp date
- **Parallel processing**: Uses `rayon` for concurrent NDJSON loading and bundle processing

## Build & Run

```bash
cargo build --release
./target/release/pharma2merge --help  # shows usage
```

## Data Directories

- `csv/` — Swissmedic CSV files and diff output (not committed)
- `ndjson/` — FOPH SL exports and diff output (not committed)
- `diff/` — Merged reports (not committed)

## Dependencies

- `calamine` — Excel XLSX reading
- `chrono` — Date handling
- `serde` / `serde_json` — JSON serialization
- `reqwest` (blocking) — HTTP downloads
- `rayon` — Parallel iteration
- `csv` — CSV parsing

## Conventions

- Date format in filenames: `DD.MM.YYYY`
- All JSON output is pretty-printed
- GTINs are always 13-digit strings starting with `7680`
- Output JSON includes a `_flag_legend` key mapping flag numbers to category names
