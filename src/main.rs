mod foph_diff;

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Cursor, Read, Write};

use calamine::{open_workbook_from_rs, Reader, Xlsx};
use chrono::{Local, Datelike};
use reqwest::blocking::Client;
use serde_json::{Map, Value, json};

// ─── Numeric flags (Swissmedic-side, matching Ruby NUMERIC_FLAGS) ───────────

/// Flags 1-16 matching Ruby OuwerkerkPlugin::NUMERIC_FLAGS.
/// The FOPH-side flags are defined in foph_diff::numeric_flags.
/// Here we define the Swissmedic-relevant subset.
mod swissmedic_flags {
    #![allow(dead_code)]
    pub const NEW: u8              = 1;
    pub const NAME_BASE: u8        = 3;
    pub const ADDRESS: u8          = 4;  // owner/company
    pub const IKSCAT: u8           = 5;
    pub const COMPOSITION: u8      = 6;
    pub const INDICATION: u8       = 7;
    pub const SEQUENCE: u8         = 8;  // handelsform (trade form / sequence info)
    pub const EXPIRY_DATE: u8      = 9;
    pub const DELETE: u8           = 14;
    pub const NOT_SPECIFIED: u8    = 16;
}

// ─── Constants ───────────────────────────────────────────────────────────────

const SWISSMEDIC_URL: &str = "https://www.swissmedic.ch/dam/swissmedic/de/dokumente/internetlisten/zugelassene_packungen_human.xlsx.download.xlsx/zugelassene_packungen_ham.xlsx";
const FOPH_SL_URL: &str = "https://epl.bag.admin.ch/static/fhir/foph-sl-export-20260203.ndjson";

// ─── JSON sanitizer ──────────────────────────────────────────────────────────

fn sanitize_json_string(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_string = false;
    let mut prev_backslash = false;

    for ch in input.chars() {
        if in_string {
            if prev_backslash {
                output.push(ch);
                prev_backslash = false;
                continue;
            }
            match ch {
                '\\' => { output.push(ch); prev_backslash = true; }
                '"'  => { output.push(ch); in_string = false; }
                '\t' => output.push_str("\\t"),
                '\n' => output.push_str("\\n"),
                '\r' => output.push_str("\\r"),
                '\x00'..='\x1F' => {}
                _ => output.push(ch),
            }
        } else {
            if ch == '"' { in_string = true; }
            output.push(ch);
            prev_backslash = false;
        }
    }
    output
}

// ─── CSV helper ──────────────────────────────────────────────────────────────

fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

// ─── Download helpers ────────────────────────────────────────────────────────

fn download_url(client: &Client, url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    println!("Downloading: {}", url);
    let response = client.get(url).send()?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("HTTP {} for {}", status, url).into());
    }
    let bytes = response.bytes()?.to_vec();
    println!("  Downloaded {} bytes", bytes.len());
    Ok(bytes)
}

/// Convert an Excel serial date number to YYYY/MM/DD string.
fn excel_serial_to_date_str(serial: f64) -> Option<String> {
    let days = serial as i64;
    let base = chrono::NaiveDate::from_ymd_opt(1899, 12, 30)?;
    let date = base.checked_add_signed(chrono::Duration::days(days))?;
    Some(format!("{}/{:02}/{:02}", date.year(), date.month(), date.day()))
}

fn xlsx_to_csv(xlsx_bytes: &[u8], csv_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let cursor = Cursor::new(xlsx_bytes);
    let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor)?;
    let sheet_name = workbook.sheet_names().first()
        .ok_or("No sheets found in xlsx")?.clone();
    let range = workbook.worksheet_range(&sheet_name)?;

    let file = File::create(csv_path)?;
    let mut writer = BufWriter::new(file);

    for row in range.rows() {
        let fields: Vec<String> = row.iter().enumerate().map(|(col_idx, cell)| {
            let s = match cell {
                calamine::Data::Empty => String::new(),
                calamine::Data::String(s) => s.clone(),
                calamine::Data::Float(f) => {
                    if *f == (*f as i64) as f64 {
                        let i = *f as i64;
                        if i > 365 && i < 73050 && (col_idx == 7 || col_idx == 8 || col_idx == 9) {
                            excel_serial_to_date_str(*f).unwrap_or_else(|| format!("{}", i))
                        } else {
                            format!("{}", i)
                        }
                    } else {
                        format!("{}", f)
                    }
                }
                calamine::Data::Int(i) => {
                    if *i > 365 && *i < 73050 && (col_idx == 7 || col_idx == 8 || col_idx == 9) {
                        excel_serial_to_date_str(*i as f64).unwrap_or_else(|| format!("{}", i))
                    } else {
                        format!("{}", i)
                    }
                }
                calamine::Data::Bool(b) => format!("{}", b),
                calamine::Data::DateTime(dt) => {
                    let serial_str = format!("{}", dt);
                    if let Ok(serial) = serial_str.parse::<f64>() {
                        excel_serial_to_date_str(serial).unwrap_or(serial_str)
                    } else {
                        serial_str
                    }
                }
                calamine::Data::DateTimeIso(s) => s.clone(),
                calamine::Data::DurationIso(s) => s.clone(),
                calamine::Data::Error(e) => format!("{:?}", e),
            };
            csv_escape(&s)
        }).collect();
        writeln!(writer, "{}", fields.join(","))?;
    }
    writer.flush()?;
    println!("  Converted to CSV: {}", csv_path);
    Ok(())
}

// ─── Shared helpers (used by foph_diff module) ───────────────────────────────

pub fn get_file_mod_date(filename: &str) -> String {
    fs::metadata(filename).ok()
        .and_then(|m| m.modified().ok())
        .map(|t| {
            let dt: chrono::DateTime<chrono::Local> = t.into();
            format!("{:02}.{:02}.{}", dt.day(), dt.month(), dt.year())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

// ─── Run modes ───────────────────────────────────────────────────────────────

fn run_download() -> Result<(), Box<dyn std::error::Error>> {
    let today = Local::now().date_naive();
    let date_str = format!("{:02}.{:02}.{}", today.day(), today.month(), today.year());

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()?;

    fs::create_dir_all("csv")?;
    fs::create_dir_all("ndjson")?;

    let swissmedic_csv = format!("csv/swissmedic_{}.csv", date_str);
    let xlsx_bytes = download_url(&client, SWISSMEDIC_URL)?;
    xlsx_to_csv(&xlsx_bytes, &swissmedic_csv)?;

    let foph_ndjson = format!("ndjson/sl_foph_{}.ndjson", date_str);
    let ndjson_bytes = download_url(&client, FOPH_SL_URL)?;
    File::create(&foph_ndjson)?.write_all(&ndjson_bytes)?;
    println!("  Saved: {}", foph_ndjson);

    println!("\nDownload completed:");
    println!("  {}", swissmedic_csv);
    println!("  {}", foph_ndjson);
    Ok(())
}

fn print_json_stats(label: &str, value: &Value) {
    if let Some(obj) = value.as_object() {
        println!("\n{}:", label);
        for (key, val) in obj {
            if let Some(arr) = val.as_array() {
                println!("  {}: {}", key, arr.len());
            }
        }
    }
}

fn run_merge(price_path: &str, swissmedic_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let today = Local::now().date_naive();
    let date_str = format!("{:02}.{:02}.{}", today.day(), today.month(), today.year());
    let output_path = format!("diff/med-drugs-update_{}.json", date_str);
    fs::create_dir_all("diff")?;

    let mut price_content = String::new();
    File::open(price_path)?.read_to_string(&mut price_content)?;
    let price_content = sanitize_json_string(&price_content);
    let price_value: Value = serde_json::from_str(&price_content)?;

    let mut swissmedic_content = String::new();
    File::open(swissmedic_path)?.read_to_string(&mut swissmedic_content)?;
    let swissmedic_content = sanitize_json_string(&swissmedic_content);
    let swissmedic_value: Value = serde_json::from_str(&swissmedic_content)?;

    // Print stats for both input files
    print_json_stats(price_path, &price_value);
    print_json_stats(swissmedic_path, &swissmedic_value);

    // Print flag-coded summary from both sources
    println!("\n=== Merged change summary (Ruby NUMERIC_FLAGS) ===");
    println!("{:<5} {:<25}: Count", "Flag", "Category");
    println!("--------------------------------------------------");

    let print_category_count = |flag: u8, label: &str, value: &Value, key: &str| {
        if let Some(arr) = value.get(key).and_then(|v| v.as_array()) {
            if !arr.is_empty() {
                println!("{:>3}   {:<25}: {}", flag, label, arr.len());
            }
        }
    };

    // FOPH/BSV price data
    println!("\n  Price data ({}):", price_path);
    print_category_count(1,  "new",              &price_value, "new");
    print_category_count(14, "del (delete)",     &price_value, "del");
    print_category_count(10, "sl_entry",         &price_value, "sl_entry");
    print_category_count(2,  "sl_entry_delete",  &price_value, "sl_entry_delete");
    print_category_count(3,  "name_base",        &price_value, "name_base");
    print_category_count(13, "retail_up",        &price_value, "retail_up");
    print_category_count(15, "retail_down",      &price_value, "retail_down");
    print_category_count(13, "exfactory_up",     &price_value, "exfactory_up");
    print_category_count(15, "exfactory_down",   &price_value, "exfactory_down");

    // Swissmedic data
    println!("\n  Swissmedic data ({}):", swissmedic_path);
    print_category_count(1,  "added (new)",            &swissmedic_value, "added");
    print_category_count(14, "deleted",                &swissmedic_value, "deleted");
    print_category_count(3,  "Name (name_base)",       &swissmedic_value, "Name");
    print_category_count(4,  "Owner (address)",        &swissmedic_value, "Owner");
    print_category_count(5,  "Categorie (ikscat)",     &swissmedic_value, "Swissmedic_Categorie");
    print_category_count(6,  "Active_Agent (comp)",    &swissmedic_value, "Active_Agent");
    print_category_count(6,  "Composition",            &swissmedic_value, "Composition");
    print_category_count(7,  "Indikation",             &swissmedic_value, "Indikation");
    print_category_count(8,  "Handelsform (sequence)", &swissmedic_value, "Handelsform");
    print_category_count(9,  "Date (expiry_date)",     &swissmedic_value, "Date");

    let mut root = Map::new();

    let mut metadata = Map::new();
    metadata.insert("generated_on".into(), Value::String(date_str.clone()));
    metadata.insert("price_source_file".into(), Value::String(price_path.to_string()));
    metadata.insert("swissmedic_source_file".into(), Value::String(swissmedic_path.to_string()));
    metadata.insert("output_filename".into(), Value::String(output_path.clone()));
    metadata.insert("note".into(), Value::String(
        "Simple file merge: the complete original JSON from both input files is nested unchanged under 'price_data' and 'swissmedic_data'. No processing, grouping, or modification of any objects — 100% preservation of all data.".to_string()
    ));
    root.insert("metadata".into(), Value::Object(metadata));
    root.insert("price_data".into(), price_value);
    root.insert("swissmedic_data".into(), swissmedic_value);

    let pretty_json = serde_json::to_string_pretty(&Value::Object(root))?;
    File::create(&output_path)?.write_all(pretty_json.as_bytes())?;

    println!("\nMerge completed → {}", output_path);

    Ok(())
}

// ─── Swissmedic CSV diff ─────────────────────────────────────────────────────

fn calculate_gtin_checksum(base12: &str) -> char {
    if base12.len() != 12 { return 'X'; }
    let sum: u32 = base12.chars().enumerate().map(|(i, c)| {
        let d = c.to_digit(10).unwrap_or(0);
        if i % 2 == 0 { d } else { d * 3 }
    }).sum();
    let checksum = (10 - (sum % 10)) % 10;
    std::char::from_digit(checksum, 10).unwrap_or('X')
}

fn build_gtin(reg_nr_raw: &str, pack_code_raw: &str) -> String {
    let reg_nr: String = reg_nr_raw.trim().chars().filter(|c| c.is_ascii_digit()).collect();
    let pack_code: String = pack_code_raw.trim().chars().filter(|c| c.is_ascii_digit()).collect();

    if reg_nr.is_empty() { return String::new(); }

    let reg_nr = format!("{:0>5}", &reg_nr[..reg_nr.len().min(5)]);
    let pack_code = if pack_code.is_empty() {
        "000".to_string()
    } else {
        format!("{:0>3}", &pack_code[..pack_code.len().min(3)])
    };

    let base12 = format!("7680{}{}", reg_nr, pack_code);
    format!("{}{}", base12, calculate_gtin_checksum(&base12))
}

fn extract_swissmedic_date(filename: &str) -> Option<String> {
    let stem = std::path::Path::new(filename)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    // Try Packungen-YYYY.MM.DD pattern
    if let Some(pos) = stem.find("Packungen-") {
        let date_part = &stem[pos + 10..];
        let segments: Vec<&str> = date_part.split('.').collect();
        if segments.len() == 3
            && segments[0].len() == 4
            && segments.iter().all(|s| s.chars().all(|c| c.is_ascii_digit()))
        {
            return Some(date_part.to_string());
        }
    }

    // Try dd.mm.yyyy pattern anywhere after '_'
    for part in stem.split('_') {
        let segments: Vec<&str> = part.split('.').collect();
        if segments.len() == 3
            && segments[0].len() <= 2
            && segments[1].len() <= 2
            && segments[2].len() == 4
            && segments.iter().all(|s| s.chars().all(|c| c.is_ascii_digit()))
        {
            return Some(part.to_string());
        }
    }

    None
}

#[derive(Clone, Debug)]
struct SwissmedicEntry {
    name: String,
    owner: String,
    date: String,
    handelsform: String,
    category: String,
    active_agent: String,
    composition: String,
    indication: String,
}

fn load_swissmedic_csv(filename: &str) -> Result<BTreeMap<String, SwissmedicEntry>, Box<dyn std::error::Error>> {
    let mut data = BTreeMap::new();
    let mut loaded = 0usize;
    let mut skipped = 0usize;
    let mut total = 0usize;

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(filename)?;

    for result in rdr.records() {
        let record = result?;
        total += 1;

        if record.len() < 11 {
            skipped += 1;
            continue;
        }

        let gtin = build_gtin(&record[0], &record[10]);
        if gtin.is_empty() || gtin.len() != 13 {
            skipped += 1;
            continue;
        }

        let get = |i: usize| -> String {
            record.get(i).unwrap_or("").trim().to_string()
        };

        data.insert(gtin, SwissmedicEntry {
            name: get(2),
            owner: get(3),
            date: get(9),
            handelsform: get(12),
            category: get(13),
            active_agent: get(16),
            composition: get(17),
            indication: get(19),
        });
        loaded += 1;
    }

    println!("{}: {} packs loaded ({} skipped, {} total lines)", filename, loaded, skipped, total);
    Ok(data)
}

fn run_swissmedic_diff(old_file: &str, new_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let old_date = extract_swissmedic_date(old_file)
        .ok_or("Could not extract date from old filename")?;
    let new_date = extract_swissmedic_date(new_file)
        .ok_or("Could not extract date from new filename")?;

    println!("Old date: {}, New date: {}", old_date, new_date);

    let old_data = load_swissmedic_csv(old_file)?;
    let new_data = load_swissmedic_csv(new_file)?;

    println!("=== Starting comparison between {} and {} ===\n", old_date, new_date);

    let mut added: Vec<Value> = Vec::new();
    let mut deleted: Vec<Value> = Vec::new();

    for (gtin, entry) in &new_data {
        if !old_data.contains_key(gtin) {
            let full_name = format!("{} {}", entry.name, entry.owner).trim().to_string();
            added.push(json!({"gtin": gtin, "name": full_name, "flags": [swissmedic_flags::NEW]}));
        }
    }
    for (gtin, entry) in &old_data {
        if !new_data.contains_key(gtin) {
            let full_name = format!("{} {}", entry.name, entry.owner).trim().to_string();
            deleted.push(json!({"gtin": gtin, "name": full_name, "flags": [swissmedic_flags::DELETE]}));
        }
    }

    type ChangeVec = Vec<Value>;
    let mut changes_name: ChangeVec = Vec::new();
    let mut changes_owner: ChangeVec = Vec::new();
    let mut changes_date: ChangeVec = Vec::new();
    let mut changes_handelsform: ChangeVec = Vec::new();
    let mut changes_category: ChangeVec = Vec::new();
    let mut changes_agent: ChangeVec = Vec::new();
    let mut changes_composition: ChangeVec = Vec::new();
    let mut changes_indication: ChangeVec = Vec::new();

    let make_change = |gtin: &str, product_name: &str, old_val: &str, new_val: &str, flags: Vec<u8>| -> Value {
        json!({
            "gtin": gtin,
            "product_name": product_name,
            "old": old_val,
            "new": new_val,
            "flags": flags,
        })
    };

    // Normalize line endings for comparison
    let normalize = |s: &str| -> String {
        s.replace("\r\n", "\n").replace('\r', "\n")
    };

    let fields_equal = |a: &str, b: &str| -> bool {
        normalize(a) == normalize(b)
    };

    for (gtin, old_entry) in &old_data {
        if let Some(new_entry) = new_data.get(gtin) {
            let pname = &new_entry.name;
            if !fields_equal(&old_entry.name, &new_entry.name) {
                changes_name.push(make_change(gtin, pname, &old_entry.name, &new_entry.name, vec![swissmedic_flags::NAME_BASE]));
            }
            if !fields_equal(&old_entry.owner, &new_entry.owner) {
                changes_owner.push(make_change(gtin, pname, &old_entry.owner, &new_entry.owner, vec![swissmedic_flags::ADDRESS]));
            }
            if !fields_equal(&old_entry.date, &new_entry.date) {
                changes_date.push(make_change(gtin, pname, &old_entry.date, &new_entry.date, vec![swissmedic_flags::EXPIRY_DATE]));
            }
            if !fields_equal(&old_entry.handelsform, &new_entry.handelsform) {
                changes_handelsform.push(make_change(gtin, pname, &old_entry.handelsform, &new_entry.handelsform, vec![swissmedic_flags::SEQUENCE]));
            }
            if !fields_equal(&old_entry.category, &new_entry.category) {
                changes_category.push(make_change(gtin, pname, &old_entry.category, &new_entry.category, vec![swissmedic_flags::IKSCAT]));
            }
            if !fields_equal(&old_entry.active_agent, &new_entry.active_agent) {
                changes_agent.push(make_change(gtin, pname, &old_entry.active_agent, &new_entry.active_agent, vec![swissmedic_flags::COMPOSITION]));
            }
            if !fields_equal(&old_entry.composition, &new_entry.composition) {
                changes_composition.push(make_change(gtin, pname, &old_entry.composition, &new_entry.composition, vec![swissmedic_flags::COMPOSITION]));
            }
            if !fields_equal(&old_entry.indication, &new_entry.indication) {
                changes_indication.push(make_change(gtin, pname, &old_entry.indication, &new_entry.indication, vec![swissmedic_flags::INDICATION]));
            }
        }
    }

    let mut output = Map::new();

    // Include numeric flag legend for downstream consumers (matching Ruby NUMERIC_FLAGS)
    let legend = json!({
        "1":  "new",
        "2":  "sl_entry_delete",
        "3":  "name_base",
        "4":  "address",
        "5":  "ikscat",
        "6":  "composition",
        "7":  "indication",
        "8":  "sequence",
        "9":  "expiry_date",
        "10": "sl_entry",
        "11": "price",
        "12": "comment",
        "13": "price_rise",
        "14": "delete",
        "15": "price_cut",
        "16": "not_specified"
    });
    output.insert("_flag_legend".into(), legend);

    output.insert("deleted".into(), Value::Array(deleted.clone()));
    output.insert("added".into(), Value::Array(added.clone()));
    output.insert("Name".into(), Value::Array(changes_name.clone()));
    output.insert("Owner".into(), Value::Array(changes_owner.clone()));
    output.insert("Date".into(), Value::Array(changes_date.clone()));
    output.insert("Handelsform".into(), Value::Array(changes_handelsform.clone()));
    output.insert("Swissmedic_Categorie".into(), Value::Array(changes_category.clone()));
    output.insert("Active_Agent".into(), Value::Array(changes_agent.clone()));
    output.insert("Composition".into(), Value::Array(changes_composition.clone()));
    output.insert("Indikation".into(), Value::Array(changes_indication.clone()));

    fs::create_dir_all("csv")?;
    let output_filename = format!("csv/diff_{}-{}.json", old_date, new_date);

    let pretty = serde_json::to_string_pretty(&Value::Object(output))?;
    File::create(&output_filename)?.write_all(pretty.as_bytes())?;

    // Terminal summary
    println!("Results summary:");
    println!("  Deleted: {} packs", deleted.len());
    println!("  Added:   {} packs\n", added.len());

    println!("Deleted packs:");
    for e in &deleted {
        println!("  {}  {}", e["gtin"].as_str().unwrap_or(""), e["name"].as_str().unwrap_or(""));
    }
    println!("\nAdded packs:");
    for e in &added {
        println!("  {}  {}", e["gtin"].as_str().unwrap_or(""), e["name"].as_str().unwrap_or(""));
    }

    let print_changes = |changes: &[Value], title: &str| {
        println!("\n{} ({} changes):", title, changes.len());
        for c in changes {
            println!("  {} [{}]: \"{}\" → \"{}\"",
                c["gtin"].as_str().unwrap_or(""),
                c["product_name"].as_str().unwrap_or(""),
                c["old"].as_str().unwrap_or(""),
                c["new"].as_str().unwrap_or(""),
            );
        }
    };

    print_changes(&changes_name, "Name");
    print_changes(&changes_owner, "Owner");
    print_changes(&changes_date, "Date");
    print_changes(&changes_handelsform, "Handelsform");
    print_changes(&changes_category, "Swissmedic Categorie");
    print_changes(&changes_agent, "Active Agent");
    print_changes(&changes_composition, "Composition");
    print_changes(&changes_indication, "Indikation");

    println!("\n=== Summary of changes per category (with Ruby NUMERIC_FLAGS) ===");
    println!("{:<5} {:<21}: Changes", "Flag", "Category");
    println!("----------------------------------------------");
    println!("{:<5} {:<21}: {} packs",  " 1",  "Added (new)",          added.len());
    println!("{:<5} {:<21}: {} packs",  "14",  "Deleted",              deleted.len());
    println!("{:<5} {:<21}: {} changes", " 3",  "Name",                changes_name.len());
    println!("{:<5} {:<21}: {} changes", " 4",  "Owner (address)",     changes_owner.len());
    println!("{:<5} {:<21}: {} changes", " 9",  "Date (expiry_date)",  changes_date.len());
    println!("{:<5} {:<21}: {} changes", " 8",  "Handelsform (seq)",   changes_handelsform.len());
    println!("{:<5} {:<21}: {} changes", " 5",  "Swissmedic Categorie", changes_category.len());
    println!("{:<5} {:<21}: {} changes", " 6",  "Active Agent (comp)", changes_agent.len());
    println!("{:<5} {:<21}: {} changes", " 6",  "Composition",         changes_composition.len());
    println!("{:<5} {:<21}: {} changes", " 7",  "Indikation",          changes_indication.len());

    println!("\nJSON output written to: {}", output_filename);
    Ok(())
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() == 2 && args[1] == "--download" {
        return run_download();
    }

    if args.len() == 4 && args[1] == "--foph-diff" {
        return foph_diff::run_foph_diff(&args[2], &args[3], None);
    }

    if args.len() == 5 && args[1] == "--foph-diff" {
        // --foph-diff --<category> <old> <new>
        let cat = args[2].trim_start_matches('-');
        return foph_diff::run_foph_diff(&args[3], &args[4], Some(cat));
    }

    if args.len() == 4 && args[1] == "--swissmedic-diff" {
        return run_swissmedic_diff(&args[2], &args[3]);
    }

    if args.len() == 3 && !args[1].starts_with('-') {
        return run_merge(&args[1], &args[2]);
    }

    eprintln!("Usage:");
    eprintln!("  {} --download", args[0]);
    eprintln!("    Download Swissmedic xlsx (→ CSV) and FOPH SL ndjson to current directory.");
    eprintln!();
    eprintln!("  {} --foph-diff <old.ndjson> <new.ndjson>", args[0]);
    eprintln!("    Compare two FOPH SL exports and output price/package diff as JSON.");
    eprintln!();
    eprintln!("  {} --foph-diff --<category> <old.ndjson> <new.ndjson>", args[0]);
    eprintln!("    Print GTINs for a category: new, del, retail_up, retail_down, exfactory_up, exfactory_down");
    eprintln!();
    eprintln!("  {} --swissmedic-diff <old.csv> <new.csv>", args[0]);
    eprintln!("    Compare two Swissmedic CSV exports and output package/field diff as JSON.");
    eprintln!();
    eprintln!("  {} <price_changes.json> <swissmedic_changes.json>", args[0]);
    eprintln!("    Merge two JSON files into 'diff/med-drugs-update_dd.mm.yyyy.json'.");
    std::process::exit(1);
}
