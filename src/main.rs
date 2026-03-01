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
const FOPH_RESOURCES_URL: &str = "https://epl.bag.admin.ch/api/sl/public/resources/current";
const FOPH_STATIC_BASE: &str = "https://epl.bag.admin.ch/static/";

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

fn resolve_foph_ndjson_url(client: &Client) -> Result<String, Box<dyn std::error::Error>> {
    println!("Fetching latest FOPH resource index from: {}", FOPH_RESOURCES_URL);
    let response = client.get(FOPH_RESOURCES_URL).send()?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("HTTP {} for {}", status, FOPH_RESOURCES_URL).into());
    }
    let body = response.text()?;
    let json: Value = serde_json::from_str(&body)?;
    let file_url = json.get("fhir")
        .and_then(|f: &Value| f.get("fileUrl"))
        .and_then(|v: &Value| v.as_str())
        .ok_or("Could not find fhir.fileUrl in API response")?;
    let full_url = format!("{}{}", FOPH_STATIC_BASE, file_url);
    println!("  Latest FOPH NDJSON: {}", full_url);
    Ok(full_url)
}

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

fn run_download(swissmedic: bool, fhir: bool) -> Result<(), Box<dyn std::error::Error>> {
    let today = Local::now().date_naive();
    let date_str = format!("{:02}.{:02}.{}", today.day(), today.month(), today.year());

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()?;

    if swissmedic {
        fs::create_dir_all("csv")?;
        let swissmedic_csv = format!("csv/swissmedic_{}.csv", date_str);
        let xlsx_bytes = download_url(&client, SWISSMEDIC_URL)?;
        xlsx_to_csv(&xlsx_bytes, &swissmedic_csv)?;
        println!("\nDownload completed:");
        println!("  {}", swissmedic_csv);
    }

    if fhir {
        fs::create_dir_all("ndjson")?;
        let foph_ndjson = format!("ndjson/sl_foph_{}.ndjson", date_str);
        let foph_url = resolve_foph_ndjson_url(&client)?;
        let ndjson_bytes = download_url(&client, &foph_url)?;
        File::create(&foph_ndjson)?.write_all(&ndjson_bytes)?;
        println!("\nDownload completed:");
        println!("  {}", foph_ndjson);
    }

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

fn run_merge(price_path: &str, swissmedic_path: &str, html: bool) -> Result<(), Box<dyn std::error::Error>> {
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

    let pretty_json = serde_json::to_string_pretty(&Value::Object(root.clone()))?;
    File::create(&output_path)?.write_all(pretty_json.as_bytes())?;

    println!("\nMerge completed → {}", output_path);

    if html {
        let html_path = output_path.replace(".json", ".html");
        generate_html_diff(&Value::Object(root), &html_path)?;
        println!("HTML output  → {}", html_path);
    }

    Ok(())
}

// ─── HTML diff output ───────────────────────────────────────────────────────

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn generate_html_diff(merged: &Value, html_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let meta = merged.get("metadata");
    let generated_on = meta.and_then(|m| m["generated_on"].as_str()).unwrap_or("unknown");

    let mut html = String::with_capacity(64 * 1024);
    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    html.push_str("<title>Pharma Diff Report – ");
    html.push_str(&html_escape(generated_on));
    html.push_str("</title>\n<style>\n");
    html.push_str(r#"
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; margin: 2em; color: #24292e; background: #fff; }
h1 { border-bottom: 2px solid #e1e4e8; padding-bottom: .3em; }
h2 { margin-top: 2em; color: #0366d6; }
h3 { margin-top: 1.5em; }
table { border-collapse: collapse; width: 100%; margin: .5em 0 1.5em; font-size: 0.92em; }
th, td { border: 1px solid #d1d5da; padding: 6px 10px; text-align: left; vertical-align: top; }
th { background: #f6f8fa; font-weight: 600; }
.added { background: #e6ffec; }
.deleted { background: #ffeef0; }
.old { color: #b31d28; text-decoration: line-through; }
.new { color: #22863a; font-weight: 500; }
.gtin { font-family: monospace; white-space: nowrap; }
.summary-table td:last-child { text-align: right; font-weight: 600; }
.price-up { color: #b31d28; }
.price-down { color: #22863a; }
.toc { background: #f6f8fa; padding: 1em 1.5em; border-radius: 6px; margin-bottom: 2em; }
.toc a { text-decoration: none; color: #0366d6; }
.toc ul { margin: .3em 0; padding-left: 1.5em; }
"#);
    html.push_str("</style>\n</head>\n<body>\n");

    // Header
    html.push_str(&format!("<h1>Pharma Diff Report – {}</h1>\n", html_escape(generated_on)));

    // Helper: render a simple added/deleted table
    let render_add_del_table = |html: &mut String, items: &[Value], css_class: &str, show_prices: bool| {
        html.push_str("<table>\n<tr><th>GTIN</th><th>Name</th>");
        if show_prices {
            html.push_str("<th>Retail</th><th>Ex-factory</th>");
        }
        html.push_str("</tr>\n");
        for item in items {
            let gtin = item["gtin"].as_str().unwrap_or("");
            let name = item["name"].as_str().unwrap_or("");
            html.push_str(&format!("<tr class=\"{}\"><td class=\"gtin\">{}</td><td>{}</td>",
                css_class, html_escape(gtin), html_escape(name)));
            if show_prices {
                let retail = item.get("retail_price").and_then(|v| v.as_f64());
                let exf = item.get("exfactory_price").and_then(|v| v.as_f64());
                html.push_str(&format!("<td>{}</td><td>{}</td>",
                    retail.map(|p| format!("{:.2}", p)).unwrap_or_default(),
                    exf.map(|p| format!("{:.2}", p)).unwrap_or_default(),
                ));
            }
            html.push_str("</tr>\n");
        }
        html.push_str("</table>\n");
    };

    // Helper: render a field-change table (old→new)
    let render_change_table = |html: &mut String, items: &[Value], old_key: &str, new_key: &str| {
        html.push_str("<table>\n<tr><th>GTIN</th><th>Name</th><th>Old</th><th>New</th></tr>\n");
        for item in items {
            let gtin = item["gtin"].as_str().unwrap_or("");
            let name = item["name"].as_str()
                .or_else(|| item["product_name"].as_str())
                .unwrap_or("");
            let old_v = item[old_key].as_str().unwrap_or("");
            let new_v = item[new_key].as_str().unwrap_or("");
            html.push_str(&format!(
                "<tr><td class=\"gtin\">{}</td><td>{}</td><td class=\"old\">{}</td><td class=\"new\">{}</td></tr>\n",
                html_escape(gtin), html_escape(name), html_escape(old_v), html_escape(new_v)
            ));
        }
        html.push_str("</table>\n");
    };

    // Helper: render price-change table
    let render_price_table = |html: &mut String, items: &[Value], direction: &str| {
        let css = if direction == "up" { "price-up" } else { "price-down" };
        html.push_str("<table>\n<tr><th>GTIN</th><th>Name</th><th>Type</th><th>Old Price</th><th>New Price</th><th>Difference</th></tr>\n");
        for item in items {
            let gtin = item["gtin"].as_str().unwrap_or("");
            let name = item["name"].as_str().unwrap_or("");
            let ptype = item["type"].as_str().unwrap_or("");
            let old_p = item["old_price"].as_f64();
            let new_p = item["new_price"].as_f64();
            let diff = item["difference"].as_f64().unwrap_or(0.0);
            html.push_str(&format!(
                "<tr><td class=\"gtin\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{:+.2}</td></tr>\n",
                html_escape(gtin), html_escape(name), ptype,
                old_p.map(|p| format!("{:.2}", p)).unwrap_or_default(),
                new_p.map(|p| format!("{:.2}", p)).unwrap_or_default(),
                css, diff
            ));
        }
        html.push_str("</table>\n");
    };

    // ── Table of Contents ────────────────────────────────────────────────
    html.push_str("<div class=\"toc\"><strong>Contents</strong>\n<ul>\n");
    html.push_str("<li><a href=\"#summary\">Summary</a></li>\n");
    html.push_str("<li><a href=\"#foph\">FOPH / BAG Price Data</a></li>\n");
    html.push_str("<li><a href=\"#swissmedic\">Swissmedic Data</a></li>\n");
    html.push_str("</ul></div>\n");

    // ── Summary table ────────────────────────────────────────────────────
    let price_data = merged.get("price_data");
    let sm_data = merged.get("swissmedic_data");

    let count = |data: Option<&Value>, key: &str| -> usize {
        data.and_then(|d| d.get(key)).and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)
    };

    html.push_str("<h2 id=\"summary\">Summary</h2>\n");
    html.push_str("<table class=\"summary-table\">\n<tr><th>Flag</th><th>Category</th><th>Source</th><th>Count</th></tr>\n");

    let summary_rows: Vec<(&str, &str, &str, usize)> = vec![
        ("1",  "New packages",        "FOPH",       count(price_data, "new")),
        ("14", "Deleted packages",     "FOPH",       count(price_data, "del")),
        ("10", "SL entry additions",   "FOPH",       count(price_data, "sl_entry")),
        ("2",  "SL entry deletions",   "FOPH",       count(price_data, "sl_entry_delete")),
        ("3",  "Name changes",         "FOPH",       count(price_data, "name_base")),
        ("13", "Retail price ↑",       "FOPH",       count(price_data, "retail_up")),
        ("15", "Retail price ↓",       "FOPH",       count(price_data, "retail_down")),
        ("13", "Ex-factory price ↑",   "FOPH",       count(price_data, "exfactory_up")),
        ("15", "Ex-factory price ↓",   "FOPH",       count(price_data, "exfactory_down")),
        ("1",  "Added packs",          "Swissmedic", count(sm_data, "added")),
        ("14", "Deleted packs",        "Swissmedic", count(sm_data, "deleted")),
        ("3",  "Name",                 "Swissmedic", count(sm_data, "Name")),
        ("4",  "Owner",                "Swissmedic", count(sm_data, "Owner")),
        ("9",  "Date",                 "Swissmedic", count(sm_data, "Date")),
        ("8",  "Handelsform",          "Swissmedic", count(sm_data, "Handelsform")),
        ("5",  "Swissmedic Categorie", "Swissmedic", count(sm_data, "Swissmedic_Categorie")),
        ("6",  "Active Agent",         "Swissmedic", count(sm_data, "Active_Agent")),
        ("6",  "Composition",          "Swissmedic", count(sm_data, "Composition")),
        ("7",  "Indikation",           "Swissmedic", count(sm_data, "Indikation")),
    ];

    for (flag, cat, source, n) in &summary_rows {
        if *n > 0 {
            html.push_str(&format!("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                flag, cat, source, n));
        }
    }
    html.push_str("</table>\n");

    // ── FOPH Price Data ──────────────────────────────────────────────────
    html.push_str("<h2 id=\"foph\">FOPH / BAG Price Data</h2>\n");

    if let Some(pd) = price_data {
        let arr = |key: &str| -> &[Value] {
            pd.get(key).and_then(|v| v.as_array()).map(|a| a.as_slice()).unwrap_or(&[])
        };

        let new_pkgs = arr("new");
        if !new_pkgs.is_empty() {
            html.push_str(&format!("<h3>New packages ({})</h3>\n", new_pkgs.len()));
            render_add_del_table(&mut html, new_pkgs, "added", true);
        }

        let del_pkgs = arr("del");
        if !del_pkgs.is_empty() {
            html.push_str(&format!("<h3>Deleted packages ({})</h3>\n", del_pkgs.len()));
            render_add_del_table(&mut html, del_pkgs, "deleted", true);
        }

        let sl_add = arr("sl_entry");
        if !sl_add.is_empty() {
            html.push_str(&format!("<h3>SL entry additions ({})</h3>\n", sl_add.len()));
            render_add_del_table(&mut html, sl_add, "added", false);
        }

        let sl_del = arr("sl_entry_delete");
        if !sl_del.is_empty() {
            html.push_str(&format!("<h3>SL entry deletions ({})</h3>\n", sl_del.len()));
            render_add_del_table(&mut html, sl_del, "deleted", false);
        }

        let names = arr("name_base");
        if !names.is_empty() {
            html.push_str(&format!("<h3>Name changes ({})</h3>\n", names.len()));
            render_change_table(&mut html, names, "old_name", "new_name");
        }

        let ru = arr("retail_up");
        if !ru.is_empty() {
            html.push_str(&format!("<h3>Retail price increases ({})</h3>\n", ru.len()));
            render_price_table(&mut html, ru, "up");
        }

        let rd = arr("retail_down");
        if !rd.is_empty() {
            html.push_str(&format!("<h3>Retail price decreases ({})</h3>\n", rd.len()));
            render_price_table(&mut html, rd, "down");
        }

        let eu = arr("exfactory_up");
        if !eu.is_empty() {
            html.push_str(&format!("<h3>Ex-factory price increases ({})</h3>\n", eu.len()));
            render_price_table(&mut html, eu, "up");
        }

        let ed = arr("exfactory_down");
        if !ed.is_empty() {
            html.push_str(&format!("<h3>Ex-factory price decreases ({})</h3>\n", ed.len()));
            render_price_table(&mut html, ed, "down");
        }
    }

    // ── Swissmedic Data ──────────────────────────────────────────────────
    html.push_str("<h2 id=\"swissmedic\">Swissmedic Data</h2>\n");

    if let Some(sm) = sm_data {
        let arr = |key: &str| -> &[Value] {
            sm.get(key).and_then(|v| v.as_array()).map(|a| a.as_slice()).unwrap_or(&[])
        };

        let added = arr("added");
        if !added.is_empty() {
            html.push_str(&format!("<h3>Added packs ({})</h3>\n", added.len()));
            render_add_del_table(&mut html, added, "added", false);
        }

        let deleted = arr("deleted");
        if !deleted.is_empty() {
            html.push_str(&format!("<h3>Deleted packs ({})</h3>\n", deleted.len()));
            render_add_del_table(&mut html, deleted, "deleted", false);
        }

        for (key, title) in [
            ("Name", "Name"),
            ("Owner", "Owner"),
            ("Date", "Date"),
            ("Handelsform", "Handelsform"),
            ("Swissmedic_Categorie", "Swissmedic Categorie"),
            ("Active_Agent", "Active Agent"),
            ("Composition", "Composition"),
            ("Indikation", "Indikation"),
        ] {
            let items = arr(key);
            if !items.is_empty() {
                html.push_str(&format!("<h3>{} changes ({})</h3>\n", title, items.len()));
                render_change_table(&mut html, items, "old", "new");
            }
        }
    }

    html.push_str("\n</body>\n</html>\n");
    File::create(html_path)?.write_all(html.as_bytes())?;
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

    if args.len() >= 2 && args[1] == "--download" {
        if args.len() == 2 {
            return run_download(true, true);
        }
        if args.len() == 3 && args[2] == "--fhir" {
            return run_download(false, true);
        }
        if args.len() == 3 && args[2] == "--swissmedic" {
            return run_download(true, false);
        }
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

    if args.len() == 4 && args[1] == "--html" && !args[2].starts_with('-') {
        return run_merge(&args[2], &args[3], true);
    }

    if args.len() == 3 && !args[1].starts_with('-') {
        return run_merge(&args[1], &args[2], false);
    }

    eprintln!("Usage:");
    eprintln!("  {} --download", args[0]);
    eprintln!("    Download both Swissmedic xlsx (→ CSV) and FOPH SL ndjson.");
    eprintln!();
    eprintln!("  {} --download --fhir", args[0]);
    eprintln!("    Download only the FOPH SL ndjson.");
    eprintln!();
    eprintln!("  {} --download --swissmedic", args[0]);
    eprintln!("    Download only the Swissmedic xlsx (→ CSV).");
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
    eprintln!();
    eprintln!("  {} --html <price_changes.json> <swissmedic_changes.json>", args[0]);
    eprintln!("    Same as above, plus generate an HTML report alongside the JSON.");
    std::process::exit(1);
}
