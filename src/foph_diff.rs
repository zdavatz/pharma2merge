use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};

use rayon::prelude::*;
use serde_json::{json, Map, Value};

// ─── Numeric flags (matching Ruby ODDB::OuwerkerkPlugin::NUMERIC_FLAGS) ─────

/// These numeric codes correspond 1:1 with the Ruby OuwerkerkPlugin:
///   new: 1, sl_entry_delete: 2, name_base/productname: 3, address: 4,
///   ikscat: 5, composition: 6, indication: 7, sequence: 8,
///   expiry_date: 9, sl_entry: 10, price: 11, comment: 12,
///   price_rise: 13, delete: 14, price_cut: 15, not_specified: 16
pub mod numeric_flags {
    #![allow(dead_code)]
    pub const NEW: u8              = 1;
    pub const SL_ENTRY_DELETE: u8  = 2;
    pub const NAME_BASE: u8        = 3;
    // pub const ADDRESS: u8       = 4;  // Swissmedic-side only (owner)
    // pub const IKSCAT: u8        = 5;  // Swissmedic-side only
    // pub const COMPOSITION: u8   = 6;  // Swissmedic-side only
    // pub const INDICATION: u8    = 7;  // Swissmedic-side only
    // pub const SEQUENCE: u8      = 8;  // Swissmedic-side only
    // pub const EXPIRY_DATE: u8   = 9;  // Swissmedic-side only
    pub const SL_ENTRY: u8         = 10;
    pub const PRICE: u8            = 11;
    pub const PRICE_RISE: u8       = 13;
    pub const DELETE: u8           = 14;
    pub const PRICE_CUT: u8        = 15;
    pub const NOT_SPECIFIED: u8    = 16;
}

// ─── Types ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct PackageInfo {
    pub name: String,
    pub retail_price: f64,
    pub exfactory_price: f64,
    pub has_sl_entry: bool,
}

pub type DateTuple = (i32, i32, i32); // (year, month, day)
pub type PackageMap = BTreeMap<String, PackageInfo>;

// ─── NDJSON reading ──────────────────────────────────────────────────────────

/// Read FOPH ndjson file: each line is a Bundle.
/// Also handles concatenated JSON (no newlines between objects) as fallback.
fn read_foph_bundles(filename: &str) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut content = String::new();
    std::fs::File::open(filename)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        .read_to_string(&mut content)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

    let mut bundles = Vec::new();

    // Try line-by-line NDJSON first
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        match serde_json::from_str::<Value>(line) {
            Ok(val) => {
                if val.get("resourceType").and_then(|v| v.as_str()) == Some("Bundle") {
                    bundles.push(val);
                }
            }
            Err(_) => {}
        }
    }

    // Fallback: if no bundles found via line-by-line, try concatenated JSON splitting
    if bundles.is_empty() {
        content.retain(|c| c != '\n' && c != '\r');
        let mut depth = 0i32;
        let mut in_string = false;
        let mut escape = false;
        let mut start = None;

        for (i, ch) in content.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            if in_string {
                match ch {
                    '\\' => escape = true,
                    '"' => in_string = false,
                    _ => {}
                }
                continue;
            }
            match ch {
                '"' => in_string = true,
                '{' => {
                    if depth == 0 { start = Some(i); }
                    depth += 1;
                }
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        if let Some(s) = start {
                            let obj_str = &content[s..=i];
                            if let Ok(val) = serde_json::from_str::<Value>(obj_str) {
                                if val.get("resourceType").and_then(|v| v.as_str()) == Some("Bundle") {
                                    bundles.push(val);
                                }
                            }
                            start = None;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Count unique GTINs across all bundles
    let mut gtin_count = std::collections::HashSet::new();
    for bundle in &bundles {
        if let Some(entries) = bundle.get("entry").and_then(|v| v.as_array()) {
            for entry in entries {
                if let Some(res) = entry.get("resource") {
                    if res.get("resourceType").and_then(|v| v.as_str()) == Some("PackagedProductDefinition") {
                        if let Some(ids) = res.get("packaging")
                            .and_then(|p| p.get("identifier"))
                            .and_then(|ids| ids.as_array())
                        {
                            for id in ids {
                                if let Some(val) = id.get("value").and_then(|v| v.as_str()) {
                                    if val.len() == 13 && val.starts_with("7680") {
                                        gtin_count.insert(val.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("Loaded {} bundles, {} packages from {}", bundles.len(), gtin_count.len(), filename);
    if bundles.is_empty() {
        return Err(format!("No valid FHIR Bundles in {}", filename).into());
    }
    Ok(bundles)
}

// ─── Date helpers ────────────────────────────────────────────────────────────

pub fn parse_date_str(d: &str) -> Option<DateTuple> {
    if d.len() < 10 { return None; }
    let y = d[0..4].parse().ok()?;
    let m = d[5..7].parse().ok()?;
    let day = d[8..10].parse().ok()?;
    Some((y, m, day))
}

pub fn extract_date_from_bundles(bundles: &[Value], fallback: DateTuple) -> DateTuple {
    let mut date_counts: BTreeMap<DateTuple, usize> = BTreeMap::new();

    for bundle in bundles {
        let timestamp = bundle.get("timestamp").and_then(|v| v.as_str())
            .or_else(|| bundle.get("meta")
                .and_then(|m| m.get("lastUpdated"))
                .and_then(|v| v.as_str()));

        if let Some(ts) = timestamp {
            if let Some(dt) = parse_date_str(ts) {
                *date_counts.entry(dt).or_default() += 1;
            }
        }
    }

    if date_counts.is_empty() {
        println!("Info: No bundle timestamp found, using fallback date.");
        return fallback;
    }

    let most_common = date_counts.iter().max_by_key(|(_, count)| *count).unwrap();
    let (y, m, d) = most_common.0;
    println!("Using bundle effective date: {}.{}.{} for price evaluation.", d, m, y);
    *most_common.0
}

// ─── Price extraction logic ──────────────────────────────────────────────────

fn get_effective_price(prices: &BTreeMap<DateTuple, f64>, current: &DateTuple) -> f64 {
    let mut best: Option<&DateTuple> = None;
    let mut price = 0.0;
    for (dt, p) in prices {
        if dt <= current && (best.is_none() || dt > best.unwrap()) {
            best = Some(dt);
            price = *p;
        }
    }
    price
}

pub fn process_bundles(bundles: &[Value], current_dt: &DateTuple) -> PackageMap {
    let mut packages = PackageMap::new();

    for bundle in bundles {
        let entries = match bundle.get("entry").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => continue,
        };

        // Build resource map: "ResourceType/id" -> resource
        let mut resources: BTreeMap<String, &Value> = BTreeMap::new();
        for entry in entries {
            let res = match entry.get("resource") {
                Some(r) => r,
                None => continue,
            };
            let rtype = res.get("resourceType").and_then(|v| v.as_str()).unwrap_or("");
            let id = res.get("id").and_then(|v| v.as_str()).unwrap_or("");
            if !rtype.is_empty() && !id.is_empty() {
                resources.insert(format!("{}/{}", rtype, id), res);
            }
        }

        // Find PackagedProductDefinition resources
        let ppd_keys: Vec<String> = resources.iter()
            .filter(|(_, r)| r.get("resourceType").and_then(|v| v.as_str()) == Some("PackagedProductDefinition"))
            .map(|(k, _)| k.clone())
            .collect();

        for ppd_key in &ppd_keys {
            let res = resources[ppd_key];

            // Extract GTIN
            let gtin = res.get("packaging")
                .and_then(|p| p.get("identifier"))
                .and_then(|ids| ids.as_array())
                .and_then(|ids| {
                    ids.iter().find_map(|id| {
                        let system = id.get("system").and_then(|v| v.as_str()).unwrap_or("");
                        let value = id.get("value").and_then(|v| v.as_str()).unwrap_or("");
                        if system == "urn:oid:2.51.1.1" && value.len() == 13 && value.starts_with("7680") {
                            Some(value.to_string())
                        } else {
                            None
                        }
                    })
                });

            let gtin = match gtin {
                Some(g) => g,
                None => continue,
            };

            // Extract name
            let name = res.get("description").and_then(|v| v.as_str())
                .or_else(|| res.get("text").and_then(|t| t.get("div")).and_then(|v| v.as_str()))
                .unwrap_or("Unknown Product")
                .to_string();

            // Collect prices and SL status from RegulatedAuthorization resources
            let mut price_by_type: BTreeMap<String, BTreeMap<DateTuple, f64>> = BTreeMap::new();
            let mut has_sl_entry = false;

            for (_, auth) in &resources {
                if auth.get("resourceType").and_then(|v| v.as_str()) != Some("RegulatedAuthorization") {
                    continue;
                }

                // Check if SL type
                let is_sl = auth.get("type")
                    .and_then(|t| t.get("coding"))
                    .and_then(|c| c.as_array())
                    .map(|codings| codings.iter().any(|c| {
                        c.get("code").and_then(|v| v.as_str()) == Some("756000002003")
                    }))
                    .unwrap_or(false);

                if !is_sl { continue; }

                // Check subject reference
                let subject_ref = auth.get("subject")
                    .and_then(|s| s.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|s| s.get("reference"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if subject_ref != ppd_key { continue; }

                // This package has an SL entry via RegulatedAuthorization
                has_sl_entry = true;

                // Extract price extensions
                let extensions = match auth.get("extension").and_then(|v| v.as_array()) {
                    Some(arr) => arr,
                    None => continue,
                };

                for ext in extensions {
                    let url = ext.get("url").and_then(|v| v.as_str()).unwrap_or("");
                    if !url.contains("productPrice") { continue; }

                    let sub_exts = match ext.get("extension").and_then(|v| v.as_array()) {
                        Some(arr) => arr,
                        None => continue,
                    };

                    let mut type_code = String::new();
                    let mut value = 0.0_f64;
                    let mut change_date = String::new();

                    for sub in sub_exts {
                        let sub_url = sub.get("url").and_then(|v| v.as_str()).unwrap_or("");
                        match sub_url {
                            "type" => {
                                type_code = sub.get("valueCodeableConcept")
                                    .and_then(|v| v.get("coding"))
                                    .and_then(|c| c.as_array())
                                    .and_then(|arr| arr.first())
                                    .and_then(|c| c.get("code"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                            }
                            "value" => {
                                value = sub.get("valueMoney")
                                    .and_then(|v| v.get("value"))
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0);
                            }
                            "changeDate" => {
                                change_date = sub.get("valueDate")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                            }
                            _ => {}
                        }
                    }

                    let price_type = match type_code.as_str() {
                        "756002005001" => "retail",
                        "756002005002" => "exfactory",
                        _ => continue,
                    };

                    if value > 0.0 && !change_date.is_empty() {
                        if let Some(dt) = parse_date_str(&change_date) {
                            price_by_type.entry(price_type.to_string())
                                .or_default()
                                .insert(dt, value);
                        }
                    }
                }
            }

            let retail = get_effective_price(
                price_by_type.get("retail").unwrap_or(&BTreeMap::new()),
                current_dt,
            );
            let exfactory = get_effective_price(
                price_by_type.get("exfactory").unwrap_or(&BTreeMap::new()),
                current_dt,
            );

            // Include packages even without prices if they have an SL entry,
            // so we can track SL status changes
            if retail > 0.0 || exfactory > 0.0 || has_sl_entry {
                packages.insert(gtin, PackageInfo {
                    name,
                    retail_price: retail,
                    exfactory_price: exfactory,
                    has_sl_entry,
                });
            }
        }
    }
    packages
}

// ─── Public entry point ──────────────────────────────────────────────────────

pub fn run_foph_diff(old_file: &str, new_file: &str, filter: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    // Extract date strings from input filenames
    let extract_date_from_filename = |path: &str| -> String {
        let stem = std::path::Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        for part in stem.split('_') {
            let segments: Vec<&str> = part.split('.').collect();
            if segments.len() == 3
                && segments[0].len() <= 2
                && segments[1].len() <= 2
                && segments[2].len() == 4
                && segments.iter().all(|s| s.chars().all(|c| c.is_ascii_digit()))
            {
                return part.to_string();
            }
        }
        crate::get_file_mod_date(path)
    };

    // Parse dd.mm.yyyy string to DateTuple
    let date_str_to_tuple = |s: &str| -> DateTuple {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 3 {
            let d: i32 = parts[0].parse().unwrap_or(1);
            let m: i32 = parts[1].parse().unwrap_or(1);
            let y: i32 = parts[2].parse().unwrap_or(2026);
            (y, m, d)
        } else {
            (2026, 1, 6)
        }
    };

    let old_date_str = extract_date_from_filename(old_file);
    let new_date_str = extract_date_from_filename(new_file);
    let old_fallback_dt = date_str_to_tuple(&old_date_str);
    let new_fallback_dt = date_str_to_tuple(&new_date_str);
    println!("Old date: {}", old_date_str);
    println!("New date: {}", new_date_str);

    // Load both files in parallel
    let old_file_owned = old_file.to_string();
    let new_file_owned = new_file.to_string();

    let (old_result, new_result) = rayon::join(
        || -> Result<(Vec<Value>, DateTuple), Box<dyn std::error::Error + Send + Sync>> {
            println!("Loading old file...");
            let bundles = read_foph_bundles(&old_file_owned)?;
            let effective_date = extract_date_from_bundles(&bundles, old_fallback_dt);
            Ok((bundles, effective_date))
        },
        || -> Result<(Vec<Value>, DateTuple), Box<dyn std::error::Error + Send + Sync>> {
            println!("Loading new file...");
            let bundles = read_foph_bundles(&new_file_owned)?;
            let effective_date = extract_date_from_bundles(&bundles, new_fallback_dt);
            Ok((bundles, effective_date))
        },
    );

    let (old_bundles, old_effective_date) = old_result.map_err(|e| -> Box<dyn std::error::Error> { e })?;
    let (new_bundles, new_effective_date) = new_result.map_err(|e| -> Box<dyn std::error::Error> { e })?;

    // Process bundles in parallel
    let (old_pkg, new_pkg) = rayon::join(
        || {
            let chunk_size = std::cmp::max(1, old_bundles.len() / rayon::current_num_threads());
            let results: Vec<PackageMap> = old_bundles.par_chunks(chunk_size)
                .map(|chunk| process_bundles(chunk, &old_effective_date))
                .collect();
            let mut m = PackageMap::new();
            for r in results { m.extend(r); }
            m
        },
        || {
            let chunk_size = std::cmp::max(1, new_bundles.len() / rayon::current_num_threads());
            let results: Vec<PackageMap> = new_bundles.par_chunks(chunk_size)
                .map(|chunk| process_bundles(chunk, &new_effective_date))
                .collect();
            let mut m = PackageMap::new();
            for r in results { m.extend(r); }
            m
        },
    );

    println!("Found {} packages (old), {} (new).", old_pkg.len(), new_pkg.len());

    // ── Compute all diff categories ──────────────────────────────────────────

    // 1. New packages (flag 1: new)
    let new_packages: Vec<Value> = new_pkg.par_iter()
        .filter(|(gtin, _)| !old_pkg.contains_key(*gtin))
        .map(|(gtin, info)| json!({
            "gtin": gtin,
            "name": info.name,
            "flags": [numeric_flags::NEW],
            "retail_price": if info.retail_price > 0.0 { json!(info.retail_price) } else { Value::Null },
            "exfactory_price": if info.exfactory_price > 0.0 { json!(info.exfactory_price) } else { Value::Null },
        }))
        .collect();

    // 14. Package deletions (flag 14: delete)
    let package_deletions: Vec<Value> = old_pkg.par_iter()
        .filter(|(gtin, _)| !new_pkg.contains_key(*gtin))
        .map(|(gtin, info)| json!({
            "gtin": gtin,
            "name": info.name,
            "flags": [numeric_flags::DELETE],
            "retail_price": if info.retail_price > 0.0 { json!(info.retail_price) } else { Value::Null },
            "exfactory_price": if info.exfactory_price > 0.0 { json!(info.exfactory_price) } else { Value::Null },
        }))
        .collect();

    // 10. SL entry additions (flag 10: sl_entry) — package exists in both but gained SL
    let sl_entry_additions: Vec<Value> = new_pkg.par_iter()
        .filter_map(|(gtin, new_info)| {
            old_pkg.get(gtin).and_then(|old_info| {
                if !old_info.has_sl_entry && new_info.has_sl_entry {
                    Some(json!({
                        "gtin": gtin,
                        "name": new_info.name,
                        "flags": [numeric_flags::SL_ENTRY],
                    }))
                } else {
                    None
                }
            })
        })
        .collect();

    // 2. SL entry deletions (flag 2: sl_entry_delete) — package exists in both but lost SL
    let sl_entry_deletions: Vec<Value> = new_pkg.par_iter()
        .filter_map(|(gtin, new_info)| {
            old_pkg.get(gtin).and_then(|old_info| {
                if old_info.has_sl_entry && !new_info.has_sl_entry {
                    Some(json!({
                        "gtin": gtin,
                        "name": new_info.name,
                        "flags": [numeric_flags::SL_ENTRY_DELETE],
                    }))
                } else {
                    None
                }
            })
        })
        .collect();

    // 3. Name changes (flag 3: name_base)
    let name_changes: Vec<Value> = new_pkg.par_iter()
        .filter_map(|(gtin, new_info)| {
            old_pkg.get(gtin).and_then(|old_info| {
                if old_info.name != new_info.name {
                    Some(json!({
                        "gtin": gtin,
                        "name": new_info.name,
                        "flags": [numeric_flags::NAME_BASE],
                        "old_name": old_info.name,
                        "new_name": new_info.name,
                    }))
                } else {
                    None
                }
            })
        })
        .collect();

    // 11/13/15. Price changes with directional flags
    let price_changes: Vec<Value> = new_pkg.par_iter()
        .filter_map(|(gtin, new_info)| {
            old_pkg.get(gtin).map(|old_info| {
                let mut changes = Vec::new();
                for (ptype, old_p, new_p) in [
                    ("retail", old_info.retail_price, new_info.retail_price),
                    ("exfactory", old_info.exfactory_price, new_info.exfactory_price),
                ] {
                    if (new_p - old_p).abs() > 0.001 {
                        let diff = new_p - old_p;
                        // flag 11 (price) always present, plus 13 (price_rise) or 15 (price_cut)
                        let flags = if diff > 0.0 {
                            vec![numeric_flags::PRICE, numeric_flags::PRICE_RISE]
                        } else {
                            vec![numeric_flags::PRICE, numeric_flags::PRICE_CUT]
                        };
                        changes.push(json!({
                            "gtin": gtin,
                            "name": new_info.name,
                            "flags": flags,
                            "type": ptype,
                            "old_price": if old_p > 0.0 { json!(old_p) } else { Value::Null },
                            "new_price": if new_p > 0.0 { json!(new_p) } else { Value::Null },
                            "difference": diff,
                        }));
                    }
                }
                changes
            })
        })
        .flatten()
        .collect();

    let mut retail_up = Vec::new();
    let mut retail_down = Vec::new();
    let mut exfactory_up = Vec::new();
    let mut exfactory_down = Vec::new();

    for change in price_changes {
        let ptype = change["type"].as_str().unwrap_or("");
        let diff = change["difference"].as_f64().unwrap_or(0.0);
        match (ptype, diff > 0.0) {
            ("retail", true) => retail_up.push(change),
            ("retail", false) => retail_down.push(change),
            ("exfactory", true) => exfactory_up.push(change),
            ("exfactory", false) => exfactory_down.push(change),
            _ => {}
        }
    }

    let n_new = new_packages.len();
    let n_del = package_deletions.len();
    let n_sl_add = sl_entry_additions.len();
    let n_sl_del = sl_entry_deletions.len();
    let n_name = name_changes.len();
    let n_ru = retail_up.len();
    let n_rd = retail_down.len();
    let n_eu = exfactory_up.len();
    let n_ed = exfactory_down.len();

    // If a filter is set, just print GTINs for that category and exit
    if let Some(cat) = filter {
        let items: &[Value] = match cat {
            "new" => &new_packages,
            "del" | "delete" => &package_deletions,
            "sl_entry" => &sl_entry_additions,
            "sl_entry_delete" => &sl_entry_deletions,
            "name" | "name_base" | "productname" => &name_changes,
            "retail_up" | "price_rise_retail" => &retail_up,
            "retail_down" | "price_cut_retail" => &retail_down,
            "exfactory_up" | "price_rise_exfactory" => &exfactory_up,
            "exfactory_down" | "price_cut_exfactory" => &exfactory_down,
            _ => {
                eprintln!("Unknown category '{}'.", cat);
                eprintln!("Valid: new, del, sl_entry, sl_entry_delete, name,");
                eprintln!("       retail_up, retail_down, exfactory_up, exfactory_down");
                std::process::exit(1);
            }
        };
        for item in items {
            if let Some(gtin) = item["gtin"].as_str() {
                println!("{}", gtin);
            }
        }
        return Ok(());
    }

    let mut output = Map::new();

    // Include numeric flag legend for downstream consumers
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

    output.insert("new".into(), Value::Array(new_packages));
    output.insert("del".into(), Value::Array(package_deletions));
    output.insert("sl_entry".into(), Value::Array(sl_entry_additions));
    output.insert("sl_entry_delete".into(), Value::Array(sl_entry_deletions));
    output.insert("name_base".into(), Value::Array(name_changes));
    output.insert("retail_up".into(), Value::Array(retail_up));
    output.insert("retail_down".into(), Value::Array(retail_down));
    output.insert("exfactory_up".into(), Value::Array(exfactory_up));
    output.insert("exfactory_down".into(), Value::Array(exfactory_down));

    fs::create_dir_all("ndjson")?;

    let output_filename = format!("ndjson/diff_{}-{}.json",
        if old_date_str == "unknown" { "old".to_string() } else { old_date_str },
        if new_date_str == "unknown" { "new".to_string() } else { new_date_str },
    );

    let pretty = serde_json::to_string_pretty(&Value::Object(output))?;
    std::fs::File::create(&output_filename)?.write_all(pretty.as_bytes())?;

    println!("Diff written to {}", output_filename);
    println!("  flag  1 new:              {}", n_new);
    println!("  flag 14 del:              {}", n_del);
    println!("  flag 10 sl_entry:         {}", n_sl_add);
    println!("  flag  2 sl_entry_delete:  {}", n_sl_del);
    println!("  flag  3 name_base:        {}", n_name);
    println!("  flag 13 retail_up:        {}", n_ru);
    println!("  flag 15 retail_down:      {}", n_rd);
    println!("  flag 13 exfactory_up:     {}", n_eu);
    println!("  flag 15 exfactory_down:   {}", n_ed);

    Ok(())
}
