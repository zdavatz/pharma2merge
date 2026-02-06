use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};

use rayon::prelude::*;
use serde_json::{json, Map, Value};

// ─── Types ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct PackageInfo {
    pub name: String,
    pub retail_price: f64,
    pub exfactory_price: f64,
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

            // Collect prices from RegulatedAuthorization resources
            let mut price_by_type: BTreeMap<String, BTreeMap<DateTuple, f64>> = BTreeMap::new();

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

            if retail > 0.0 || exfactory > 0.0 {
                packages.insert(gtin, PackageInfo { name, retail_price: retail, exfactory_price: exfactory });
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

    // Compute diff categories in parallel
    let new_packages: Vec<Value> = new_pkg.par_iter()
        .filter(|(gtin, _)| !old_pkg.contains_key(*gtin))
        .map(|(gtin, info)| json!({
            "gtin": gtin,
            "name": info.name,
            "retail_price": if info.retail_price > 0.0 { json!(info.retail_price) } else { Value::Null },
            "exfactory_price": if info.exfactory_price > 0.0 { json!(info.exfactory_price) } else { Value::Null },
        }))
        .collect();

    let package_deletions: Vec<Value> = old_pkg.par_iter()
        .filter(|(gtin, _)| !new_pkg.contains_key(*gtin))
        .map(|(gtin, info)| json!({
            "gtin": gtin,
            "name": info.name,
            "retail_price": if info.retail_price > 0.0 { json!(info.retail_price) } else { Value::Null },
            "exfactory_price": if info.exfactory_price > 0.0 { json!(info.exfactory_price) } else { Value::Null },
        }))
        .collect();

    // Price changes: collect in parallel, then partition
    let price_changes: Vec<Value> = new_pkg.par_iter()
        .filter_map(|(gtin, new_info)| {
            old_pkg.get(gtin).map(|old_info| {
                let mut changes = Vec::new();
                for (ptype, old_p, new_p) in [
                    ("retail", old_info.retail_price, new_info.retail_price),
                    ("exfactory", old_info.exfactory_price, new_info.exfactory_price),
                ] {
                    if (new_p - old_p).abs() > 0.001 {
                        changes.push(json!({
                            "gtin": gtin,
                            "name": new_info.name,
                            "type": ptype,
                            "old_price": if old_p > 0.0 { json!(old_p) } else { Value::Null },
                            "new_price": if new_p > 0.0 { json!(new_p) } else { Value::Null },
                            "difference": new_p - old_p,
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
    let n_ru = retail_up.len();
    let n_rd = retail_down.len();
    let n_eu = exfactory_up.len();
    let n_ed = exfactory_down.len();

    // If a filter is set, just print GTINs for that category and exit
    if let Some(cat) = filter {
        let items = match cat {
            "new" => &new_packages,
            "del" => &package_deletions,
            "retail_up" => &retail_up,
            "retail_down" => &retail_down,
            "exfactory_up" => &exfactory_up,
            "exfactory_down" => &exfactory_down,
            _ => {
                eprintln!("Unknown category '{}'. Valid: new, del, retail_up, retail_down, exfactory_up, exfactory_down", cat);
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
    output.insert("new".into(), Value::Array(new_packages));
    output.insert("del".into(), Value::Array(package_deletions));
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

    println!("Diff written to {} ({} new, {} del, {} retail_up, {} retail_down, {} exfactory_up, {} exfactory_down)",
        output_filename, n_new, n_del, n_ru, n_rd, n_eu, n_ed,
    );

    Ok(())
}
