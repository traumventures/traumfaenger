//! Runs the parser over an export of stored exchanges and prints a coverage
//! report: what parsed, what did not and why, how often each field is filled,
//! and totals to compare with the provider's own numbers.
//!
//! ```text
//! traumfaenger-replay export.jsonl.gz [--show <reason|api|outcome>] [--dump <id>]
//! ```
//!
//! The input is JSON Lines of `GET /v1/exchanges/{id}` responses, optionally gzipped.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, Read},
    process::ExitCode,
};

use traumfaenger_api::{StoredExchange, Transport};
use traumfaenger_process::{PartKind, Side, process};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(path) = args.first() else {
        eprintln!("usage: traumfaenger-replay <export.jsonl[.gz]> [--show <reason|api|outcome>] [--dump <id>]");
        return ExitCode::from(2);
    };
    let option = |name: &str| args.iter().position(|arg| arg == name).and_then(|index| args.get(index + 1)).cloned();
    let show = option("--show");
    let dump = option("--dump");

    let file = File::open(path).unwrap_or_else(|error| panic!("{path}: {error}"));
    let reader: Box<dyn Read> =
        if path.ends_with(".gz") { Box::new(flate2::read::MultiGzDecoder::new(file)) } else { Box::new(file) };

    let mut count = 0u64;
    let mut by: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut fill: BTreeMap<&str, u64> = BTreeMap::new();
    let mut streams = 0u64;
    let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
    let mut contents: HashMap<[u8; 16], usize> = HashMap::new();
    let mut references = 0u64;
    let mut part_bytes = 0u64;
    let mut cost: BTreeMap<String, f64> = BTreeMap::new();
    let mut emitted: HashSet<String> = HashSet::new();
    let mut answered: HashSet<String> = HashSet::new();
    let mut tool_errors = 0u64;
    let mut samples: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for line in BufReader::new(reader).lines() {
        let line = line.expect("read");
        if line.trim().is_empty() {
            continue;
        }
        let exchange: StoredExchange = serde_json::from_str(&line).expect("an exchange per line");
        let call = process(&exchange);
        count += 1;
        if dump.as_deref() == Some(exchange.id.as_str()) {
            println!("{call:#?}");
        }

        let reason = call.parse.reason().unwrap_or("").to_owned();
        *by.entry((format!("{} {}", call.api.as_str(), call.parse.status()), reason.clone())).or_default() += 1;
        *by.entry(("outcome".into(), call.outcome.as_str().into())).or_default() += 1;
        *by.entry(("finish".into(), call.finish_reason.unwrap_or("-").into())).or_default() += 1;
        *by.entry(("cost_source".into(), call.cost_source.as_str().into())).or_default() += 1;
        let key = match show.as_deref() {
            Some("api") => Some(call.api.as_str().to_owned()),
            Some("outcome") => Some(call.outcome.as_str().to_owned()),
            Some(wanted) if wanted == reason => Some(reason.clone()),
            _ => None,
        };
        if let Some(key) = key {
            let list = samples.entry(key).or_default();
            if list.len() < 5 {
                list.push(format!("{} {} {:?}", exchange.id, exchange.operation, call.error));
            }
        }

        for (name, filled) in [
            ("model", call.model.is_some()),
            ("provider", call.provider.is_some()),
            ("usage.input", call.usage.input.is_some()),
            ("usage.cached", call.usage.cached.is_some()),
            ("usage.cache_write", call.usage.cache_write.is_some()),
            ("usage.output", call.usage.output.is_some()),
            ("usage.reasoning", call.usage.reasoning.is_some()),
            ("cost", call.cost_usd.is_some()),
            ("finish_reason", call.finish_reason.is_some()),
            ("tools", call.tools.is_some()),
            ("response parts", call.parts.iter().any(|part| part.side == Side::Response)),
        ] {
            *fill.entry(name).or_default() += u64::from(filled);
        }
        if exchange.transport == Transport::Sse {
            streams += 1;
            *fill.entry("ttft (streams)").or_default() += u64::from(call.ttft_ms.is_some());
        }

        *cost.entry(exchange.operation.clone()).or_default() += call.cost_usd.unwrap_or(0.0);
        for part in &call.parts {
            *kinds.entry(format!("{} {} {}", part.side.as_str(), part.role, part.kind.as_str())).or_default() += 1;
            references += 1;
            part_bytes += part.content.text.len() as u64;
            contents.insert(part.content.hash, part.content.text.len());
            if part.kind == PartKind::ToolCall
                && part.side == Side::Response
                && let Some(id) = &part.tool_call_id
            {
                emitted.insert(id.clone());
            }
            if part.kind == PartKind::ToolResult
                && let Some(id) = &part.tool_call_id
                && answered.insert(id.clone())
                && part.is_error
            {
                tool_errors += 1;
            }
        }
        if let Some(tools) = &call.tools {
            references += 1;
            part_bytes += tools.text.len() as u64;
            contents.insert(tools.hash, tools.text.len());
        }
    }

    println!("exchanges {count}");
    println!("\n-- parse / outcome / finish / cost source");
    for ((group, reason), n) in &by {
        println!("{n:>7}  {group:<28} {reason}");
    }
    println!("\n-- fields filled");
    for (name, n) in &fill {
        let total = if name.starts_with("ttft") { streams } else { count };
        println!("{:>6.1}%  {name} ({n}/{total})", 100.0 * *n as f64 / total.max(1) as f64);
    }
    println!("\n-- parts");
    for (kind, n) in &kinds {
        println!("{n:>7}  {kind}");
    }
    let unique: usize = contents.values().sum();
    println!(
        "\n-- contents: {references} references, {} unique; {:.1} MB referenced, {:.1} MB unique ({:.1}x)",
        contents.len(),
        part_bytes as f64 / 1e6,
        unique as f64 / 1e6,
        part_bytes as f64 / unique.max(1) as f64
    );
    println!(
        "-- tool calls: {} emitted, {} of them answered in a later request, {tool_errors} answered with an error",
        emitted.len(),
        emitted.intersection(&answered).count()
    );
    println!("\n-- cost (USD) by operation");
    let mut rows: Vec<_> = cost.into_iter().collect();
    rows.sort_by(|a, b| b.1.total_cmp(&a.1));
    let total: f64 = rows.iter().map(|row| row.1).sum();
    for (operation, usd) in rows {
        println!("{usd:>10.4}  {operation}");
    }
    println!("{total:>10.4}  total");
    for (key, list) in samples {
        println!("\n-- samples: {key}");
        for sample in list {
            println!("  {sample}");
        }
    }
    ExitCode::SUCCESS
}
