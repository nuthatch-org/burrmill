//! #347: a statement over about thirty layered views plans in bounded time.
//!
//! `corpus/deep-views/network-capacity.jsonl` is what nuthatch registered for one statement of its
//! network contract (`capacity_uses_the_last_refresh_event_and_its_protocol_parameters`, block
//! 12): 56 event tables with their hot rows, its text functions, and the network nest's 33 views,
//! in order. `SELECT ... FROM indexer` over them was the contract's dearest plan.
//! `cargo test --release -p burrmill --features datafusion --test df_deepviews -- --nocapture`
//! prints the time.

use std::sync::Arc;
use std::time::Instant;

use burrmill::Engine;
use serde_json::Value;

const CATALOG: &str = include_str!("../../../corpus/deep-views/network-capacity.jsonl");
const STATEMENT: &str =
    "SELECT \"delegatedCapacity\", \"tokenCapacity\", \"availableStake\" FROM indexer";

/// Release builds answer [`STATEMENT`] under this. Debug builds only answer it.
const BOUND_MS: u128 = 2_000;

fn decimal(s: &str) -> Result<u128, String> {
    s.parse().map_err(|e| format!("{s:?}: {e}"))
}

/// Enough of nuthatch's own text functions to answer the contract's statement.
fn text_function(name: &str) -> burrmill::df::TextFunction {
    match name {
        "nuthatch_uint256" => Arc::new(|v: &[&str]| {
            let hex = v[0].trim_start_matches("0x").trim_start_matches('0');
            u128::from_str_radix(if hex.is_empty() { "0" } else { hex }, 16)
                .map(|n| n.to_string())
                .map_err(|e| e.to_string())
        }),
        "nuthatch_mul_div" => Arc::new(|v: &[&str]| {
            let d = decimal(v[2])?;
            if d == 0 {
                return Err("integer division by zero".into());
            }
            Ok((decimal(v[0])? * decimal(v[1])? / d).to_string())
        }),
        _ => Arc::new(|v: &[&str]| Ok(v[0].to_string())),
    }
}

fn engine() -> Engine {
    let mut e = Engine::open_empty().unwrap();
    for line in CATALOG.lines() {
        let op: Value = serde_json::from_str(line).unwrap();
        let name = op["name"].as_str().unwrap();
        match op["op"].as_str().unwrap() {
            "text_fn" => {
                let arity = op["arity"].as_u64().unwrap() as usize;
                e.register_text_function(name, arity, text_function(name));
            }
            "facts" => {
                let declared: Vec<(String, String)> =
                    serde_json::from_value(op["declared"].clone()).unwrap();
                let hot = op["hot"].as_array().unwrap();
                let window = (op["window"][0].as_u64(), op["window"][1].as_u64());
                e.register_facts(name, &declared, Vec::new(), hot, window)
                    .unwrap_or_else(|err| panic!("{name}: {err}"));
            }
            "rows" => e
                .register_rows(name, op["rows"].as_array().unwrap())
                .unwrap(),
            "view" => e
                .register_view(name, op["body"].as_str().unwrap())
                .unwrap_or_else(|err| panic!("{name}: {err}")),
            other => panic!("{other}: not in the capture"),
        }
    }
    e
}

fn answer(e: &Engine) -> Vec<Value> {
    e.sql(STATEMENT)
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

/// Over a dozen hot rows the statement's time is its plan's.
#[test]
fn the_network_nest_statement_answers_in_bounded_time() {
    let e = engine();
    let mut least = u128::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let rows = answer(&e);
        least = least.min(t.elapsed().as_millis());
        assert_eq!(
            rows,
            vec![
                serde_json::json!({"delegatedCapacity":"60","tokenCapacity":"85","availableStake":"65"})
            ]
        );
    }
    eprintln!("answered in {least} ms");
    if !cfg!(debug_assertions) {
        assert!(least < BOUND_MS, "{least} ms, past {BOUND_MS}");
    }
}
