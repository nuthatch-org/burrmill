//! `engine-views <nest>` (gate 1): every authored view of a real nest through `burrmill::Engine`
//! against DuckDB set up as nuthatch sets it up, and the nest's pinned checks.
//!
//! DuckDB's base layer is nuthatch's: `read_parquet([...], union_by_name=true)` per table, `_dec`
//! as `TRY_CAST(... AS DECIMAL(38,0))` and `_overflow` beside it, then the views in file order.
//! Each view's whole contents are compared as a multiset of rows in nuthatch's JSON encoding.

use std::path::Path;
use std::time::Instant;

use serde_json::Value;

use crate::df_views::{load_nest, Nest};

pub(crate) fn duck(nest: &Nest) -> anyhow::Result<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch(&format!(
        "SET threads TO {};",
        burrmill::Limits::default().max_threads
    ))?;
    for t in &nest.tables {
        let derived: String = t
            .wide
            .iter()
            .map(|c| {
                format!(
                    ", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\", \
                     (\"{c}\" IS NOT NULL AND TRY_CAST(\"{c}\" AS DECIMAL(38,0)) IS NULL) AS \"{c}_overflow\""
                )
            })
            .collect();
        if t.files.is_empty() {
            let cols: Vec<String> = t
                .schema
                .fields()
                .iter()
                .map(|f| {
                    let ty = if *f.data_type() == arrow::datatypes::DataType::UInt64 {
                        "UBIGINT"
                    } else {
                        "VARCHAR"
                    };
                    format!("\"{}\" {ty}", f.name())
                })
                .collect();
            conn.execute_batch(&format!(
                "CREATE TABLE \"{}__raw\" ({});",
                t.name,
                cols.join(", ")
            ))?;
            conn.execute_batch(&format!(
                "CREATE VIEW \"{}\" AS SELECT *{derived} FROM \"{}__raw\";",
                t.name, t.name
            ))?;
        } else {
            let list = t
                .files
                .iter()
                .map(|(p, _)| format!("'{}'", p.display()))
                .collect::<Vec<_>>()
                .join(",");
            conn.execute_batch(&format!(
                "CREATE VIEW \"{}\" AS SELECT *{derived} FROM read_parquet([{list}], union_by_name=true);",
                t.name
            ))?;
        }
    }
    Ok(conn)
}

fn sorted_rows(v: Value) -> Vec<String> {
    let mut rows: Vec<String> = match v {
        Value::Array(a) => a
            .iter()
            .map(|r| serde_json::to_string(r).unwrap())
            .collect(),
        _ => vec![],
    };
    rows.sort();
    rows
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(220).collect()
}

pub fn run(root: &str) -> anyhow::Result<()> {
    let root = Path::new(root);
    let nest = load_nest(root)?;
    let conn = duck(&nest)?;
    let mut duck_ok = Vec::new();
    for v in &nest.views {
        match conn.execute_batch(&v.text) {
            Ok(()) => duck_ok.push(true),
            Err(e) => {
                println!(
                    "DUCK-FAIL {} ({}): {}",
                    v.name,
                    v.file,
                    first_line(&e.to_string())
                );
                duck_ok.push(false);
            }
        }
    }

    let root_owned = root.to_path_buf();
    let views: Vec<(String, String)> = nest
        .views
        .iter()
        .map(|v| (v.name.clone(), v.body.clone()))
        .collect();
    let (engine, reg) = std::thread::spawn(move || -> anyhow::Result<_> {
        let mut e = burrmill::Engine::open_nest(&root_owned)?;
        let reg: Vec<Option<String>> = views
            .iter()
            .map(|(n, b)| {
                e.register_view(n, b)
                    .err()
                    .map(|e| first_line(&e.to_string()))
            })
            .collect();
        Ok((e, reg))
    })
    .join()
    .expect("engine thread")?;
    let engine = std::sync::Arc::new(engine);

    let (mut same, mut diff, mut fail) = (0, 0, 0);
    for (i, v) in nest.views.iter().enumerate() {
        if let Some(why) = &reg[i] {
            fail += 1;
            println!("REGISTER-FAIL {:<34} {}", v.name, why);
            continue;
        }
        if !duck_ok[i] {
            continue;
        }
        let sql = format!("SELECT * FROM \"{}\"", v.name);
        let t = Instant::now();
        let want = crate::encode_parity::nuthatch_rows(&conn, &sql).map(sorted_rows);
        let duck_ms = t.elapsed().as_millis();
        let e2 = std::sync::Arc::clone(&engine);
        let q = sql.clone();
        let t = Instant::now();
        let got = std::thread::spawn(move || -> Result<Vec<String>, String> {
            let batches = e2
                .sql(&q)
                .map_err(|e| first_line(&e.to_string().replace('\n', " | ")))?;
            let mut rows = Vec::new();
            for b in &batches {
                rows.extend(burrmill::df::encode::rows(b).map_err(|e| e.to_string())?);
            }
            Ok(sorted_rows(Value::Array(rows)))
        })
        .join()
        .expect("engine thread");
        let bm_ms = t.elapsed().as_millis();
        match (want, got) {
            (Ok(w), Ok(g)) if w == g => {
                same += 1;
                // Warm medians when asked: the parity run above is each side's first, cold, query.
                let (duck_ms, bm_ms) = match std::env::var("TIMING")
                    .ok()
                    .and_then(|t| t.parse::<usize>().ok())
                {
                    Some(n) if n > 0 => {
                        let median = |mut v: Vec<u128>| {
                            v.sort_unstable();
                            v[v.len() / 2]
                        };
                        let mut d = Vec::new();
                        for _ in 0..n {
                            let t = Instant::now();
                            let mut stmt = conn.prepare(&sql)?;
                            for b in stmt.query_arrow([])? {
                                std::hint::black_box(b.num_rows());
                            }
                            d.push(t.elapsed().as_millis());
                        }
                        let e3 = std::sync::Arc::clone(&engine);
                        let q = sql.clone();
                        let b = std::thread::spawn(move || {
                            (0..n)
                                .map(|_| {
                                    let t = Instant::now();
                                    let _ = e3.sql_for_each(&q, |b| {
                                        std::hint::black_box(b.num_rows());
                                        Ok(())
                                    });
                                    t.elapsed().as_millis()
                                })
                                .collect::<Vec<_>>()
                        })
                        .join()
                        .expect("engine thread");
                        if std::env::var("PROFILE").is_ok() {
                            let e4 = std::sync::Arc::clone(&engine);
                            let q = format!("EXPLAIN {sql}");
                            let plan_ms = std::thread::spawn(move || {
                                let mut v: Vec<u128> = (0..n)
                                    .map(|_| {
                                        let t = Instant::now();
                                        let _ = e4.sql(&q);
                                        t.elapsed().as_millis()
                                    })
                                    .collect();
                                v.sort_unstable();
                                v[v.len() / 2]
                            })
                            .join()
                            .expect("engine thread");
                            println!("      {} planning (EXPLAIN) median {plan_ms} ms", v.name);
                        }
                        (median(d), median(b))
                    }
                    _ => (duck_ms, bm_ms),
                };
                println!(
                    "SAME  {:<34} rows={:<7} duck_ms={duck_ms:<6} burrmill_ms={bm_ms:<6} ratio={:.2}",
                    v.name,
                    w.len(),
                    bm_ms as f64 / duck_ms.max(1) as f64
                );
            }
            (Ok(w), Ok(g)) => {
                diff += 1;
                let at = w
                    .iter()
                    .zip(&g)
                    .position(|(a, b)| a != b)
                    .unwrap_or(w.len().min(g.len()));
                println!(
                    "DIFF  {:<34} rows duck={} burrmill={}",
                    v.name,
                    w.len(),
                    g.len()
                );
                println!(
                    "      duck     {}",
                    w.get(at)
                        .map(|s| s.chars().take(300).collect::<String>())
                        .unwrap_or_default()
                );
                println!(
                    "      burrmill {}",
                    g.get(at)
                        .map(|s| s.chars().take(300).collect::<String>())
                        .unwrap_or_default()
                );
            }
            (Ok(_), Err(e)) => {
                fail += 1;
                println!("FAIL  {:<34} {e}", v.name);
            }
            (Err(e), _) => println!(
                "DUCK-QUERY-FAIL {:<28} {}",
                v.name,
                first_line(&e.to_string())
            ),
        }
    }

    let mut checks = 0;
    let mut checks_ok = 0;
    // `main` is inside a runtime, where dropping the engine panics; an early `?` here would drop it.
    let checked = (|| -> anyhow::Result<()> {
        if let Ok(dir) = std::fs::read_dir(root.join("checks")) {
            let mut files: Vec<_> = dir
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "sql"))
                .collect();
            files.sort();
            for f in files {
                let stem = f.file_stem().unwrap().to_string_lossy().to_string();
                // A check with no expected file expects zero rows, as its header says.
                let expected: Value = match std::fs::read_to_string(
                    root.join("checks/expected").join(format!("{stem}.json")),
                ) {
                    Ok(t) => serde_json::from_str(&t)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Value::Array(vec![]),
                    Err(e) => return Err(e.into()),
                };
                let sql: String = std::fs::read_to_string(&f)?
                    .lines()
                    .filter(|l| !l.trim_start().starts_with("--"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let sql = sql.trim().trim_end_matches(';').to_string();
                let duck = crate::encode_parity::nuthatch_rows(&conn, &sql)
                    .map_err(|e| first_line(&e.to_string()));
                let e2 = std::sync::Arc::clone(&engine);
                let q = sql.clone();
                let bm = std::thread::spawn(move || -> Result<Value, String> {
                    let batches = e2.sql(&q).map_err(|e| first_line(&e.to_string()))?;
                    let mut rows = Vec::new();
                    for b in &batches {
                        rows.extend(burrmill::df::encode::rows(b).map_err(|e| e.to_string())?);
                    }
                    Ok(Value::Array(rows))
                })
                .join()
                .expect("engine thread");
                checks += 1;
                let bm_ok = bm.as_ref().is_ok_and(|v| *v == expected);
                checks_ok += bm_ok as usize;
                println!(
                    "CHECK {stem:<14} burrmill={} duckdb={}",
                    if bm_ok {
                        "expected".to_string()
                    } else {
                        format!("{bm:?}")
                    },
                    if duck.as_ref().is_ok_and(|v| *v == expected) {
                        "expected".to_string()
                    } else {
                        format!("{duck:?}")
                    }
                );
            }
        }
        Ok(())
    })();
    std::thread::spawn(move || drop(engine))
        .join()
        .expect("drop engine");
    checked?;
    println!(
        "VIEWS\tviews={}\tsame={same}\tdiffering={diff}\tfailing={fail}\tchecks={checks_ok}/{checks}",
        nest.views.len()
    );
    Ok(())
}

/// `engine-analyze <nest> <view>`: `EXPLAIN ANALYZE` of one authored view through `Engine`, after a
/// warm-up, so operator timings are the steady state rather than the first read of the footers.
pub fn analyze(root: &str, view: &str) -> anyhow::Result<()> {
    let nest = load_nest(Path::new(root))?;
    let root_owned = Path::new(root).to_path_buf();
    let views: Vec<(String, String)> = nest
        .views
        .iter()
        .map(|v| (v.name.clone(), v.body.clone()))
        .collect();
    let view = view.to_string();
    std::thread::spawn(move || -> anyhow::Result<()> {
        // `BUDGET_MB` (with `THREADS`, default 2) opens the session a host opens, batch size and all.
        let mut e = match std::env::var("BUDGET_MB")
            .ok()
            .and_then(|m| m.parse::<usize>().ok())
        {
            Some(mb) => burrmill::Engine::open_nest_budgeted(
                &root_owned,
                burrmill::Budget {
                    memory_bytes: mb << 20,
                    threads: std::env::var("THREADS")
                        .ok()
                        .and_then(|t| t.parse().ok())
                        .unwrap_or(2),
                    spill: None,
                },
            )?,
            None => burrmill::Engine::open_nest(&root_owned)?,
        };
        for (n, b) in &views {
            let _ = e.register_view(n, b);
        }
        // A path to a statement instead of a view's name analyses that statement.
        let sql = match std::fs::read_to_string(&view) {
            Ok(text) => text.trim().trim_end_matches(';').to_string(),
            Err(_) => format!("SELECT * FROM \"{view}\""),
        };
        let t = std::time::Instant::now();
        let mut rows = 0usize;
        e.sql_for_each(&sql, |b| {
            rows += b.num_rows();
            Ok(())
        })?;
        eprintln!("first run: {rows} rows in {} ms", t.elapsed().as_millis());
        // Warm repeats before the analysed run, so a profiler over the process sees the query
        // rather than opening the nest.
        let repeats = std::env::var("REPEATS")
            .ok()
            .and_then(|r| r.parse::<usize>().ok())
            .unwrap_or(0);
        for i in 0..repeats {
            let t = std::time::Instant::now();
            let mut rows = 0usize;
            e.sql_for_each(&sql, |b| {
                rows += b.num_rows();
                Ok(())
            })?;
            eprintln!(
                "repeat {}: {rows} rows in {} ms",
                i + 1,
                t.elapsed().as_millis()
            );
        }
        use arrow::array::Array;
        for b in e.sql(&format!("EXPLAIN ANALYZE {sql}"))? {
            let plan = b.column(1);
            let plan = arrow::compute::cast(plan, &arrow::datatypes::DataType::Utf8)?;
            let plan = plan
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..plan.len() {
                println!("{}", plan.value(i));
            }
        }
        std::thread::spawn(move || drop(e)).join().expect("drop");
        Ok(())
    })
    .join()
    .expect("analyze thread")
}

/// `engine-explain <nest> <view>`: the plan of one authored view through `Engine`, not run. For a
/// view that cannot be run to completion to be analysed.
pub fn explain(root: &str, view: &str) -> anyhow::Result<()> {
    let nest = load_nest(Path::new(root))?;
    let root_owned = Path::new(root).to_path_buf();
    let views: Vec<(String, String)> = nest
        .views
        .iter()
        .map(|v| (v.name.clone(), v.body.clone()))
        .collect();
    let view = view.to_string();
    std::thread::spawn(move || -> anyhow::Result<()> {
        let mut e = burrmill::Engine::open_nest(&root_owned)?;
        for (n, b) in &views {
            let _ = e.register_view(n, b);
        }
        use arrow::array::Array;
        // A path to a statement instead of a view's name explains that statement.
        let sql = match std::fs::read_to_string(&view) {
            Ok(text) => text.trim().trim_end_matches(';').to_string(),
            Err(_) => format!("SELECT * FROM \"{view}\""),
        };
        for b in e.sql(&format!("EXPLAIN {sql}"))? {
            let kind = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8)?;
            let kind = kind
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let plan = arrow::compute::cast(b.column(1), &arrow::datatypes::DataType::Utf8)?;
            let plan = plan
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..plan.len() {
                println!("== {}\n{}", kind.value(i), plan.value(i));
            }
        }
        std::thread::spawn(move || drop(e)).join().expect("drop");
        Ok(())
    })
    .join()
    .expect("explain thread")
}

/// `engine-sql <nest> <sql-file>...`: each file's statement on both engines over the nest with its
/// views, as a sorted row multiset in nuthatch's JSON, with warm medians of 5.
pub fn sql_files(root: &str, files: &[String]) -> anyhow::Result<()> {
    let nest = load_nest(Path::new(root))?;
    let conn = duck(&nest)?;
    for v in &nest.views {
        let _ = conn.execute_batch(&v.text);
    }
    let root_owned = Path::new(root).to_path_buf();
    let views: Vec<(String, String)> = nest
        .views
        .iter()
        .map(|v| (v.name.clone(), v.body.clone()))
        .collect();
    let engine = std::thread::spawn(move || -> anyhow::Result<_> {
        let mut e = burrmill::Engine::open_nest(&root_owned)?;
        for (n, b) in &views {
            let _ = e.register_view(n, b);
        }
        Ok(e)
    })
    .join()
    .expect("engine")?;
    let engine = std::sync::Arc::new(engine);
    let median = |mut v: Vec<u128>| {
        v.sort_unstable();
        v[v.len() / 2]
    };
    for f in files {
        let sql = std::fs::read_to_string(f)?
            .trim()
            .trim_end_matches(';')
            .to_string();
        let duck_rows = crate::encode_parity::nuthatch_rows(&conn, &sql).map(sorted_rows);
        let duck_ms = match &duck_rows {
            Ok(_) => median(
                (0..5)
                    .map(|_| {
                        let t = Instant::now();
                        let mut s = conn.prepare(&sql).unwrap();
                        for b in s.query_arrow([]).unwrap() {
                            std::hint::black_box(b.num_rows());
                        }
                        t.elapsed().as_millis()
                    })
                    .collect(),
            ),
            Err(_) => 0,
        };
        let e2 = std::sync::Arc::clone(&engine);
        let q = sql.clone();
        let (rows, ms) = std::thread::spawn(move || -> (Result<Vec<String>, String>, Vec<u128>) {
            let rows = e2
                .sql(&q)
                .map_err(|e| first_line(&e.to_string()))
                .and_then(|bs| {
                    let mut r = Vec::new();
                    for b in &bs {
                        r.extend(burrmill::df::encode::rows(b).map_err(|e| e.to_string())?);
                    }
                    Ok(sorted_rows(Value::Array(r)))
                });
            let ms = (0..5)
                .map(|_| {
                    let t = Instant::now();
                    let _ = e2.sql_for_each(&q, |_| Ok(()));
                    t.elapsed().as_millis()
                })
                .collect();
            (rows, ms)
        })
        .join()
        .expect("engine");
        if std::env::var("PRINT").is_ok() {
            if let Ok(r) = &duck_rows {
                r.iter().for_each(|row| println!("  duck {row}"));
            }
            if let Ok(r) = &rows {
                r.iter().for_each(|row| println!("  row  {row}"));
            }
        }
        let digest = |r: &Vec<String>| {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            r.hash(&mut h);
            format!("{:016x}", h.finish())
        };
        println!(
            "{f}\n  duckdb   {} ms={duck_ms}\n  burrmill {} ms={}",
            match &duck_rows {
                Ok(r) => format!("rows={} digest={}", r.len(), digest(r)),
                Err(e) => format!("ERROR {}", first_line(&e.to_string())),
            },
            match &rows {
                Ok(r) => format!("rows={} digest={}", r.len(), digest(r)),
                Err(e) => format!("ERROR {e}"),
            },
            median(ms)
        );
    }
    std::thread::spawn(move || drop(engine))
        .join()
        .expect("drop");
    Ok(())
}

/// DuckDB set up as nuthatch sets it up and an `Engine` over the same nest, both with every
/// authored view, and the names of the views both answer identically.
pub fn both_engines(
    root: &str,
) -> anyhow::Result<(duckdb::Connection, burrmill::Engine, Vec<String>)> {
    let nest = load_nest(Path::new(root))?;
    let conn = duck(&nest)?;
    let mut ok = Vec::new();
    for v in &nest.views {
        if conn.execute_batch(&v.text).is_ok() {
            ok.push(v.name.clone());
        }
    }
    let root_owned = Path::new(root).to_path_buf();
    let views: Vec<(String, String)> = nest
        .views
        .iter()
        .map(|v| (v.name.clone(), v.body.clone()))
        .collect();
    let (engine, registered) = std::thread::spawn(move || -> anyhow::Result<_> {
        let mut e = burrmill::Engine::open_nest(&root_owned)?;
        let mut reg = Vec::new();
        for (n, b) in &views {
            if e.register_view(n, b).is_ok() {
                reg.push(n.clone());
            }
        }
        Ok((e, reg))
    })
    .join()
    .expect("engine")?;
    let mut same = Vec::new();
    for name in ok.into_iter().filter(|n| registered.contains(n)) {
        let sql = format!("SELECT * FROM \"{name}\"");
        let want = crate::encode_parity::nuthatch_rows(&conn, &sql)
            .map(sorted_rows)
            .ok();
        let got = std::thread::scope(|s| {
            s.spawn(|| {
                engine.sql(&sql).ok().map(|bs| {
                    let mut r = Vec::new();
                    for b in &bs {
                        r.extend(burrmill::df::encode::rows(b).unwrap_or_default());
                    }
                    sorted_rows(Value::Array(r))
                })
            })
            .join()
            .expect("engine")
        });
        if want.is_some() && want == got {
            same.push(name);
        }
    }
    Ok((conn, engine, same))
}

/// `rewrite-parity <nest> <views-dir>`: portable rewrites of a nest's views (plan phase 1b). Each
/// rewritten view must answer on DuckDB exactly as the original does, and on `Engine` exactly as on
/// DuckDB. Only views whose text changed are compared against the original; every view is run on
/// both engines.
pub fn rewrite_parity(root: &str, dir: &str) -> anyhow::Result<()> {
    let nest = load_nest(Path::new(root))?;
    let rewritten = crate::df_views::load_views(Path::new(dir))?;
    let original = duck(&nest)?;
    for v in &nest.views {
        if let Err(e) = original.execute_batch(&v.text) {
            println!("ORIGINAL-FAIL {} {}", v.name, first_line(&e.to_string()));
        }
    }
    let portable = duck(&nest)?;
    for v in &rewritten {
        if let Err(e) = portable.execute_batch(&v.text) {
            println!(
                "REWRITE-DUCK-FAIL {} {}",
                v.name,
                first_line(&e.to_string())
            );
        }
    }
    let root_owned = Path::new(root).to_path_buf();
    let bodies: Vec<(String, String)> = rewritten
        .iter()
        .map(|v| (v.name.clone(), v.body.clone()))
        .collect();
    let (engine, reg) = std::thread::spawn(move || -> anyhow::Result<_> {
        let mut e = burrmill::Engine::open_nest(&root_owned)?;
        let reg: Vec<Option<String>> = bodies
            .iter()
            .map(|(n, b)| {
                e.register_view(n, b)
                    .err()
                    .map(|e| first_line(&e.to_string()))
            })
            .collect();
        Ok((e, reg))
    })
    .join()
    .expect("engine")?;
    let engine = std::sync::Arc::new(engine);
    let text_of = |name: &str| {
        nest.views
            .iter()
            .find(|v| v.name == name)
            .map(|v| v.text.clone())
    };
    let (mut ok, mut bad) = (0, 0);
    for (i, v) in rewritten.iter().enumerate() {
        let sql = format!("SELECT * FROM \"{}\"", v.name);
        let changed = text_of(&v.name).is_none_or(|t| t != v.text);
        let on_portable = crate::encode_parity::nuthatch_rows(&portable, &sql).map(sorted_rows);
        let vs_original = if changed {
            let orig = crate::encode_parity::nuthatch_rows(&original, &sql).map(sorted_rows);
            match (&orig, &on_portable) {
                (Ok(a), Ok(b)) if a == b => "same-as-original".to_string(),
                (Ok(a), Ok(b)) => format!("DIFFERS-FROM-ORIGINAL rows {} vs {}", a.len(), b.len()),
                (Err(e), _) => format!("original fails: {}", first_line(&e.to_string())),
                (_, Err(e)) => format!("REWRITE FAILS ON DUCKDB: {}", first_line(&e.to_string())),
            }
        } else {
            "unchanged".to_string()
        };
        let on_engine = match &reg[i] {
            Some(why) => Err(why.clone()),
            None => {
                let e2 = std::sync::Arc::clone(&engine);
                let q = sql.clone();
                std::thread::spawn(move || -> Result<Vec<String>, String> {
                    let bs = e2.sql(&q).map_err(|e| first_line(&e.to_string()))?;
                    let mut r = Vec::new();
                    for b in &bs {
                        r.extend(burrmill::df::encode::rows(b).map_err(|e| e.to_string())?);
                    }
                    Ok(sorted_rows(Value::Array(r)))
                })
                .join()
                .expect("engine")
            }
        };
        let vs_engine = match (&on_portable, &on_engine) {
            (Ok(a), Ok(b)) if a == b => format!("burrmill-same rows={}", a.len()),
            (Ok(a), Ok(b)) => {
                let at = a.iter().zip(b).position(|(x, y)| x != y).unwrap_or(0);
                format!(
                    "BURRMILL-DIFFERS rows {} vs {}\n      duck     {}\n      burrmill {}",
                    a.len(),
                    b.len(),
                    a.get(at)
                        .map(|s| s.chars().take(240).collect::<String>())
                        .unwrap_or_default(),
                    b.get(at)
                        .map(|s| s.chars().take(240).collect::<String>())
                        .unwrap_or_default()
                )
            }
            (_, Err(e)) => format!("BURRMILL-FAILS {e}"),
            (Err(_), _) => "duckdb-fails".to_string(),
        };
        let good = !vs_original.contains("DIFFERS")
            && !vs_original.contains("FAILS")
            && vs_engine.starts_with("burrmill-same");
        if good {
            ok += 1
        } else {
            bad += 1
        }
        println!(
            "{} {:<36} {vs_original:<22} {vs_engine}",
            if good { "OK  " } else { "TODO" },
            v.name
        );
    }
    println!("REWRITE\tviews={}\tok={ok}\ttodo={bad}", rewritten.len());
    std::thread::spawn(move || drop(engine))
        .join()
        .expect("drop");
    Ok(())
}
