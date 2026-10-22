//! #40: every operator that holds its input, run under a budget over string views, is charged within
//! a stated factor of the same statement over `Utf8`.
//!
//! A view column is charged for every buffer it references, and a 128-row batch of views can
//! reference a 1 MiB buffer it fills a few kilobytes of. Each test runs one statement twice on one
//! budgeted engine, once over the table's own `Utf8View` columns and once over the same columns cast
//! to `Utf8`, and compares the pool's peak reservation. The plan is checked for the operator, so a
//! planner change that stops exercising it fails here rather than passing vacuously.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, AsArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::Value;

const ROWS: u64 = 200_000;
const BUDGET: usize = 1 << 30;
/// The views run may hold this many times the `Utf8` run's peak.
const FACTOR: usize = 4;
/// Below this the `Utf8` peak is noise from a few batches in flight, and a factor of it is not a
/// measurement of what the operator holds.
const FLOOR: usize = 1 << 20;

fn segments() -> &'static Vec<(PathBuf, u64)> {
    static SEGS: OnceLock<Vec<(PathBuf, u64)>> = OnceLock::new();
    SEGS.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("df_viewbudget");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("who", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, false),
        ]));
        (0..8u64)
            .map(|n| {
                let span = n * ROWS / 8..(n + 1) * ROWS / 8;
                let who: Vec<String> = span.clone().map(|i| format!("0x{i:040x}")).collect();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(span.collect::<Vec<_>>())),
                        Arc::new(StringArray::from(who.clone())),
                        Arc::new(StringArray::from(who)),
                    ],
                )
                .unwrap();
                let path = dir.join(format!("t-{n:064x}.parquet"));
                let tmp = dir.join(format!("t-{n}.{}.tmp", std::process::id()));
                let f = std::fs::File::create(&tmp).unwrap();
                let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                w.write(&batch).unwrap();
                w.close().unwrap();
                std::fs::rename(&tmp, &path).unwrap();
                let len = std::fs::metadata(&path).unwrap().len();
                (path, len)
            })
            .collect()
    })
}

fn engine() -> Engine {
    engine_with(BUDGET, None)
}

fn engine_with(memory_bytes: usize, spill: Option<(PathBuf, u64)>) -> Engine {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes,
        threads: 8,
        spill,
    })
    .unwrap();
    let declared = vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
        ("amount".into(), "string".into()),
    ];
    e.register_facts("t", &declared, segments().clone(), &[], (None, None))
        .unwrap();
    e
}

fn run(e: &Engine, sql: &str) -> Result<(Vec<Value>, usize), String> {
    e.take_memory_peak();
    let batches = e.sql(sql).map_err(|err| err.to_string())?;
    let peak = e.take_memory_peak();
    let rows = batches
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    Ok((rows, peak))
}

fn plan(e: &Engine, sql: &str) -> String {
    let batches = e.sql(&format!("EXPLAIN {sql}")).unwrap();
    let mut out = String::new();
    for b in &batches {
        for row in burrmill::df::encode::rows(b).unwrap() {
            out.push_str(&row.to_string());
        }
    }
    out
}

/// `body` reads `s`, which is the table with its text columns as views in one run and as `Utf8` in
/// the other; `marker` is a fragment of the physical plan that names the operator under test.
fn within(operator: &str, marker: &str, body: &str) {
    within_as(operator, marker, ("Utf8View", "Utf8"), body)
}

/// As [`within`], with `who` and `amount` cast to `types.0` in one run and `types.1` in the other.
fn within_as(operator: &str, marker: &str, types: (&str, &str), body: &str) {
    let e = engine();
    let cte = |t: &str| {
        format!(
            "WITH s AS (SELECT block_number, arrow_cast(who, '{t}') AS who, \
             arrow_cast(amount, '{t}') AS amount FROM t) {body}"
        )
    };
    let (views, text) = (cte(types.0), cte(types.1));
    let shown = plan(&e, &views);
    assert!(
        shown.contains(marker),
        "{operator}: the plan has no {marker}, so this does not test it:\n{shown}"
    );
    assert!(
        plan(&e, &text).contains(marker),
        "{operator}: the Utf8 plan has no {marker}"
    );
    let (want, text_peak) = run(&e, &text).unwrap_or_else(|m| panic!("{operator} over Utf8: {m}"));
    let got = run(&e, &views);
    eprintln!(
        "MATRIX | {operator} | {text_peak} | {} |",
        match &got {
            Ok((_, p)) => format!("{p} | {:.2}", *p as f64 / text_peak.max(1) as f64),
            Err(m) => format!("refused: {}", m.lines().next().unwrap_or_default()),
        }
    );
    let (rows, views_peak) = got.unwrap_or_else(|m| panic!("{operator} over views refused: {m}"));
    assert_eq!(rows, want, "{operator}: the two runs disagree");
    assert!(
        views_peak <= FACTOR * text_peak.max(FLOOR),
        "{operator}: views peaked at {views_peak} bytes against {text_peak} over Utf8"
    );
}

#[test]
fn the_table_reads_its_text_as_views() {
    let e = engine();
    let (rows, _) = run(&e, "SELECT arrow_typeof(who) AS t FROM t LIMIT 1").unwrap();
    assert_eq!(rows, vec![serde_json::json!({"t": "Utf8View"})]);
}

#[test]
fn hash_join_partitioned() {
    within(
        "hash join (partitioned)",
        "HashJoinExec: mode=Partitioned",
        "SELECT count(*) AS n, max(b.amount) AS m FROM s a JOIN s b ON a.who = b.who",
    );
}

#[test]
fn hash_join_collect_left() {
    within(
        "hash join (collect left)",
        "HashJoinExec: mode=CollectLeft",
        "SELECT count(*) AS n, max(b.amount) AS m FROM s a \
         JOIN (SELECT who, amount FROM s ORDER BY block_number LIMIT 1000) b ON a.who = b.who",
    );
}

#[test]
fn nested_loop_join() {
    within(
        "nested-loop join",
        "NestedLoopJoinExec",
        "SELECT count(*) AS n, max(a.amount) AS m FROM s a \
         JOIN (SELECT who FROM s WHERE block_number < 3) b ON a.who < b.who OR a.amount < b.who",
    );
}

#[test]
fn cross_join() {
    within(
        "cross join",
        "CrossJoinExec",
        "SELECT count(*) AS n, max(a.amount) AS m, max(b.who) AS w FROM s a \
         CROSS JOIN (SELECT who FROM s WHERE block_number < 3) b",
    );
}

#[test]
fn range_join() {
    within(
        "range join (burrmill)",
        "RangeJoinExec",
        "SELECT count(*) AS n, max(a.amount) AS m, max(e.who) AS w FROM s a \
         JOIN (SELECT block_number AS lo, block_number + 1000 AS hi, who FROM s \
               WHERE block_number % 1000 = 0) e \
         ON a.block_number >= e.lo AND a.block_number < e.hi",
    );
}

#[test]
fn hash_aggregate_partial_and_final() {
    within(
        "hash aggregate (partial + final)",
        "AggregateExec: mode=Partial",
        "SELECT count(*) AS n, max(c) AS c FROM \
         (SELECT who, amount, count(*) AS c FROM s GROUP BY who, amount)",
    );
}

#[test]
fn aggregate_min_max_text() {
    within(
        "aggregate max(text)",
        "AggregateExec",
        "SELECT count(*) AS n, max(m) AS m FROM \
         (SELECT block_number % 1000 AS g, max(who) AS m, min(amount) AS l FROM s GROUP BY 1)",
    );
}

#[test]
fn aggregate_array_agg() {
    within(
        "aggregate array_agg",
        "array_agg",
        "SELECT count(*) AS n, max(cardinality(l)) AS c FROM \
         (SELECT block_number % 100 AS g, array_agg(who) AS l FROM s GROUP BY 1)",
    );
}

#[test]
fn aggregate_ordered_array_agg() {
    within(
        "aggregate array_agg ORDER BY (burrmill)",
        "array_agg",
        "SELECT count(*) AS n, max(l[1]) AS f FROM \
         (SELECT block_number % 100 AS g, array_agg(who ORDER BY block_number DESC) AS l \
          FROM s GROUP BY 1)",
    );
}

#[test]
fn aggregate_string_agg() {
    within(
        "aggregate string_agg",
        "string_agg",
        "SELECT count(*) AS n, max(length(l)) AS c FROM \
         (SELECT block_number % 100 AS g, string_agg(who, ',') AS l FROM s GROUP BY 1)",
    );
}

#[test]
fn aggregate_count_distinct() {
    within(
        "aggregate count(DISTINCT)",
        "AggregateExec",
        "SELECT block_number % 4 AS g, count(DISTINCT who) AS n, count(*) AS c \
         FROM s GROUP BY 1 ORDER BY 1",
    );
}

#[test]
fn top_per_group() {
    within(
        "top per group (burrmill)",
        "first_value",
        "SELECT count(*) AS n, max(who) AS w FROM \
         (SELECT who, amount, row_number() OVER (PARTITION BY amount ORDER BY block_number DESC) AS rn \
          FROM s) WHERE rn = 1",
    );
}

#[test]
fn window_bounded() {
    within(
        "window (bounded)",
        "BoundedWindowAggExec",
        "SELECT count(*) AS n, max(p) AS p FROM \
         (SELECT lag(who) OVER (PARTITION BY block_number % 10 ORDER BY block_number) AS p FROM s)",
    );
}

#[test]
fn window_unbounded() {
    within(
        "window (whole partition)",
        "WindowAggExec",
        "SELECT count(*) AS n, max(p) AS p FROM \
         (SELECT max(who) OVER (PARTITION BY block_number % 10) AS p FROM s)",
    );
}

#[test]
fn window_last_value_ignore_nulls() {
    within(
        "window last_value IGNORE NULLS (burrmill)",
        "WindowAggExec",
        "SELECT count(*) AS n, max(p) AS p FROM \
         (SELECT last_value(CASE WHEN block_number % 7 = 0 THEN who END) IGNORE NULLS OVER \
          (PARTITION BY block_number % 10 ORDER BY block_number \
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p FROM s)",
    );
}

#[test]
fn union_distinct() {
    within(
        "union (distinct)",
        "UnionExec",
        "SELECT count(*) AS n, max(w) AS w FROM (SELECT who AS w FROM s UNION SELECT amount FROM s)",
    );
}

#[test]
fn union_all_sorted() {
    within(
        "union all under a sort",
        "UnionExec",
        "SELECT count(*) AS n, max(r) AS r FROM (SELECT row_number() OVER (ORDER BY w) AS r FROM \
         (SELECT who AS w FROM s UNION ALL SELECT amount FROM s))",
    );
}

#[test]
fn sort() {
    within(
        "sort",
        "SortExec",
        "SELECT count(*) AS n, max(r) AS r FROM \
         (SELECT row_number() OVER (ORDER BY who DESC) AS r FROM s)",
    );
}

#[test]
fn sort_top_k() {
    within(
        "sort with a limit (top-k)",
        "TopK",
        "SELECT who, amount FROM s ORDER BY who DESC LIMIT 10",
    );
}

#[test]
fn shared_subquery() {
    within(
        "shared subquery (burrmill)",
        "Shared",
        "SELECT count(*) AS n, max(a.m) AS m FROM \
         (SELECT block_number % 1000 AS k, max(who) AS m FROM s GROUP BY 1) a JOIN \
         (SELECT block_number % 1000 AS k, max(who) AS m FROM s GROUP BY 1) b ON a.k = b.k",
    );
}

/// A shared subquery the pool cannot hold is read by running its plan for each reader, and a
/// `RepartitionExec` runs once per partition: the gate's `poi.overview` panicked under the second
/// reader with "partition not used yet" (nuthatch 5.9.0 bump, 2026-10-22).
#[test]
fn a_shared_subquery_too_big_to_hold_is_read_by_each_reader() {
    let e = engine_with(32 << 20, None);
    let sql = "WITH g AS (SELECT k, n, repeat('x', 800) AS w FROM \
                          (SELECT block_number % 50000 AS k, count(*) AS n FROM t GROUP BY 1)) \
               SELECT (SELECT count(*) FROM g) + (SELECT sum(n) FROM g WHERE k > 10) \
                      + (SELECT max(length(w)) FROM g) AS total";
    let p = plan(&e, sql);
    assert!(
        p.contains("SharedExec") && p.contains("RepartitionExec"),
        "the plan does not share a repartitioned definition: {p}"
    );
    let (rows, _) = run(&e, sql).unwrap_or_else(|m| panic!("the shared read refused: {m}"));
    assert_eq!(rows.len(), 1);
    // 50,000 groups of four rows, 11 of them at k <= 10, and a width of 800; a sum of BIGINT
    // is a HUGEINT, which is served as text.
    assert_eq!(rows[0]["total"], Value::from("250756"));
}

/// The QoS nest's budget: 2 GB over eight partitions, where production refused in `ExternalSorter`
/// on 4.2.0. Without a spill directory the sort must answer in memory, and with one it must answer
/// without needing to write there.
#[test]
fn sort_at_the_production_budget_answers_in_memory() {
    let sql = |from: &str| {
        format!(
            "WITH s AS (SELECT block_number, {from}) SELECT count(*) AS n, max(r) AS r FROM \
             (SELECT row_number() OVER (ORDER BY who DESC, amount) AS r FROM s)"
        )
    };
    let views = sql("who, amount FROM t");
    let text = sql("arrow_cast(who, 'Utf8') AS who, arrow_cast(amount, 'Utf8') AS amount FROM t");
    let dir = tempfile::tempdir().unwrap();
    for spill in [None, Some((dir.path().to_path_buf(), 1u64 << 30))] {
        let e = engine_with(2_000_000_000, spill.clone());
        let (want, text_peak) = run(&e, &text).unwrap();
        let got = run(&e, &views);
        let wrote = holds_a_file(dir.path());
        eprintln!(
            "MATRIX | sort at 2 GB, spill {} | {text_peak} | {} | wrote spill: {wrote}",
            spill.is_some(),
            match &got {
                Ok((_, p)) => format!("{p} | {:.2}", *p as f64 / text_peak.max(1) as f64),
                Err(m) => format!("refused: {}", m.lines().next().unwrap_or_default()),
            }
        );
        let (rows, peak) = got.unwrap_or_else(|m| panic!("the sort over views refused: {m}"));
        assert_eq!(rows, want);
        assert!(!wrote, "the sort spilled what fits in memory as Utf8");
        assert!(
            peak <= FACTOR * text_peak.max(FLOOR),
            "views peaked at {peak} bytes against {text_peak} over Utf8"
        );
    }
}

/// #56: a sort too large for the pool spills and answers, and one whose spill outgrows the cap is
/// stopped by the cap. Over a budget's 128-row batches the spill merged one run per batch, and the
/// merge took the pool before anything was written: a projection's output always reached the sort
/// that way, and a cross join's has since #49 copied it into such batches.
#[test]
fn a_sort_too_large_for_the_pool_spills() {
    let open = |memory_bytes: usize, spill: Option<(PathBuf, u64)>| {
        Engine::open_empty_budgeted(burrmill::Budget {
            memory_bytes,
            threads: 2,
            spill,
        })
        .unwrap()
    };
    let cross = |rows: u64| {
        format!(
            "SELECT a.i FROM range({rows}) a(i), range(120) b(j) \
             ORDER BY (a.i * 2654435761) % 1000003, b.j"
        )
    };
    let projected = "SELECT i FROM (SELECT range * 3 AS i FROM range(3000000)) \
                     ORDER BY (i * 2654435761) % 1000003, i";
    let dir = tempfile::tempdir().unwrap();
    for sql in [cross(25_000), projected.to_string()] {
        assert!(
            open(32 << 20, None).sql(&sql).is_err(),
            "{sql}: the sort fits in memory, so this does not test its spill"
        );
        let batches = open(32 << 20, Some((dir.path().to_path_buf(), 1 << 30)))
            .sql(&sql)
            .unwrap_or_else(|m| panic!("{sql}: the sort refused rather than spill: {m}"));
        let keys: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let i = b.column(0).as_primitive::<arrow::datatypes::Int64Type>();
                i.values()
                    .iter()
                    .map(|i| (i * 2654435761) % 1000003)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(keys.len(), 3_000_000, "{sql}");
        assert!(
            keys.is_sorted(),
            "{sql}: the spilled sort answered out of order"
        );
    }

    let capped = open(128 << 20, Some((dir.path().to_path_buf(), 16 << 20)))
        .sql(&cross(1_000_000))
        .map(|_| ())
        .expect_err("120 million rows answered under a 16 MB spill cap");
    assert!(
        capped.to_string().contains("exceeded the allowable limit"),
        "stopped for something other than the spill cap: {capped}"
    );
}

/// DataFusion makes its own directory under the spill directory before it has anything to write.
fn holds_a_file(dir: &std::path::Path) -> bool {
    std::fs::read_dir(dir).unwrap().any(|e| {
        let e = e.unwrap();
        match e.file_type().unwrap().is_dir() {
            true => holds_a_file(&e.path()),
            false => true,
        }
    })
}

#[test]
fn aggregate_ordered_by_text() {
    let body = "SELECT count(*) AS n, max(l[1]) AS f FROM \
         (SELECT block_number % 100 AS g, array_agg(block_number ORDER BY amount DESC) AS l \
          FROM s GROUP BY 1)";
    within(
        "aggregate array_agg ORDER BY text (burrmill)",
        "array_agg",
        body,
    );
    // The repartition's overcharge reaches the peak only when its queue backs up, which takes a
    // contended machine (#74), so the plan is checked for the repartition running over offsets.
    let shown = plan(
        &engine(),
        &format!(
            "WITH s AS (SELECT block_number, arrow_cast(who, 'Utf8View') AS who, \
             arrow_cast(amount, 'Utf8View') AS amount FROM t) {body}"
        ),
    );
    assert!(
        shown.contains("CastViewsExec\\n              RepartitionExec: partitioning=Hash"),
        "the state's repartition charges the views' buffers:\n{shown}"
    );
}

#[test]
fn hash_join_partitioned_binary() {
    within_as(
        "hash join (partitioned) over binary",
        "HashJoinExec: mode=Partitioned",
        ("BinaryView", "Binary"),
        "SELECT count(*) AS n, max(b.amount) AS m FROM s a JOIN s b ON a.who = b.who",
    );
}

/// #46: a key that is an expression over a view column was planned for views, and the operator now
/// reads offsets. Each statement must answer as it does over `Utf8`, within the factor.
#[test]
fn expression_keys() {
    let cases = [
        (
            "sort on a column then LOWER (#46)",
            "TopK",
            "SELECT who FROM s ORDER BY who, LOWER(amount) LIMIT 3",
        ),
        (
            "sort on LOWER",
            "SortExec",
            "SELECT count(*) AS n, max(r) AS r FROM \
             (SELECT row_number() OVER (ORDER BY LOWER(who) DESC, amount) AS r FROM s)",
        ),
        (
            "top-k on CONCAT",
            "TopK",
            "SELECT who FROM s ORDER BY CONCAT(amount, who) DESC, who LIMIT 3",
        ),
        (
            "top-k on CASE",
            "TopK",
            "SELECT who FROM s ORDER BY CASE WHEN block_number % 2 = 0 THEN who ELSE amount END, \
             block_number LIMIT 3",
        ),
        (
            "top-k on COALESCE",
            "TopK",
            "SELECT who FROM s ORDER BY block_number % 3, COALESCE(NULLIF(who, amount), 'z') DESC, \
             who LIMIT 3",
        ),
        (
            "top-k on a cast",
            "TopK",
            "SELECT who FROM s ORDER BY CAST(amount AS VARCHAR) DESC, block_number LIMIT 3",
        ),
        (
            "sort on COALESCE alone",
            "SortExec",
            "SELECT count(*) AS n, max(r) AS r FROM \
             (SELECT row_number() OVER (ORDER BY COALESCE(NULLIF(who, amount), amount) DESC) AS r FROM s)",
        ),
        (
            "group by LOWER",
            "AggregateExec",
            "SELECT count(*) AS n, max(c) AS c FROM \
             (SELECT LOWER(who) AS w, count(*) AS c FROM s GROUP BY LOWER(who))",
        ),
        (
            "join on LOWER",
            "HashJoinExec",
            "SELECT count(*) AS n, max(b.amount) AS m FROM s a JOIN s b ON LOWER(a.who) = LOWER(b.amount)",
        ),
        (
            "partition by LOWER",
            "Hash([lower(",
            "SELECT count(*) AS n, max(p) AS p FROM \
             (SELECT max(amount) OVER (PARTITION BY LOWER(who)) AS p FROM s)",
        ),
        (
            "join on a window partitioned by LOWER (#1790)",
            "HashJoinExec: mode=Partitioned",
            "SELECT count(*) AS n, count(c.amount) AS m FROM \
             (SELECT LOWER(who) AS k, LOWER(amount) AS v FROM s GROUP BY 1, 2) f \
             LEFT JOIN (SELECT * FROM (SELECT LOWER(who) AS k, LOWER(amount) AS v, amount, \
             ROW_NUMBER() OVER (PARTITION BY LOWER(who), LOWER(amount) ORDER BY block_number DESC) \
             AS rn FROM s) WHERE rn = 1) c ON c.k = f.k AND c.v = f.v",
        ),
    ];
    let failed: Vec<String> = cases
        .iter()
        .filter_map(|(operator, marker, body)| {
            std::panic::catch_unwind(|| within(operator, marker, body))
                .err()
                .map(|_| operator.to_string())
        })
        .collect();
    assert!(
        failed.is_empty(),
        "refused, over budget or answering differently: {failed:?}"
    );
}

/// #51: a cross join collects its left side and charges each batch as it arrives. An aggregate's
/// output arrives as 128-row slices that each keep the whole partition's buffers, and 130,000 groups
/// refused under 1 GiB. The join may add this many times the rows' own size to its left side's peak.
const HELD_FACTOR: usize = 2;

#[test]
fn cross_join_charges_what_its_left_side_holds() {
    let e = engine();
    let left = "SELECT block_number % 130000 AS k, max(who) AS w, sum(block_number) AS s \
                FROM t GROUP BY 1";
    let sql = format!(
        "SELECT count(*) AS n, max(p.w) AS w, max(p.s + q.n) AS s FROM ({left}) p \
         CROSS JOIN (SELECT count(*) AS n FROM t) q"
    );
    let shown = plan(&e, &sql);
    assert!(
        shown.contains("CrossJoinExec"),
        "no cross join, so this does not test it:\n{shown}"
    );
    // A host's admission bound reads the same plan, and refuses an operator it does not know.
    assert_eq!(e.parquet_scans(&sql).unwrap(), 2);
    let (_, alone) = run(&e, left).unwrap();
    let batches = e.sql(left).unwrap();
    let held = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let live: usize = held
        .columns()
        .iter()
        .map(|c| match c.as_string_view_opt() {
            Some(v) => v.gc().get_array_memory_size(),
            None => c.get_array_memory_size(),
        })
        .sum();
    let got = run(&e, &sql);
    eprintln!(
        "MATRIX | cross join, collected side | live {live} | left alone {alone} | {}",
        match &got {
            Ok((_, p)) => p.to_string(),
            Err(m) => format!("refused: {}", m.lines().next().unwrap_or_default()),
        }
    );
    let (rows, peak) = got.unwrap_or_else(|m| panic!("the cross join refused: {m}"));
    assert_eq!(rows[0]["n"], 130000);
    assert!(
        peak <= alone + HELD_FACTOR * live,
        "the cross join peaked at {peak} bytes, its left side at {alone} alone, holding {live}"
    );
}

/// #55: a cross join whose left side carries no columns, which `count(*)` and a projection of the
/// right side alone plan to, answers under a budget. Its batches have only a row count.
#[test]
fn cross_join_with_no_left_columns() {
    let e = engine();
    let cases = [
        (
            "SELECT count(*) AS n FROM (SELECT block_number FROM t WHERE block_number < 300) a, \
             (SELECT block_number FROM t WHERE block_number < 200) b",
            vec![("n", 60000)],
        ),
        (
            "SELECT count(*) AS n FROM (SELECT block_number FROM t WHERE block_number < 40) a, \
             (SELECT block_number FROM t WHERE block_number < 30) b, \
             (SELECT block_number FROM t WHERE block_number < 20) c",
            vec![("n", 24000)],
        ),
        (
            "SELECT q.n FROM (SELECT block_number FROM t WHERE block_number = 7) p \
             CROSS JOIN (SELECT count(*) AS n FROM t) q",
            vec![("n", ROWS)],
        ),
        (
            "SELECT max(b.block_number) AS m FROM (SELECT count(*) AS n FROM t) a, t b \
             WHERE b.block_number < 3",
            vec![("m", 2)],
        ),
        (
            "SELECT count(*) OVER () AS n FROM (SELECT block_number FROM t WHERE block_number < 300) a, \
             (SELECT block_number FROM t WHERE block_number < 200) b LIMIT 1",
            vec![("n", 60000)],
        ),
    ];
    let mut refused = Vec::new();
    for (sql, want) in cases {
        let shown = plan(&e, sql);
        assert!(
            shown.contains("CrossJoinExec"),
            "no cross join, so this does not test it:\n{shown}"
        );
        match run(&e, sql) {
            Ok((rows, _)) => {
                assert_eq!(rows.len(), 1, "{sql}");
                for (k, v) in want {
                    assert_eq!(rows[0][k], v, "{sql}");
                }
            }
            Err(m) => refused.push(format!("{sql}: {m}")),
        }
    }
    assert!(refused.is_empty(), "refused:\n{}", refused.join("\n"));
}

/// #1790: under a budget a hash repartition reads offsets, and a key that is an expression over a
/// view hashed as a view where a bare column key hashed as offsets. A partitioned join between the
/// two met only the keys both hashes happened to send to one partition.
#[test]
fn a_join_partitioned_by_a_window_key_meets_every_row() {
    let e = engine();
    let sql = "SELECT count(*) AS n, count(c.amount) AS matched FROM \
               (SELECT LOWER(who) AS k, LOWER(amount) AS v FROM t GROUP BY 1, 2) f \
               LEFT JOIN (SELECT * FROM (SELECT LOWER(who) AS k, LOWER(amount) AS v, amount, \
               ROW_NUMBER() OVER (PARTITION BY LOWER(who), LOWER(amount) ORDER BY block_number DESC) \
               AS rn FROM t) WHERE rn = 1) c ON c.k = f.k AND c.v = f.v";
    let shown = plan(&e, sql);
    assert!(
        shown.contains("HashJoinExec: mode=Partitioned") && shown.contains("CastViewsExec"),
        "the join is not partitioned over offsets, so this does not test it:\n{shown}"
    );
    let (rows, _) = run(&e, sql).unwrap();
    assert_eq!(rows, vec![serde_json::json!({"n": ROWS, "matched": ROWS})]);
}

/// #64: a partitioned join's dynamic filter reaches the probe scan and routes each row by its key
/// hashed as the scan reads it, a view. Under a budget the repartition below the join runs over
/// offsets, and while it hashed a bare key as offsets (before #66) rows went to another partition's
/// filter and were dropped. Plain DataFusion keeps one type on both sides and answers.
#[test]
fn a_partitioned_join_filter_at_the_scan_drops_no_row() {
    let e = engine();
    let sql = "SELECT count(*) AS n FROM t a JOIN t b ON a.who = b.who";
    let shown = plan(&e, sql);
    assert!(
        shown.contains("HashJoinExec: mode=Partitioned")
            && shown.contains("CastViewsExec")
            && shown.contains("DynamicFilter"),
        "no partitioned join over offsets with a filter at its scan, so this does not test it:\n{shown}"
    );
    let (rows, _) = run(&e, sql).unwrap();
    assert_eq!(rows, vec![serde_json::json!({"n": ROWS})]);
}
