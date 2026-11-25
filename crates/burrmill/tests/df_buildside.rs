//! A hash join builds on the input that reads less, whichever side of `JOIN` it was written on.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn write(segs: &Path, name: &str, k: Vec<Option<i64>>, v: Vec<i64>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(k)), Arc::new(Int64Array::from(v))];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let f = std::fs::File::create(segs.join(format!("{name}-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// 50,000 rows over keys 0..=99 with some NULL, and 12 rows over keys 95..=106: the keys 95..=99
/// match, each side has rows the other lacks.
fn fixture() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let n = 50_000i64;
    write(
        &segs,
        "big",
        (0..n).map(|i| (i % 211 != 0).then_some(i % 100)).collect(),
        (0..n).collect(),
    );
    write(
        &segs,
        "small",
        (0..12).map(|i| Some(95 + i)).collect(),
        (0..12).collect(),
    );
    (tmp, Engine::open_segments(&segs).unwrap())
}

fn one(e: &Engine, sql: &str) -> (String, String) {
    let b = &e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}"))[0];
    let col = |i: usize| {
        let text = arrow::compute::cast(b.column(i), &DataType::Utf8).unwrap();
        text.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    (col(0), col(1))
}

/// The file the join's build side scans: the first scan under the first `HashJoinExec`.
fn build_side(e: &Engine, sql: &str) -> String {
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let kind = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..plans.len() {
            if kind.value(i) != "physical_plan" {
                continue;
            }
            let after = plans
                .value(i)
                .split("HashJoinExec")
                .nth(1)
                .expect("a hash join");
            let scan = after
                .split("segments/")
                .nth(1)
                .expect("a scan under the join");
            return scan.split('-').next().unwrap().to_string();
        }
    }
    panic!("no physical plan for {sql}");
}

#[test]
fn the_build_side_is_the_smaller_input_and_the_answer_is_the_same() {
    let (_tmp, e) = fixture();
    for join in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        let big_first = format!(
            "SELECT count(*), coalesce(sum(b.v + s.v), 0) FROM big b {join} small s ON b.k = s.k"
        );
        assert_eq!(build_side(&e, &big_first), "small", "{big_first}");
        // The same join written the other way round, which DataFusion already builds on `small`.
        let mirrored = match join {
            "LEFT JOIN" => "RIGHT JOIN",
            "RIGHT JOIN" => "LEFT JOIN",
            j => j,
        };
        let small_first = format!(
            "SELECT count(*), coalesce(sum(b.v + s.v), 0) FROM small s {mirrored} big b ON b.k = s.k"
        );
        assert_eq!(build_side(&e, &small_first), "small", "{small_first}");
        assert_eq!(one(&e, &big_first), one(&e, &small_first), "{join}");
    }
    // Brute force for the inner join: keys 95..=99 match one small row each.
    let matched = (0..50_000i64)
        .filter(|i| i % 211 != 0 && i % 100 >= 95)
        .count() as i64;
    assert_eq!(
        one(
            &e,
            "SELECT count(*), 0 FROM big b JOIN small s ON b.k = s.k"
        )
        .0,
        matched.to_string()
    );
}

/// A semi or anti join holds its build side too: the QoS nest's rows, written first and filtered by
/// two small tables, were held whole in each of two nested semi joins and refused out of memory.
#[test]
fn a_semi_or_anti_join_builds_on_the_smaller_input_and_the_answer_is_the_same() {
    let (_tmp, e) = fixture();
    let keep = |anti: bool| {
        (0..50_000i64)
            .filter(|i| {
                let k = (i % 211 != 0).then_some(i % 100);
                k.is_some_and(|k| k >= 95) != anti
            })
            .collect::<Vec<_>>()
    };
    for (sql, anti) in [
        // DataFusion makes this inner join a semi join, the DISTINCT above being blind to duplicates.
        (
            "SELECT count(*), coalesce(sum(v), 0) FROM (SELECT DISTINCT b.k, b.v FROM big b JOIN small s ON b.k = s.k)",
            false,
        ),
        (
            "SELECT count(*), coalesce(sum(b.v), 0) FROM big b WHERE EXISTS (SELECT 1 FROM small s WHERE s.k = b.k)",
            false,
        ),
        (
            "SELECT count(*), coalesce(sum(b.v), 0) FROM big b WHERE NOT EXISTS (SELECT 1 FROM small s WHERE s.k = b.k)",
            true,
        ),
    ] {
        assert_eq!(build_side(&e, sql), "small", "{sql}");
        let rows = keep(anti);
        let want = (rows.len().to_string(), rows.iter().sum::<i64>().to_string());
        assert_eq!(one(&e, sql), want, "{sql}");
    }
}

/// `NOT IN` is a null-aware anti join, which DataFusion cannot swap: a NULL key is never kept.
#[test]
fn not_in_keeps_its_answer() {
    let (_tmp, e) = fixture();
    let rows: Vec<i64> = (0..50_000i64)
        .filter(|i| i % 211 != 0 && i % 100 < 95)
        .collect();
    let want = (rows.len().to_string(), rows.iter().sum::<i64>().to_string());
    let sql =
        "SELECT count(*), coalesce(sum(b.v), 0) FROM big b WHERE b.k NOT IN (SELECT k FROM small)";
    assert_eq!(one(&e, sql), want, "{sql}");
}

struct Q12 {
    _tmp: tempfile::TempDir,
    e: Engine,
    lines: Vec<[i64; 5]>,
    ords: Vec<(i64, i64)>,
}

/// A table of `lines` larger on disk than `ords`, whose filter keeps a few of its rows: Q12's
/// shape (#433). `k` is an order key, `mode` one of seven, `a`, `b`, `c` dates as day numbers.
fn q12_fixture() -> Q12 {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = |m: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % m) as i64
    };
    let orders = 10_000i64;
    let ords: Vec<(i64, i64)> = (0..orders).map(|k| (k, next(1 << 40))).collect();
    let lines: Vec<[i64; 5]> = (0..4 * orders)
        .map(|_| {
            let a = next(2_500);
            [next(orders as u64), next(7), a, a + next(30), a + next(60)]
        })
        .collect();
    let modes = ["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];
    let int = |name: &str| Field::new(name, DataType::Int64, false);
    let put = |name: &str, fields: Vec<Field>, arrays: Vec<ArrayRef>| {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let f = std::fs::File::create(segs.join(format!("{name}-{:064x}.parquet", 1))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    };
    let col = |i: usize| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(lines.iter().map(|l| l[i])))
    };
    put(
        "lines",
        vec![
            int("k"),
            Field::new("mode", DataType::Utf8, false),
            int("a"),
            int("b"),
            int("c"),
        ],
        vec![
            col(0),
            Arc::new(StringArray::from_iter_values(
                lines.iter().map(|l| modes[l[1] as usize]),
            )),
            col(2),
            col(3),
            col(4),
        ],
    );
    put(
        "ords",
        vec![int("k"), int("v")],
        vec![
            Arc::new(Int64Array::from_iter_values(ords.iter().map(|o| o.0))),
            Arc::new(Int64Array::from_iter_values(ords.iter().map(|o| o.1))),
        ],
    );
    let size = |name: &str| {
        std::fs::metadata(segs.join(format!("{name}-{:064x}.parquet", 1)))
            .unwrap()
            .len()
    };
    // On the bytes read alone the join would build on `ords`.
    assert!(
        size("lines") > size("ords"),
        "{} {}",
        size("lines"),
        size("ords")
    );
    let e = Engine::open_segments(&segs).unwrap();
    // Planned across threads, as a large statement is, so that a join can be partitioned.
    e.set_small_statement_bytes(0);
    Q12 {
        _tmp: tmp,
        e,
        lines,
        ords,
    }
}

/// Q12 at SF10 filters `lineitem` to 310.8K of its 60M rows and joined `orders` to them by building
/// on all 15M orders, the side that reads fewer bytes. A filter that plainly keeps a few rows now
/// makes its side the build side, collected; one that keeps most, or might, does not.
#[test]
fn a_side_whose_filter_keeps_few_rows_is_built_on_and_collected() {
    let Q12 {
        _tmp,
        e,
        lines,
        ords,
    } = q12_fixture();
    let filter = "l.mode IN ('MAIL', 'SHIP') AND l.b < l.c AND l.a < l.b \
                  AND l.c >= 1000 AND l.c < 1100";
    let kept = |l: &[i64; 5]| {
        (l[1] == 2 || l[1] == 5) && l[3] < l[4] && l[2] < l[3] && (1000..1100).contains(&l[4])
    };
    for (from, on) in [
        ("lines l JOIN ords o", "l.k = o.k"),
        ("ords o JOIN lines l", "o.k = l.k"),
    ] {
        let sql = format!(
            "SELECT count(*), coalesce(sum(o.v % 1000), 0) FROM {from} ON {on} WHERE {filter}"
        );
        assert_eq!(build_side(&e, &sql), "lines", "{sql}");
        assert!(
            plan(&e, &sql).contains("HashJoinExec: mode=CollectLeft"),
            "{sql}"
        );
        let rows: Vec<i64> = lines
            .iter()
            .filter(|l| kept(l))
            .map(|l| ords[l[0] as usize].1 % 1000)
            .collect();
        assert!(
            !rows.is_empty() && rows.len() < lines.len() / 100,
            "{}",
            rows.len()
        );
        let want = (rows.len().to_string(), rows.iter().sum::<i64>().to_string());
        assert_eq!(one(&e, &sql), want, "{sql}");
    }
    // A filter that keeps most rows, an equality that might, or one it cannot read, leaves the
    // build on the smaller read.
    for weak in [
        "l.mode NOT LIKE 'R%'",
        "l.mode = 'MAIL'",
        "starts_with(l.mode, 'M')",
    ] {
        let sql = format!("SELECT count(*), 0 FROM lines l JOIN ords o ON l.k = o.k WHERE {weak}");
        assert_eq!(build_side(&e, &sql), "ords", "{sql}");
        assert!(!plan(&e, &sql).contains("CollectLeft"), "{sql}");
    }
}

/// Q4 at SF10 keeps the 573.7K orders of one quarter and asks which have a late `lineitem` row. The
/// semi join built on those orders partitioned, so the dynamic filter it gave the `lineitem` scan
/// was a `CASE` over each of its 60M rows' repartition hash: 1.77 s of row-filter CPU, against
/// 0.39 s collected (#433). A semi join whose build side is guessed under an eighth of what its
/// probe reads is now collected; one whose build side is not filtered is not.
#[test]
fn a_semi_join_on_a_few_filtered_rows_is_collected() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = |m: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % m) as i64
    };
    let orders = 10_000i64;
    let ords: Vec<(i64, i64)> = (0..orders).map(|k| (k, next(1 << 40))).collect();
    // `k`, `a`, `b`, `c` as in Q12's fixture; the note makes `lines` read about as many times
    // `ords` as Q4's `lineitem` does its `orders`.
    let lines: Vec<[i64; 5]> = (0..4 * orders)
        .map(|_| {
            let a = next(2_500);
            [next(orders as u64), 0, a, a + next(30), a + next(60)]
        })
        .collect();
    let notes: Vec<String> = (0..lines.len())
        .map(|_| format!("{:010x}", next(1 << 40)))
        .collect();
    let int = |name: &str| Field::new(name, DataType::Int64, false);
    let put = |name: &str, fields: Vec<Field>, arrays: Vec<ArrayRef>| {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let path = segs.join(format!("{name}-{:064x}.parquet", 1));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        std::fs::metadata(&path).unwrap().len() as f64
    };
    let col = |i: usize| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(lines.iter().map(|l| l[i])))
    };
    let line_bytes = put(
        "lines",
        vec![
            int("k"),
            int("b"),
            int("c"),
            Field::new("note", DataType::Utf8, false),
        ],
        vec![
            col(0),
            col(3),
            col(4),
            Arc::new(StringArray::from_iter_values(notes.iter())),
        ],
    );
    let ord_bytes = put(
        "ords",
        vec![int("k"), int("v")],
        vec![
            Arc::new(Int64Array::from_iter_values(ords.iter().map(|o| o.0))),
            Arc::new(Int64Array::from_iter_values(ords.iter().map(|o| o.1))),
        ],
    );
    // Over an eighth of `lines` unfiltered, under it with a range's guessed third.
    let ratio = line_bytes / ord_bytes;
    assert!((8.0 / 3.0..8.0).contains(&ratio), "{ratio}");
    let e = Engine::open_segments(&segs).unwrap();
    e.set_small_statement_bytes(0);
    let (lo, hi) = (1i64 << 38, 1i64 << 39);
    let sql = format!(
        "SELECT count(*), coalesce(sum(o.v % 1000), 0) FROM ords o \
         WHERE o.v >= {lo} AND o.v < {hi} \
         AND EXISTS (SELECT 1 FROM lines l WHERE l.k = o.k AND l.b < l.c)"
    );
    assert_eq!(build_side(&e, &sql), "ords", "{sql}");
    assert!(
        plan(&e, &sql).contains("HashJoinExec: mode=CollectLeft, join_type=LeftSemi"),
        "{}",
        plan(&e, &sql)
    );
    let late: std::collections::HashSet<i64> =
        lines.iter().filter(|l| l[3] < l[4]).map(|l| l[0]).collect();
    let rows: Vec<i64> = ords
        .iter()
        .filter(|o| (lo..hi).contains(&o.1) && late.contains(&o.0))
        .map(|o| o.1 % 1000)
        .collect();
    assert!(
        !rows.is_empty() && rows.len() < ords.len() / 2,
        "{}",
        rows.len()
    );
    let want = (rows.len().to_string(), rows.iter().sum::<i64>().to_string());
    assert_eq!(one(&e, &sql), want, "{sql}");
    // Every order, unfiltered, is not guessed small enough to collect.
    let all = "SELECT count(*), 0 FROM ords o \
               WHERE EXISTS (SELECT 1 FROM lines l WHERE l.k = o.k AND l.b < l.c)";
    assert!(!plan(&e, all).contains("CollectLeft"), "{}", plan(&e, all));
}

fn plan(e: &Engine, sql: &str) -> String {
    let mut out = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..plans.len() {
            out.push_str(plans.value(i));
        }
    }
    out
}
