//! nuthatch #1849: SUM and AVG over DOUBLE give the same bits however the rows are split into
//! segments, batches and partitions, and those bits are the correctly rounded exact sum.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const ROWS: usize = 20_000;

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// `(k, g, v)`: magnitudes across sixty binary orders and both signs, so that where a partial sum
/// is rounded decides its last bits.
fn rows() -> Vec<(i64, i64, f64)> {
    let mut s = 1849;
    (0..ROWS)
        .map(|k| {
            let r = splitmix(&mut s);
            let mant = (r >> 11) as f64 / (1u64 << 53) as f64;
            let exp = (r % 61) as i32 - 30;
            let sign = if r & (1 << 10) == 0 { 1.0 } else { -1.0 };
            (k as i64, (r >> 7) as i64 % 7, sign * mant * 2f64.powi(exp))
        })
        .collect()
}

/// Python's `math.fsum`: Shewchuk's exact partials, then one correctly rounded result.
fn fsum(xs: impl IntoIterator<Item = f64>) -> f64 {
    let mut p: Vec<f64> = Vec::new();
    for mut x in xs {
        let mut i = 0;
        for j in 0..p.len() {
            let mut y = p[j];
            if x.abs() < y.abs() {
                std::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            let lo = y - (hi - x);
            if lo != 0.0 {
                p[i] = lo;
                i += 1;
            }
            x = hi;
        }
        p.truncate(i);
        p.push(x);
    }
    let Some(mut n) = p.len().checked_sub(1) else {
        return 0.0;
    };
    let mut hi = p[n];
    let mut lo = 0.0;
    while n > 0 {
        let x = hi;
        n -= 1;
        let y = p[n];
        hi = x + y;
        lo = y - (hi - x);
        if lo != 0.0 {
            break;
        }
    }
    if n > 0 && ((lo < 0.0 && p[n - 1] < 0.0) || (lo > 0.0 && p[n - 1] > 0.0)) {
        let y = lo * 2.0;
        let x = hi + y;
        if y == x - hi {
            hi = x;
        }
    }
    hi
}

/// The rows shuffled by `seed` and written as `files` segments.
fn segments(dir: &Path, rows: &[(i64, i64, f64)], files: usize, seed: u64) -> Vec<(PathBuf, u64)> {
    let mut rows = rows.to_vec();
    let mut s = seed;
    for i in (1..rows.len()).rev() {
        rows.swap(i, (splitmix(&mut s) % (i as u64 + 1)) as usize);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let per = rows.len().div_ceil(files);
    rows.chunks(per)
        .enumerate()
        .map(|(i, c)| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(c.iter().map(|r| r.0))),
                    Arc::new(Int64Array::from_iter_values(c.iter().map(|r| r.1))),
                    Arc::new(Float64Array::from_iter_values(c.iter().map(|r| r.2))),
                ],
            )
            .unwrap();
            let path = dir.join(format!("t-{seed:032x}{i:032x}.parquet"));
            let f = std::fs::File::create(&path).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            let len = std::fs::metadata(&path).unwrap().len();
            (path, len)
        })
        .collect()
}

fn engine(threads: Option<usize>, files: Vec<(PathBuf, u64)>) -> Engine {
    let mut e = match threads {
        Some(threads) => Engine::open_empty_budgeted(burrmill::Budget {
            memory_bytes: 256 << 20,
            threads,
            spill: None,
        }),
        None => Engine::open_empty(),
    }
    .unwrap();
    // Every partition count is the point; small, each would be one.
    e.set_small_statement_bytes(0);
    e.register_facts("t", &[], files, &[], (None, None))
        .unwrap();
    e
}

/// Every cell, a DOUBLE as its bits.
fn answer(e: &Engine, sql: &str) -> Vec<Vec<String>> {
    let batches = e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err:?}"));
    let mut out = Vec::new();
    for b in batches {
        for i in 0..b.num_rows() {
            out.push(b.columns().iter().map(|c| cell(c, i)).collect::<Vec<_>>());
        }
    }
    out
}

fn cell(c: &ArrayRef, i: usize) -> String {
    if c.is_null(i) {
        return "NULL".into();
    }
    match c.data_type() {
        DataType::Float64 => bits(c.as_primitive::<Float64Type>().value(i)),
        DataType::Int64 => c.as_primitive::<Int64Type>().value(i).to_string(),
        t => panic!("unexpected {t}"),
    }
}

fn bits(x: f64) -> String {
    format!("{:016x}", x.to_bits())
}

const QUERIES: &[&str] = &[
    "SELECT sum(v), avg(v) FROM t",
    "SELECT g, sum(v), avg(v) FROM t GROUP BY g ORDER BY g",
    "SELECT DISTINCT g, sum(v) OVER (PARTITION BY g) FROM t ORDER BY g",
    "SELECT g, sum(v) FROM (SELECT g, v FROM t UNION ALL SELECT g, -v / 3 FROM t) GROUP BY g ORDER BY g",
];

#[test]
fn double_sums_do_not_depend_on_partitioning() {
    let tmp = tempfile::tempdir().unwrap();
    let data = rows();
    let mut first: Option<Vec<Vec<Vec<String>>>> = None;
    for (files, seed) in [(1, 1u64), (4, 2), (13, 3), (40, 4)] {
        let dir = tmp.path().join(format!("l{seed}"));
        std::fs::create_dir(&dir).unwrap();
        let segs = segments(&dir, &data, files, seed);
        for threads in [None, Some(1), Some(3), Some(8)] {
            let e = engine(threads, segs.clone());
            let got: Vec<_> = QUERIES.iter().map(|q| answer(&e, q)).collect();
            match &first {
                None => first = Some(got),
                Some(f) => {
                    for (q, (a, b)) in QUERIES.iter().zip(f.iter().zip(&got)) {
                        assert_eq!(a, b, "{q}: {files} segments, threads {threads:?}");
                    }
                }
            }
        }
    }
}

#[test]
fn double_sums_are_correctly_rounded() {
    let tmp = tempfile::tempdir().unwrap();
    let data = rows();
    let e = engine(Some(4), segments(tmp.path(), &data, 7, 9));
    let total = fsum(data.iter().map(|r| r.2));
    assert_eq!(
        answer(&e, "SELECT sum(v), avg(v) FROM t"),
        vec![vec![bits(total), bits(total / ROWS as f64)]]
    );
    let by_group: Vec<Vec<String>> = (0..7)
        .map(|g| {
            let vs: Vec<f64> = data.iter().filter(|r| r.1 == g).map(|r| r.2).collect();
            let s = fsum(vs.iter().copied());
            vec![g.to_string(), bits(s), bits(s / vs.len() as f64)]
        })
        .collect();
    assert_eq!(
        answer(&e, "SELECT g, sum(v), avg(v) FROM t GROUP BY g ORDER BY g"),
        by_group
    );
    // A sliding frame takes rows back out, and must land where summing the frame afresh does.
    let w = answer(
        &e,
        "SELECT k, sum(v) OVER (ORDER BY k ROWS BETWEEN 37 PRECEDING AND CURRENT ROW) \
         FROM t ORDER BY k",
    );
    for k in (0..ROWS).step_by(997).chain([ROWS - 1]) {
        let s = fsum(data[k.saturating_sub(37)..=k].iter().map(|r| r.2));
        assert_eq!(w[k], vec![k.to_string(), bits(s)], "frame ending at {k}");
    }
}

/// An infinity in one segment and its opposite in another meet only in the final merge.
#[test]
fn infinities_survive_the_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let parts: [&[f64]; 3] = [
        &[1.0, f64::INFINITY],
        &[2.0, 3.0],
        &[f64::NEG_INFINITY, 4.0],
    ];
    let files: Vec<_> = parts
        .iter()
        .enumerate()
        .flat_map(|(i, vs)| {
            let rows: Vec<_> = vs.iter().map(|&v| (0, 0, v)).collect();
            let dir = tmp.path().join(format!("p{i}"));
            std::fs::create_dir(&dir).unwrap();
            segments(&dir, &rows, 1, i as u64)
        })
        .collect();
    let pos = &files[..2];
    for threads in [Some(1), Some(3)] {
        let e = engine(threads, files.clone());
        for q in ["SELECT sum(v) FROM t", "SELECT g, sum(v) FROM t GROUP BY g"] {
            let got = answer(&e, q);
            assert!(is_nan(got[0].last().unwrap()), "{q}: {got:?}");
        }
        let e = engine(threads, pos.to_vec());
        assert_eq!(
            answer(&e, "SELECT g, sum(v) FROM t GROUP BY g"),
            vec![vec!["0".to_string(), bits(f64::INFINITY)]]
        );
        assert_eq!(one(&e, "SELECT sum(v) FROM t"), bits(f64::INFINITY));
    }
}

fn one(e: &Engine, sql: &str) -> String {
    answer(e, sql)[0].join(" ")
}

fn is_nan(cell: &str) -> bool {
    f64::from_bits(u64::from_str_radix(cell, 16).unwrap()).is_nan()
}

#[test]
fn double_sum_edges() {
    let e = Engine::open_empty().unwrap();
    let over = |f: &str, list: &[&str]| {
        let rows: Vec<String> = list.iter().map(|x| format!("({x})")).collect();
        one(
            &e,
            &format!(
                "SELECT {f}(CAST(x AS DOUBLE)) FROM (VALUES {}) AS u(x)",
                rows.join(", ")
            ),
        )
    };
    let sum = |list: &[&str]| over("sum", list);
    assert_eq!(sum(&["NULL"]), "NULL");
    assert_eq!(sum(&["'-0.0'"]), bits(0.0));
    assert_eq!(sum(&["'-0.0'", "'-0.0'"]), bits(0.0));
    assert_eq!(sum(&["'1'", "'inf'"]), bits(f64::INFINITY));
    assert_eq!(sum(&["'-inf'", "'1'", "NULL"]), bits(f64::NEG_INFINITY));
    assert!(is_nan(&sum(&["'inf'", "'-inf'"])));
    assert!(is_nan(&sum(&["'nan'", "'1'"])));
    assert_eq!(sum(&["'1e308'", "'1e308'"]), bits(f64::INFINITY));
    // Exact, an intermediate overflow cancels: only the true sum is rounded.
    assert_eq!(sum(&["'1e308'", "'1e308'", "'-1e308'"]), bits(1e308));
    assert_eq!(sum(&["'1e308'", "'-1e308'", "'1e308'"]), bits(1e308));
    assert_eq!(sum(&["'5e-324'", "'5e-324'"]), bits(1e-323));
    assert_eq!(sum(&["'1e16'", "'1'", "'-1e16'"]), bits(1.0));
    assert_eq!(over("avg", &["'1'", "'2'", "NULL"]), bits(1.5));
    assert_eq!(over("avg", &["NULL"]), "NULL");
}

/// Keyed as finely as its rows, a partial aggregate stops grouping after its first 100,000 rows and
/// passes each row on as its own state; the sums still land on the exact ones.
#[test]
fn fine_keys_sum_exactly() {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = 1895;
    let data: Vec<(i64, i64, f64)> = (0..400_000)
        .map(|k| {
            let r = splitmix(&mut s);
            let v = match r % 97 {
                0 => -0.0,
                1 => f64::from_bits(r >> 12),
                _ => {
                    let mant = (r >> 11) as f64 / (1u64 << 53) as f64;
                    let sign = if r & (1 << 10) == 0 { 1.0 } else { -1.0 };
                    sign * mant * 2f64.powi((r % 41) as i32 - 20)
                }
            };
            (k, k / 2, v)
        })
        .collect();
    let segs = segments(tmp.path(), &data, 3, 5);
    for threads in [Some(2), Some(3)] {
        let e = engine(threads, segs.clone());
        for (key, per) in [("k", 1), ("g", 2)] {
            let got = answer(
                &e,
                &format!("SELECT {key}, sum(v), avg(v) FROM t GROUP BY {key} ORDER BY {key}"),
            );
            let want: Vec<Vec<String>> = data
                .chunks(per)
                .map(|c| {
                    // An exact zero is +0, as `double_sum_edges` has it.
                    let s = fsum(c.iter().map(|r| r.2)) + 0.0;
                    let k = if per == 1 { c[0].0 } else { c[0].1 };
                    vec![k.to_string(), bits(s), bits(s / c.len() as f64)]
                })
                .collect();
            let bad = got.iter().zip(&want).position(|(a, b)| a != b);
            assert!(
                got.len() == want.len() && bad.is_none(),
                "GROUP BY {key}, threads {threads:?}: {} rows, first wrong {:?}",
                got.len(),
                bad.map(|i| (&got[i], &want[i]))
            );
        }
    }
}
