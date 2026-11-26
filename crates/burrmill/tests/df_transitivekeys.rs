//! TPC-H Q5's shape: a table equated with a filtered dimension's key only through another table,
//! and an unfiltered probe joined to a filtered build (#433).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

struct Q5 {
    _tmp: tempfile::TempDir,
    e: Engine,
    region: Vec<i64>,
    customer: Vec<i64>,
    orders: Vec<[i64; 2]>,
    lines: Vec<[i64; 3]>,
    supplier: Vec<i64>,
}

/// Five regions of five nations each; customers and suppliers in a nation each; orders of a
/// customer on a day, and lines of an order from a supplier with a value.
fn q5() -> Q5 {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let mut seed = 0x510e_527f_ade6_82d1u64;
    let mut next = |m: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % m) as i64
    };
    let region: Vec<i64> = (0..25).map(|n| n % 5).collect();
    let customer: Vec<i64> = (0..20_000).map(|_| next(25)).collect();
    let supplier: Vec<i64> = (0..1_000).map(|_| next(25)).collect();
    let orders: Vec<[i64; 2]> = (0..100_000)
        .map(|_| [next(customer.len() as u64), next(2_555)])
        .collect();
    let lines: Vec<[i64; 3]> = (0..600_000)
        .map(|_| {
            [
                next(orders.len() as u64),
                next(supplier.len() as u64),
                next(1 << 30),
            ]
        })
        .collect();
    let put = |name: &str, cols: Vec<(&str, ArrayRef)>| {
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|(n, a)| Field::new(*n, a.data_type().clone(), false))
                .collect::<Vec<_>>(),
        ));
        let batch =
            RecordBatch::try_new(schema.clone(), cols.into_iter().map(|c| c.1).collect()).unwrap();
        let f = std::fs::File::create(segs.join(format!("{name}-{:064x}.parquet", 1))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    };
    let ints = |v: Vec<i64>| -> ArrayRef { Arc::new(Int64Array::from(v)) };
    let seq = |n: usize| ints((0..n as i64).collect());
    let names = |p: &str, n: usize| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| format!("{p}{i}")),
        ))
    };
    put("region", vec![("rk", seq(5)), ("rname", names("R", 5))]);
    put(
        "nation",
        vec![
            ("nk", seq(25)),
            ("rk", ints(region.clone())),
            ("nname", names("N", 25)),
        ],
    );
    put(
        "customer",
        vec![("ck", seq(customer.len())), ("nk", ints(customer.clone()))],
    );
    put(
        "supplier",
        vec![("sk", seq(supplier.len())), ("nk", ints(supplier.clone()))],
    );
    put(
        "orders",
        vec![
            ("ok", seq(orders.len())),
            ("ck", ints(orders.iter().map(|o| o[0]).collect())),
            ("d", ints(orders.iter().map(|o| o[1]).collect())),
        ],
    );
    put(
        "lines",
        vec![
            ("ok", ints(lines.iter().map(|l| l[0]).collect())),
            ("sk", ints(lines.iter().map(|l| l[1]).collect())),
            ("v", ints(lines.iter().map(|l| l[2]).collect())),
        ],
    );
    let e = Engine::open_segments(&segs).unwrap();
    e.set_small_statement_bytes(0);
    Q5 {
        _tmp: tmp,
        e,
        region,
        customer,
        orders,
        lines,
        supplier,
    }
}

/// Q5 over the fixture: the lines of a year's orders whose customer and supplier share a nation of
/// region 2, summed by nation. `extra` is ANDed to the `WHERE`.
fn sql(nations: &str, extra: &str) -> String {
    format!(
        "SELECT n.nname, sum(l.v) FROM customer c, orders o, lines l, supplier s, nation n, region r \
         WHERE c.ck = o.ck AND l.ok = o.ok AND l.sk = s.sk AND {nations} AND s.nk = n.nk \
         AND n.rk = r.rk AND r.rname = 'R2' AND o.d >= 365 AND o.d < 730{extra} \
         GROUP BY n.nname ORDER BY n.nname"
    )
}

fn plan(e: &Engine, sql: &str) -> String {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        for row in burrmill::df::encode::rows(&b).unwrap() {
            plan.push_str(&row.to_string());
        }
    }
    plan
}

fn rows(e: &Engine, sql: &str) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let text = arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
        let names = text.as_any().downcast_ref::<StringArray>().unwrap();
        let sums = arrow::compute::cast(b.column(1), &DataType::Int64).unwrap();
        let sums = sums.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((names.value(i).to_string(), sums.value(i)));
        }
    }
    out
}

impl Q5 {
    /// The answer by brute force.
    fn want(&self) -> Vec<(String, i64)> {
        let mut by: HashMap<i64, i64> = HashMap::new();
        for l in &self.lines {
            let [ck, d] = self.orders[l[0] as usize];
            let nk = self.customer[ck as usize];
            if (365..730).contains(&d)
                && self.supplier[l[1] as usize] == nk
                && self.region[nk as usize] == 2
            {
                *by.entry(nk).or_default() += l[2];
            }
        }
        let mut out: Vec<(String, i64)> =
            by.into_iter().map(|(n, v)| (format!("N{n}"), v)).collect();
        out.sort();
        out
    }
}

/// Q5's `customer` is equated with `nation` only through `supplier`, `c_nationkey = s_nationkey`,
/// so the nations of one region never reached it, and at SF10 its joins carried 2.28M orders and
/// 9.10M of their lines where DuckDB carries 456.8K and 1.83M. `TransitiveKeys` gives `customer` a
/// semi join on the region's nations; a customer joined to `nation` itself, or a region filtered
/// with something volatile, is left alone.
#[test]
fn a_table_equated_with_a_filtered_dimension_through_another_reads_its_keys() {
    let q = q5();
    let want = q.want();
    assert!(want.len() == 5, "{want:?}");
    let reduced = "join_type=RightSemi, on=[(__transitive_key@0, nk@";
    let through_supplier = sql("c.nk = s.nk", "");
    let p = plan(&q.e, &through_supplier);
    assert!(p.contains(reduced), "{p}");
    assert_eq!(rows(&q.e, &through_supplier), want);
    // The same query written with the nation key on both sides of the supplier join, as Q5 is.
    let both = sql("c.nk = s.nk AND s.nk = c.nk", "");
    assert_eq!(rows(&q.e, &both), want);
    for (sql, why) in [
        (
            sql("c.nk = n.nk AND c.nk = s.nk", ""),
            "joined to the nation itself",
        ),
        (sql("c.nk = s.nk", " AND r.rk + random() < 100"), "volatile"),
    ] {
        let p = plan(&q.e, &sql);
        assert!(!p.contains("__transitive_key"), "{why}: {p}");
        assert_eq!(rows(&q.e, &sql), want, "{why}");
    }
}

/// Partitioned, the join of Q5's year of orders to `lineitem` gave the unfiltered `lineitem` scan a
/// dynamic filter that is a `CASE` over each row's repartition hash: 2.77 s of its row-filter CPU at
/// SF10, against 0.77 s collected (#433). A filtered build side guessed at a quarter or less of an
/// unfiltered probe is collected; a probe with a filter of its own is not.
#[test]
fn a_filtered_build_under_an_unfiltered_probe_is_collected() {
    let q = q5();
    let collected = |p: &str| {
        p.lines().any(|l| {
            l.contains("HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(ok@")
                && l.contains(", ok@0)]")
        })
    };
    let unfiltered = sql("c.nk = s.nk", "");
    let p = plan(&q.e, &unfiltered);
    assert!(collected(&p), "{p}");
    assert_eq!(rows(&q.e, &unfiltered), q.want());
    let filtered = sql("c.nk = s.nk", " AND l.v % 2 = 0");
    let p = plan(&q.e, &filtered);
    assert!(
        p.contains("join_type=Inner, on=[(ok@") && !collected(&p),
        "{p}"
    );
}
