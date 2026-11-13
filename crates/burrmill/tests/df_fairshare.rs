//! A stacked view answers under a budget with a spill directory when its aggregates fit, rather than
//! being refused at a share of every operator the plan registered. A final aggregate that has spilled
//! replays its runs through one that cannot spill again, and was refused at a thirtieth of the pool.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use burrmill::{Budget, Engine};

/// The shape of nuthatch's QoS nest: first document per bucket, then bucket, day and indexer grains.
const DAILY: &str = r#"WITH att AS (
    SELECT * FROM attempt
    QUALIFY block * 1000000 + log = min(block * 1000000 + log) OVER (PARTITION BY day, bucket)
), bkt AS (
    SELECT indexer, deployment, chain, gateway, day, bucket,
           sum(q) AS q, sum(ok) AS ok, sum(lat * q) AS lq
    FROM att GROUP BY ALL
), daily AS (
    SELECT indexer, deployment, chain, gateway, day,
           sum(q) AS q, sum(ok) AS ok, count(*) AS buckets,
           count(*) FILTER (WHERE q >= 50 AND ok < 0.9 * q) AS bad,
           arg_min(bucket, [ok / q, bucket::DOUBLE]) FILTER (WHERE q >= 50) AS worst,
           sum(lq) / nullif(sum(q), 0) AS lat
    FROM bkt GROUP BY ALL
)
SELECT indexer, day, sum(q) AS q, sum(ok) AS ok, count(DISTINCT gateway) AS gateways,
       sum(bad) AS bad, min(worst) AS worst
FROM daily WHERE day = 3 GROUP BY indexer, day ORDER BY indexer"#;

/// Four days of `buckets` buckets, each with `allocs` allocations published in one document.
fn segments(segs: &Path, allocs: usize, buckets: usize) {
    std::fs::create_dir_all(segs).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("day", DataType::Int64, false),
        Field::new("bucket", DataType::Int64, false),
        Field::new("block", DataType::Int64, false),
        Field::new("log", DataType::Int64, false),
        Field::new("indexer", DataType::Utf8, false),
        Field::new("deployment", DataType::Utf8, false),
        Field::new("chain", DataType::Utf8, false),
        Field::new("gateway", DataType::Utf8, false),
        Field::new("q", DataType::Int64, false),
        Field::new("ok", DataType::Int64, false),
        Field::new("lat", DataType::Float64, false),
    ]));
    let chains = ["mainnet", "arbitrum-one", "base", "gnosis"];
    for day in 0..4 {
        let rows: Vec<(usize, usize)> = (0..buckets)
            .flat_map(|b| (0..allocs).map(move |a| (b, a)))
            .collect();
        let int = |f: &dyn Fn(usize, usize) -> usize| {
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|&(b, a)| f(b, a) as i64),
            ))
        };
        let text = |f: &dyn Fn(usize, usize) -> String| {
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|&(b, a)| f(b, a)),
            ))
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                int(&|_, _| day),
                int(&|b, _| day * 1000 + b),
                int(&|b, _| 1000 + b),
                int(&|_, _| 7),
                text(&|_, a| format!("0x{:040x}", a % 150)),
                text(&|_, a| format!("Qm{a:044x}")),
                text(&|_, a| chains[a % 4].to_string()),
                text(&|_, a| format!("gw-{}", a % 3)),
                int(&|b, a| (b * 31 + a) % 120),
                int(&|b, a| (b * 17 + a) % 100),
                Arc::new(Float64Array::from_iter_values(
                    rows.iter().map(|&(b, a)| ((b + a) % 500) as f64),
                )),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(segs.join(format!("attempt-{day:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

#[test]
fn a_spilled_aggregate_replays_within_what_the_pool_has_free() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    segments(&segs, 2000, 288);
    let spill = tmp.path().join("spill");
    std::fs::create_dir(&spill).unwrap();
    let budgeted = Engine::builder()
        .segments(&segs)
        .budget(Budget {
            memory_bytes: 512 << 20,
            threads: 4,
            spill: Some((spill, 4 << 30)),
        })
        .open()
        .unwrap();
    let got = budgeted
        .sql(DAILY)
        .unwrap_or_else(|e| panic!("refused: {e}"));
    let want = Engine::builder()
        .segments(&segs)
        .open()
        .unwrap()
        .sql(DAILY)
        .unwrap();
    assert_eq!(got.iter().map(RecordBatch::num_rows).sum::<usize>(), 150);
    assert_eq!(
        arrow::compute::concat_batches(&got[0].schema(), &got).unwrap(),
        arrow::compute::concat_batches(&want[0].schema(), &want).unwrap()
    );
}
