//! #110: since #107 a running max over a growing frame plans as a `WindowAggExec`. At one target
//! partition, two such branches unioned under an aggregate failed in `EnsureRequirements`: it built
//! an InterleaveExec over their one-partition hash repartitions, then its sort enforcement changed a
//! child's partitioning under it ("Can not create InterleaveExec").

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use burrmill::Engine;

const INDEXERS: u64 = 7;
/// An event's indexer, value and block.
type Row = (String, String, u64);
const BLOCKS: u64 = 100_000;

/// 64 hex characters that do not compress. The planner sizes a scan by its files, and only scans
/// of a real event table's weight keep the window's sort above the repartition, which is the case.
fn memo(block: u64) -> String {
    let mut x = block.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (0..4)
        .map(|_| {
            x ^= x >> 31;
            x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            format!("{x:016x}")
        })
        .collect()
}

/// One segment: `indexer`, `value`, `block_number`, `log_index`, as nuthatch seals an event.
fn segment(dir: &Path, table: &str, rows: &[Row]) -> (PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("indexer", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("memo", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.0.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.1.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|_| 0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| memo(r.2)),
            )),
        ],
    )
    .unwrap();
    let path = dir.join(format!("{table}-{:064x}.parquet", rows.len()));
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

/// Cuts on a few blocks in every thousand, rewards on the rest.
fn tables() -> [(&'static str, Vec<Row>); 3] {
    let who = |b: u64| format!("0x{:X}", b % INDEXERS);
    let rows = |keep: fn(u64) -> bool, value: fn(u64) -> u64| {
        (1..BLOCKS)
            .filter(|b| keep(*b))
            .map(|b| (who(b), value(b).to_string(), b))
            .collect::<Vec<_>>()
    };
    [
        ("cuts", rows(|b| b % 1000 < 2 * INDEXERS, |b| b % 13)),
        (
            "rewards_a",
            rows(|b| b % 1000 >= 2 * INDEXERS && b % 2 == 0, |b| b % 97),
        ),
        (
            "rewards_b",
            rows(|b| b % 1000 >= 2 * INDEXERS && b % 2 == 1, |b| b % 89),
        ),
    ]
}

/// Each reward times the newest cut of its indexer at or before it, summed per indexer: a reward
/// before any cut of its indexer drops, as the inner join drops it.
fn expected() -> String {
    use std::collections::BTreeMap;
    let [cuts, a, b] = tables();
    let mut by_sp: BTreeMap<String, BTreeMap<u64, u128>> = BTreeMap::new();
    for (sp, cut, block) in &cuts.1 {
        by_sp
            .entry(sp.clone())
            .or_default()
            .insert(*block, cut.parse().unwrap());
    }
    let mut paid = BTreeMap::<String, u128>::new();
    for (sp, amount, block) in a.1.iter().chain(b.1.iter()) {
        if let Some((_, cut)) = by_sp.get(sp).and_then(|c| c.range(..=*block).next_back()) {
            *paid.entry(sp.to_lowercase()).or_default() += amount.parse::<u128>().unwrap() * cut;
        }
    }
    paid.into_iter()
        .map(|(sp, paid)| format!("{sp} {paid}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The three tables, from local files as nuthatch binds them, or from an object store as a tab does.
fn engine(dir: &Path, threads: usize, from_store: bool) -> Engine {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 256 << 20,
        threads,
        spill: None,
    })
    .unwrap();
    let store = Arc::new(object_store::memory::InMemory::new());
    if from_store {
        e.attach_store(store.clone()).unwrap();
    }
    for (table, rows) in tables() {
        let (path, len) = segment(dir, table, &rows);
        if from_store {
            use object_store::ObjectStoreExt;
            let key = format!(
                "nest/segments/{}",
                path.file_name().unwrap().to_string_lossy()
            );
            futures::executor::block_on(async {
                let bytes = std::fs::read(&path).unwrap();
                store
                    .put(&object_store::path::Path::from(key.as_str()), bytes.into())
                    .await
                    .unwrap();
                e.register_store_facts(table, &[], vec![(key, len)], &[], (None, None))
                    .await
                    .unwrap();
            });
        } else {
            e.register_facts(table, &[], vec![(path, len)], &[], (None, None))
                .unwrap();
        }
    }
    e
}

/// The shape of the Lodestar nest's `lodestar_indexer_ledger` reward shares: each branch carries the
/// newest cut forward over a union by a running max, joins the cut back on it, and the branches are
/// unioned and summed per indexer.
fn branch(rewards: &str) -> String {
    format!(
        "SELECT r.sp, r.amount * c.cut AS t FROM (
           SELECT sp, k, amount, ck FROM (
             SELECT sp, k, amount, src,
                    MAX(CASE WHEN src = 0 THEN k END) OVER (PARTITION BY sp ORDER BY k, src ROWS UNBOUNDED PRECEDING) AS ck
             FROM (SELECT sp, k, 0 AS src, CAST(NULL AS HUGEINT) AS amount FROM cuts_k
                   UNION ALL SELECT LOWER(indexer), block_number * 100000 + log_index, 2, CAST(value AS HUGEINT) FROM {rewards})
           ) WHERE src = 2
         ) r JOIN cuts_k c ON c.sp = r.sp AND c.k = r.ck"
    )
}

/// The answer as `indexer paid` lines.
fn answer(threads: usize, from_store: bool) -> String {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine(dir.path(), threads, from_store);
    let body = format!(
        "WITH cuts_k AS (SELECT LOWER(indexer) AS sp, CAST(value AS HUGEINT) AS cut, block_number * 100000 + log_index AS k FROM cuts)
         SELECT sp, SUM(t) AS paid FROM ({} UNION ALL {}) GROUP BY sp",
        branch("rewards_a"),
        branch("rewards_b"),
    );
    e.register_view("paid", &body).unwrap();
    let batches = e
        .sql("SELECT sp, CAST(paid AS VARCHAR) FROM paid ORDER BY sp")
        .unwrap_or_else(|err| panic!("{threads} thread(s), store {from_store}: {err}"));
    pretty_format_batches(&batches)
        .unwrap()
        .to_string()
        .lines()
        .filter(|l| l.starts_with("| 0x"))
        .map(|l| {
            l.split('|')
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn a_running_window_under_a_union_plans_at_one_partition() {
    let want = expected();
    for from_store in [false, true] {
        assert_eq!(
            answer(1, from_store),
            want,
            "one partition, store {from_store}"
        );
        assert_eq!(
            answer(8, from_store),
            want,
            "eight partitions, store {from_store}"
        );
    }
}
