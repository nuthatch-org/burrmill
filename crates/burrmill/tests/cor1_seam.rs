//! COR-1: no row is double-counted or dropped across the hot∪cold boundary (RFC-0044 §3.4).
//!
//! The highest-risk invariant in the design, and the risks table has it as M/Critical. It is tested
//! the only way an invariant about a race can honestly be tested: with the race actually running.
//!
//! The shape of the test is a **conserved quantity**. Every row credits one party and debits
//! another by the same amount, so the sum of all balances is exactly zero however the rows are split
//! between hot and cold. More usefully, the *per-party* answer is fixed by the data and does not
//! depend on the boundary at all - so the query can be run against a nest that is being sealed
//! underneath it, and the answer must be the same every time.
//!
//! Both failure arms are visible in that one number:
//!
//! - **Double-count**: a row in both halves inflates two parties' balances.
//! - **Drop**: a row in neither half deflates two parties' balances.
//!
//! Neither would raise an error on its own. They would return a balance sheet that looks completely
//! ordinary and is wrong, which is the failure this whole project is arranged against.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Burrmill, Catalog, HotRow, Limits, MemoryTip, SealedSegments};

const SQL: &str = "SELECT addr, SUM(d) AS net FROM (\
                     SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                     UNION ALL \
                     SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
                   ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

/// One indexed row. `block` decides which side of the seam it is on at any moment.
#[derive(Clone)]
struct Ev {
    block: u64,
    from: String,
    to: String,
    value: u64,
}

fn events(n: u64, parties: u64) -> Vec<Ev> {
    (0..n)
        .map(|i| Ev {
            // Several rows per block, so a seal always lands mid-run rather than tidily between.
            block: i / 3,
            from: format!("0x{:040x}", i % parties),
            to: format!("0x{:040x}", (i * 7 + 3) % parties),
            value: 100 + (i % 17),
        })
        .collect()
}

/// Write one sealed segment holding exactly the rows given.
fn seal_segment(dir: &Path, name: &str, rows: &[Ev]) -> std::path::PathBuf {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.block).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.from.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.to.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.value.to_string()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    // **Installed atomically: temp file, then rename.** A reader globbing the directory must see
    // either no segment or a complete one, and `rename` on one filesystem gives exactly that.
    //
    // Nuthatch does NOT do this today - `seal.rs:176` is a bare `std::fs::write` - which the first
    // version of this test reproduced faithfully and failed on: "Parquet file too small. Size is 0
    // but need 8". Its manifest right beside it *is* written tmp-then-rename, with a comment
    // explaining why. See `docs/upstream/nuthatch-segment-install-not-atomic.md`. This test models
    // the contract Burrmill needs rather than the behaviour it currently gets, because a reader
    // cannot tell a half-written segment from a corrupt one and must not guess.
    let path = dir.join(format!("t-{name}.parquet"));
    let tmp = dir.join(format!(".t-{name}.parquet.tmp"));
    let f = std::fs::File::create(&tmp).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    std::fs::rename(&tmp, &path).unwrap();
    path
}

/// The answer the data determines, independent of where the seam happens to be.
fn expected(rows: &[Ev]) -> Vec<(String, i128)> {
    let mut m: std::collections::BTreeMap<String, i128> = std::collections::BTreeMap::new();
    for r in rows {
        *m.entry(r.to.clone()).or_default() += r.value as i128;
        *m.entry(r.from.clone()).or_default() -= r.value as i128;
    }
    m.into_iter().filter(|(_, v)| *v != 0).collect()
}

fn answer(db: &Burrmill) -> Vec<(String, i128)> {
    let a = match db.query(SQL, Limits::default()) {
        Ok(a) => a,
        Err(e) => panic!("the seam refused a well-formed nest: {e}"),
    };
    a.rows().iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// **The test.** A sealer advances the boundary while a reader folds, over and over.
///
/// The sealer does what the indexer does and in the same order: write the segment first, then
/// advance the watermark and prune hot in one step. Doing those two the other way round is the bug,
/// and doing them non-atomically is the other bug.
#[test]
fn no_row_is_double_counted_or_dropped_while_the_nest_seals_underneath() {
    let rows = events(600, 40);
    let want = expected(&rows);
    let dir = tempfile::tempdir().unwrap();

    let tip = Arc::new(MemoryTip::new());
    for r in &rows {
        tip.append(HotRow {
            block: r.block,
            credit: r.to.as_str().into(),
            debit: r.from.as_str().into(),
            value: Some(r.value.to_string().into_boxed_str()),
        });
    }

    let stop = Arc::new(AtomicBool::new(false));
    let folds = Arc::new(AtomicUsize::new(0));
    let max_block = rows.iter().map(|r| r.block).max().unwrap();

    std::thread::scope(|scope| {
        let sealer = {
            let (tip, dir, stop, rows, folds) = (
                tip.clone(),
                dir.path().to_path_buf(),
                stop.clone(),
                rows.clone(),
                folds.clone(),
            );
            scope.spawn(move || {
                // Seal in ragged steps, so the boundary lands at every sort of place relative to a
                // block's rows rather than always tidily between blocks.
                let mut at = 0u64;
                let mut from = 0u64;
                let mut step = 1u64;
                while at < max_block {
                    at = (at + step).min(max_block);
                    step = step % 7 + 1;
                    // **The new range only.** Nuthatch seals "each table's rows *in that range*"
                    // into their own segment, so segments partition the block space. The first
                    // version of this test wrote a cumulative segment every time, which put early
                    // rows in several segments at once and duly failed on run 0 - the test was
                    // wrong and the engine was right, which is a nicer way round than the reverse.
                    let batch: Vec<Ev> = rows
                        .iter()
                        .filter(|r| r.block >= from && r.block <= at)
                        .cloned()
                        .collect();
                    from = at + 1;
                    // **Segment first, watermark second.** Advancing the watermark before the
                    // segment is durable would leave a window where the range is in neither half.
                    seal_segment(&dir, &format!("{at:06}"), &batch);
                    tip.seal_through(at);
                    // Wait for a fold before the next seal, so folds overlap seals however slow a
                    // fold is. A fixed 2 ms sleep gave 18 overlaps on a CI runner. Bounded, so a
                    // reader that panicked cannot hang the sealer.
                    let seen = folds.load(Ordering::Acquire);
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    while folds.load(Ordering::Acquire) == seen
                        && std::time::Instant::now() < deadline
                    {
                        std::thread::sleep(std::time::Duration::from_micros(200));
                    }
                }
                stop.store(true, Ordering::Release);
            })
        };

        let reader = {
            let (tip, dir, stop, folds) = (
                tip.clone(),
                dir.path().to_path_buf(),
                stop.clone(),
                folds.clone(),
            );
            scope.spawn(move || {
                let mut runs = 0usize;
                let mut raced = 0usize;
                loop {
                    // Folds that completed while the sealer was still advancing the boundary. A
                    // test that only ever folded a settled nest would prove nothing about a race,
                    // and would pass just as happily with the invariant broken.
                    let still_sealing = !stop.load(Ordering::Acquire);
                    // The catalog is rebuilt each time, as a serving path would: new segments must
                    // become visible, and the invariant has to hold while they do.
                    let mut catalog = Catalog::new();
                    catalog.register(SealedSegments::discover("t", &dir).unwrap());
                    let db = Burrmill::with_threads(catalog, 4)
                        .unwrap()
                        .with_hot_tip(tip.clone(), "block_number");
                    assert_eq!(
                        answer(&db),
                        want,
                        "run {runs}: the answer changed while the nest was sealing underneath it. \
                         Either a row was counted on both sides of the seam or it fell between them."
                    );
                    runs += 1;
                    folds.fetch_add(1, Ordering::Release);
                    if still_sealing {
                        raced += 1;
                    }
                    if stop.load(Ordering::Acquire) && runs > 40 {
                        break;
                    }
                }
                (runs, raced)
            })
        };

        sealer.join().unwrap();
        let (runs, raced) = reader.join().unwrap();
        eprintln!("COR-1: {runs} folds, {raced} of them while the boundary was still moving");
        assert!(runs > 40, "only {runs} folds; the test proved little");
        assert!(
            raced >= 20,
            "only {raced} folds overlapped the sealer. A COR-1 test that folds a settled nest \
             passes just as happily with the invariant broken, so this is a failed test rather \
             than a passing one."
        );
    });

    // And once everything is sealed, the same answer with nothing left in hot.
    assert_eq!(
        tip.snapshot_rows_len(),
        0,
        "every row should have been pruned from hot"
    );
}

/// A hot row at or below the watermark is in a cold segment too. Refused, not counted twice.
#[test]
fn a_hot_row_below_the_watermark_is_refused_not_double_counted() {
    let dir = tempfile::tempdir().unwrap();
    let rows = vec![Ev {
        block: 1,
        from: "0xaa".into(),
        to: "0xbb".into(),
        value: 5,
    }];
    seal_segment(dir.path(), "000001", &rows);

    let tip = Arc::new(MemoryTip::new());
    tip.seal_through(10);
    // Block 1 is at or below sealed_through 10, so this row is already in the segment above.
    tip.append(HotRow {
        block: 1,
        credit: "0xbb".into(),
        debit: "0xaa".into(),
        value: Some("5".into()),
    });

    let mut catalog = Catalog::new();
    catalog.register(SealedSegments::discover("t", dir.path()).unwrap());
    let db = Burrmill::with_threads(catalog, 2)
        .unwrap()
        .with_hot_tip(tip, "block_number");
    let err = db.query(SQL, Limits::default()).unwrap_err();
    assert!(
        matches!(err, burrmill::BurrmillError::Seam(_)),
        "a row on both sides of the seam must be refused, got {err:?}"
    );
}

/// With no hot tip attached, every row is cold and nothing about the fold changes. Slice 1's
/// behaviour has to survive slice 2 arriving.
#[test]
fn without_a_hot_tip_the_fold_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let rows = events(90, 12);
    seal_segment(dir.path(), "000000", &rows);
    let mut catalog = Catalog::new();
    catalog.register(SealedSegments::discover("t", dir.path()).unwrap());
    let db = Burrmill::with_threads(catalog, 2).unwrap();
    assert_eq!(answer(&db), expected(&rows));
}

/// The hot half folds `+value` to credit and `-value` to debit and nothing else, so under a tip any
/// other plan is refused rather than answered with hot rows folded to the wrong shape (#148).
fn refused_under_a_tip_but_answered_without(sql: &str) {
    let rows = events(30, 6);
    let (t, u) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    seal_segment(t.path(), "000000", &rows);
    seal_segment(u.path(), "000000", &rows);
    let catalog = || {
        let mut c = Catalog::new();
        c.register(SealedSegments::discover("t", t.path()).unwrap());
        c.register(SealedSegments::discover("u", u.path()).unwrap());
        c
    };
    let cold = Burrmill::with_threads(catalog(), 2).unwrap();
    cold.query(sql, Limits::default())
        .unwrap_or_else(|e| panic!("the plan should run without a tip: {e}"));

    let tip = Arc::new(MemoryTip::new());
    tip.seal_through(rows.iter().map(|r| r.block).max().unwrap());
    tip.append(HotRow {
        block: 1_000,
        credit: "0xAB".into(),
        debit: "0xCD".into(),
        value: Some("5".into()),
    });
    let db = Burrmill::with_threads(catalog(), 2)
        .unwrap()
        .with_hot_tip(tip, "block_number");
    match db.query(sql, Limits::default()) {
        Err(burrmill::BurrmillError::NotAllowed(m)) => {
            assert!(m.contains("hot tip"), "{m}")
        }
        other => panic!("expected the seam to refuse the plan, got {other:?}"),
    }
}

#[test]
fn a_lowered_key_is_refused_under_a_hot_tip() {
    refused_under_a_tip_but_answered_without(
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT lower(\"to\") AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
           UNION ALL \
           SELECT lower(\"from\") AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
         ) GROUP BY addr HAVING SUM(d) <> 0",
    );
}

#[test]
fn a_two_sum_plan_is_refused_under_a_hot_tip() {
    refused_under_a_tip_but_answered_without(
        "SELECT addr, SUM(a) AS na, SUM(b) AS nb FROM (\
           SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS a, \
                  TRY_CAST(\"value\" AS HUGEINT) AS b FROM t \
           UNION ALL \
           SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT), \
                  -TRY_CAST(\"value\" AS HUGEINT) FROM t\
         ) GROUP BY addr",
    );
}

#[test]
fn a_credit_only_plan_is_refused_under_a_hot_tip() {
    refused_under_a_tip_but_answered_without(
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
           UNION ALL \
           SELECT \"from\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
         ) GROUP BY addr HAVING SUM(d) <> 0",
    );
}

#[test]
fn branches_on_two_tables_are_refused_under_a_hot_tip() {
    refused_under_a_tip_but_answered_without(
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
           UNION ALL \
           SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM u\
         ) GROUP BY addr HAVING SUM(d) <> 0",
    );
}

/// A NULL block read as 0 and a negative one cast to NULL and then 0, so both rows were sealed
/// whatever their real block (#149).
#[test]
fn a_cold_row_without_a_block_is_refused_under_a_hot_tip() {
    let blocks: [(DataType, Arc<dyn arrow::array::Array>); 2] = [
        (
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![Some(1), None])),
        ),
        (
            DataType::Int64,
            Arc::new(arrow::array::Int64Array::from(vec![1, -1])),
        ),
    ];
    for (ty, block) in blocks {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", ty.clone(), true),
            Field::new("from", DataType::Utf8, false),
            Field::new("to", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                block,
                Arc::new(StringArray::from(vec!["0xaa", "0xaa"])),
                Arc::new(StringArray::from(vec!["0xbb", "0xbb"])),
                Arc::new(StringArray::from(vec!["5", "7"])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join("t-000000.parquet")).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let tip = Arc::new(MemoryTip::new());
        tip.seal_through(10);
        let mut catalog = Catalog::new();
        catalog.register(SealedSegments::discover("t", dir.path()).unwrap());
        let db = Burrmill::with_threads(catalog, 2)
            .unwrap()
            .with_hot_tip(tip, "block_number");
        match db.query(SQL, Limits::default()) {
            Err(burrmill::BurrmillError::Seam(m)) => {
                assert!(m.contains("block-stamped"), "{ty}: {m}")
            }
            other => panic!("{ty}: expected the seam to refuse, got {other:?}"),
        }
    }
}
