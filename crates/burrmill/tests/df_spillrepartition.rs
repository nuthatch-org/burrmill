//! A statement whose repartition spills under a budget answers, or is refused; it does not wait for
//! ever. DataFusion 55.2's shared spill pool read its files in turn and could wait on an unfinished
//! one for a batch written to another, while the writers that would finish it waited on that reader.
//! The wait was a race, so the statement runs several times, each held to a deadline.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, BurrmillError, Engine, Stop};

const FOLD: &str = r#"SELECT addr, SUM(d) AS net FROM (
    SELECT "to" AS addr, CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
    UNION ALL
    SELECT "from" AS addr, -CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
) GROUP BY addr HAVING SUM(d) <> 0"#;

/// Two segments of `rows` transfers, every party distinct.
fn segments(segs: &Path, rows: usize) {
    std::fs::create_dir_all(segs).unwrap();
    let schema = Arc::new(Schema::new(
        ["from", "to", "value"]
            .map(|n| Field::new(n, DataType::Utf8, false))
            .to_vec(),
    ));
    for s in 0..2 {
        let party = |i: usize| format!("0x{:040x}", s * 2 * rows + i);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values((0..rows).map(party))),
                Arc::new(StringArray::from_iter_values((rows..2 * rows).map(party))),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| (i + 1).to_string()),
                )),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(segs.join(format!("transfer-{s:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

#[test]
fn a_spilling_repartition_ends() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    segments(&segs, 250_000);
    let spill = tmp.path().join("spill");
    std::fs::create_dir(&spill).unwrap();
    let e = Engine::builder()
        .segments(&segs)
        .budget(Budget {
            memory_bytes: 64 << 20,
            threads: 2,
            spill: Some((spill, 4 << 30)),
        })
        .open()
        .unwrap();
    for run in 0..8 {
        let started = Instant::now();
        let stop = Stop {
            cancel: burrmill::CancelToken::new(),
            deadline: Some(started + Duration::from_secs(60)),
        };
        let mut rows = 0usize;
        let r = e.sql_for_each_stoppable(FOLD, &[], &stop, None, |b| {
            rows += b.num_rows();
            Ok(())
        });
        assert!(
            !matches!(r, Err(BurrmillError::Timeout)),
            "run {run} was still waiting after {:?}",
            started.elapsed()
        );
        r.unwrap_or_else(|err| panic!("run {run} refused: {err}"));
        assert_eq!(rows, 1_000_000);
    }
}
