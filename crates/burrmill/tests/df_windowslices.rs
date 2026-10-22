//! nuthatch #1899: a window's output arrives in batches of the session's batch size.
//! `WindowAggExec` emits its whole input as one batch, so everything above it worked on one array
//! the size of the partition: on the QoS nest one statement's pool peak was 877 MB that way, and
//! 538 MB with the output sliced.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const ROWS: u64 = 200_000;

fn segments() -> &'static Vec<(PathBuf, u64)> {
    static SEGS: OnceLock<Vec<(PathBuf, u64)>> = OnceLock::new();
    SEGS.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("df_windowslices");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("who", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, false),
        ]));
        (0..4u64)
            .map(|n| {
                let path = dir.join(format!("t-{n:064x}.parquet"));
                if !path.exists() {
                    let span = n * ROWS / 4..(n + 1) * ROWS / 4;
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(UInt64Array::from(span.clone().collect::<Vec<_>>())),
                            Arc::new(StringArray::from(
                                span.clone()
                                    .map(|i| format!("0x{i:040x}"))
                                    .collect::<Vec<_>>(),
                            )),
                            Arc::new(StringArray::from(
                                span.map(|i| (i % 1000).to_string()).collect::<Vec<_>>(),
                            )),
                        ],
                    )
                    .unwrap();
                    let tmp = dir.join(format!("t-{n}.{}.tmp", std::process::id()));
                    let f = std::fs::File::create(&tmp).unwrap();
                    let mut w =
                        parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                    w.write(&batch).unwrap();
                    w.close().unwrap();
                    std::fs::rename(&tmp, &path).unwrap();
                }
                let len = std::fs::metadata(&path).unwrap().len();
                (path, len)
            })
            .collect()
    })
}

fn engine(memory_bytes: Option<usize>) -> Engine {
    let mut e = match memory_bytes {
        Some(memory_bytes) => Engine::open_empty_budgeted(burrmill::Budget {
            memory_bytes,
            threads: 2,
            spill: None,
        }),
        None => Engine::open_empty(),
    }
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

/// The window's output rows and the largest batch it came in, with the rows sorted.
fn windowed(e: &Engine) -> (Vec<String>, usize) {
    let sql = "SELECT block_number, \
        sum(CAST(amount AS BIGINT)) OVER (PARTITION BY block_number % 7) AS s FROM t";
    let (mut rows, mut largest) = (Vec::new(), 0);
    e.sql_for_each(sql, |b| {
        largest = largest.max(b.num_rows());
        rows.extend(
            burrmill::df::encode::rows(&b)
                .unwrap()
                .iter()
                .map(|r| r.to_string()),
        );
        Ok(())
    })
    .unwrap();
    rows.sort();
    (rows, largest)
}

#[test]
fn a_window_emits_batches_of_the_batch_size() {
    let (rows, largest) = windowed(&engine(Some(1 << 30)));
    let (want, _) = windowed(&engine(None));
    assert_eq!(rows.len(), ROWS as usize);
    assert_eq!(rows, want, "budgeted and unbudgeted answers differ");
    // A budget's batch size, as `session.rs` sets it.
    assert!(
        largest <= 2048,
        "the window emitted a batch of {largest} rows"
    );
}
