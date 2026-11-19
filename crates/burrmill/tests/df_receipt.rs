//! Query receipts (#341): a receipt made by one engine verifies on another holding the same inputs,
//! wherever they are held, and fails once any byte of an input, the statement or a setting moves.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{BurrmillError, Engine, Receipt};

const SQL: &str = "SELECT who, SUM(v) AS total, COUNT(*) AS n FROM t GROUP BY who";

fn batch(rows: i64, bump: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("who", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter(
                (0..rows).map(|i| (i % 9 != 0).then(|| format!("0x{:x}", i % 5))),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| i * 3 - 40 + bump),
            )),
        ],
    )
    .unwrap()
}

fn nest(dir: &Path) {
    let segs = dir.join("segments");
    std::fs::create_dir_all(&segs).unwrap();
    for s in 0..3 {
        let b = batch(200, s * 1_000);
        let f = std::fs::File::create(segs.join(format!("t-{s:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, b.schema(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    }
}

fn deterministic(e: Engine) -> Engine {
    e.set_deterministic(true);
    e
}

/// The same segment files as `dir` holds, in an object store, as a bucket or a tab holds them.
fn store_engine(dir: &Path) -> Engine {
    use object_store::{ObjectStoreExt, PutPayload, path::Path as Key};
    let store = object_store::memory::InMemory::new();
    for f in std::fs::read_dir(dir.join("segments")).unwrap() {
        let p = f.unwrap().path();
        let key = Key::parse(format!(
            "nest/segments/{}",
            p.file_name().unwrap().to_string_lossy()
        ))
        .unwrap();
        futures::executor::block_on(store.put(&key, PutPayload::from(std::fs::read(&p).unwrap())))
            .unwrap();
    }
    let e = futures::executor::block_on(Engine::open_store(Arc::new(store), "nest", 4)).unwrap();
    deterministic(e)
}

#[test]
fn a_receipt_verifies_wherever_the_same_files_are_held() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let disk = deterministic(Engine::open_nest(tmp.path()).unwrap());
    let store = store_engine(tmp.path());
    let (answer, receipt) = disk.sql_receipt(SQL).unwrap();
    assert_eq!(answer.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
    assert_eq!(receipt.inputs.len(), 3, "{receipt:?}");
    assert!(receipt.inputs.iter().all(|i| i.starts_with("parquet:")));
    disk.verify_receipt(&receipt).unwrap();
    store.verify_receipt(&receipt).unwrap();
    let (_, from_store) = store.sql_receipt(SQL).unwrap();
    assert_eq!(from_store, receipt);
    disk.verify_receipt(&Receipt::parse(&from_store.to_json()).unwrap())
        .unwrap();
}

#[test]
fn rows_a_host_registered_are_hashed_as_rows() {
    let mut a = deterministic(Engine::builder().open().unwrap());
    let mut b = deterministic(Engine::builder().open().unwrap());
    a.register_batch("t", vec![batch(300, 0)]).unwrap();
    b.register_batch(
        "t",
        vec![batch(150, 0).slice(0, 150), batch(300, 0).slice(150, 150)],
    )
    .unwrap();
    let (_, r) = a.sql_receipt(SQL).unwrap();
    assert!(
        r.inputs[0].starts_with("rows:") && r.inputs[0].ends_with(":300"),
        "{r:?}"
    );
    b.verify_receipt(&r).unwrap();
    let mut c = deterministic(Engine::builder().open().unwrap());
    c.register_batch("t", vec![batch(300, 1)]).unwrap();
    let e = c.verify_receipt(&r).unwrap_err();
    assert!(
        matches!(&e, BurrmillError::Unverified(m) if m.contains("inputs")),
        "{e}"
    );
}

#[test]
fn flipping_a_byte_of_an_input_fails_verification() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = deterministic(Engine::open_nest(tmp.path()).unwrap());
    let (_, receipt) = e.sql_receipt(SQL).unwrap();
    let seg = tmp.path().join(format!("segments/t-{:064x}.parquet", 1));
    let original = std::fs::read(&seg).unwrap();
    let step = (original.len() / 40).max(1);
    for i in (0..original.len())
        .step_by(step)
        .chain([original.len() - 1])
    {
        let mut flipped = original.clone();
        flipped[i] ^= 0x01;
        std::fs::write(&seg, &flipped).unwrap();
        assert!(
            e.verify_receipt(&receipt).is_err(),
            "byte {i} flipped and verified"
        );
    }
    std::fs::write(&seg, &original).unwrap();
    e.verify_receipt(&receipt).unwrap();
}

#[test]
fn another_statement_or_setting_fails_verification() {
    let tmp = tempfile::tempdir().unwrap();
    nest(tmp.path());
    let e = deterministic(Engine::open_nest(tmp.path()).unwrap());
    let (_, receipt) = e.sql_receipt(SQL).unwrap();

    // A receipt rebuilt consistently, digest and all, for a statement whose answer differs.
    let mut other = receipt.clone();
    other.statement = other.statement.replace("SUM(v)", "MAX(v)");
    let e2 = e
        .verify_receipt(&Receipt::parse(&other.to_json()).unwrap())
        .unwrap_err();
    assert!(e2.to_string().contains("answer"), "{e2}");

    let mut strict = receipt.clone();
    strict.strict_decimals = true;
    let e3 = e.verify_receipt(&strict).unwrap_err();
    assert!(e3.to_string().contains("strict decimals"), "{e3}");

    let mut older = receipt.clone();
    older.engine = "burrmill 0.0.0, datafusion 55.2.0".into();
    assert!(e.verify_receipt(&older).is_err());

    // Whitespace and comments are not the statement.
    let (_, spaced) = e
        .sql_receipt(&format!("  {}  -- by hand\n", SQL.replace(' ', "\n  ")))
        .unwrap();
    assert_eq!(spaced, receipt);
}

#[test]
fn a_receipt_needs_deterministic_mode_and_hashable_inputs() {
    let e = Engine::builder().open().unwrap();
    let m = e.sql_receipt("SELECT 1").unwrap_err();
    assert!(matches!(m, BurrmillError::Config(_)), "{m}");
    e.set_deterministic(true);
    let (_, r) = e.sql_receipt("SELECT 1 AS one").unwrap();
    assert!(r.inputs.is_empty());
    e.verify_receipt(&r).unwrap();
}
