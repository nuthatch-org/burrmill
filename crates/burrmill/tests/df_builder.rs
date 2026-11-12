//! One builder opens every engine: source, budget and shared pool in any combination, including
//! a budgeted segments directory, which no `open_*` constructor offered.
#![cfg(feature = "datafusion")]

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Budget, BurrmillError, Engine, SharedPool};

fn segments(rows: usize) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    let values: Vec<String> = (0..rows).map(|i| i.to_string()).collect();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap();
    let f = std::fs::File::create(tmp.path().join(format!("t-{:064x}.parquet", 0))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

fn budget() -> Budget {
    Budget {
        memory_bytes: 256 << 20,
        threads: 1,
        spill: None,
    }
}

fn batch_sizes(e: &Engine) -> Vec<usize> {
    let mut sizes = Vec::new();
    e.sql_for_each("SELECT v FROM t", |b| {
        sizes.push(b.num_rows());
        Ok(())
    })
    .unwrap();
    sizes
}

#[test]
fn a_budgeted_segments_directory_reads_as_a_budgeted_session() {
    let dir = segments(10_000);
    let budgeted = Engine::builder()
        .segments(dir.path())
        .budget(budget())
        .open()
        .unwrap();
    let sizes = batch_sizes(&budgeted);
    assert_eq!(sizes.iter().sum::<usize>(), 10_000);
    assert_eq!(sizes.iter().copied().max(), Some(2048), "{sizes:?}");

    let plain = Engine::open_segments(dir.path()).unwrap();
    let sizes = batch_sizes(&plain);
    assert!(sizes.iter().copied().max().unwrap() > 2048, "{sizes:?}");
}

#[test]
fn a_shared_pool_without_a_budget_is_refused() {
    let pool = SharedPool::new(64 << 20);
    let r = Engine::builder().shared_pool(&pool).open();
    assert!(
        matches!(&r, Err(BurrmillError::Config(m)) if m.contains("needs a budget")),
        "{:?}",
        r.err()
    );
    let e = Engine::builder()
        .budget(budget())
        .shared_pool(&pool)
        .open()
        .unwrap();
    assert_eq!(e.memory_reserved(), 0);
}

#[test]
fn a_store_source_opens_only_asynchronously() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let r = Engine::builder().store(store.clone(), "nest").open();
    assert!(matches!(r, Err(BurrmillError::Config(_))), "{:?}", r.err());
    // Listed, and empty: the same refusal `open_store` gives.
    let r = futures::executor::block_on(Engine::builder().store(store, "nest").open_async());
    assert!(
        matches!(r, Err(BurrmillError::NoSegments(_))),
        "{:?}",
        r.err()
    );
}

#[test]
fn a_missing_segments_directory_names_its_path() {
    let tmp = tempfile::tempdir().unwrap();
    let gone = tmp.path().join("nest");
    for r in [Engine::open_nest(&gone), Engine::open_segments(&gone)] {
        let e = r.err().expect("a missing directory opens nothing");
        assert!(matches!(e, BurrmillError::NoSegments(_)), "{e:?}");
        assert!(e.to_string().contains(&*gone.to_string_lossy()), "{e}");
    }
    let empty = Engine::open_nest(tmp.path()).err().unwrap();
    assert!(matches!(empty, BurrmillError::NoSegments(_)), "{empty:?}");
    assert!(empty.to_string().contains("segments"), "{empty}");
}

#[test]
fn files_without_a_hyphen_are_named_and_the_rule_stated() {
    let tmp = segments(1);
    std::fs::rename(
        tmp.path().join(format!("t-{:064x}.parquet", 0)),
        tmp.path().join("trades.parquet"),
    )
    .unwrap();
    let e = Engine::open_segments(tmp.path()).err().unwrap();
    assert!(matches!(e, BurrmillError::NoSegments(_)), "{e:?}");
    let e = e.to_string();
    assert!(
        e.contains("trades.parquet") && e.contains("<table>-<hash>.parquet"),
        "{e}"
    );
}

#[test]
fn a_table_is_named_by_the_text_before_the_last_hyphen() {
    let tmp = segments(1);
    std::fs::rename(
        tmp.path().join(format!("t-{:064x}.parquet", 0)),
        tmp.path().join("daily-trades-2024.parquet"),
    )
    .unwrap();
    let e = Engine::open_segments(tmp.path()).unwrap();
    assert_eq!(
        e.sql("SELECT count(*) c FROM \"daily-trades\"")
            .unwrap()
            .len(),
        1
    );
}
