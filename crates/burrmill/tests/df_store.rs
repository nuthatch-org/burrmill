//! A nest opened from an object store answers as the same nest opened from its directory. A tab
//! has only the store, so this is the browser path run natively.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as Key;

fn segment(cols: &[(&str, Vec<Option<&str>>)]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v)| Arc::new(StringArray::from(v.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut out = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut out, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    out
}

const SCHEMA_JSON: &str = r#"{"tables": [
    {"table": "transfer", "columns": [
        {"name": "from", "storage": "text"}, {"name": "to", "storage": "text"},
        {"name": "value", "storage": "text"}]},
    {"table": "approval", "columns": [
        {"name": "owner", "storage": "text"}, {"name": "block", "storage": "u64"}]}
]}"#;

/// The same files twice: in a directory, and under `nest/` in a store.
fn both() -> (tempfile::TempDir, Engine, Engine, Arc<InMemory>) {
    let files = [
        (
            format!("transfer-{:064x}.parquet", 0),
            segment(&[
                ("from", vec![Some("0xA"), Some("0xb"), Some("0xa")]),
                ("to", vec![Some("0xb"), Some("0xc"), Some("")]),
                ("value", vec![Some("10"), Some("4"), Some("1")]),
            ]),
        ),
        (
            format!("transfer-{:064x}.parquet", 1),
            segment(&[
                ("from", vec![Some("0xc"), Some("0xd")]),
                ("to", vec![Some("0xa"), Some("0xe")]),
                ("value", vec![Some("2"), None]),
            ]),
        ),
    ];
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    std::fs::write(tmp.path().join("schema.json"), SCHEMA_JSON).unwrap();
    let store = Arc::new(InMemory::new());
    futures::executor::block_on(async {
        for (name, bytes) in &files {
            std::fs::write(segs.join(name), bytes).unwrap();
            let key = Key::from(format!("nest/segments/{name}"));
            store.put(&key, bytes.clone().into()).await.unwrap();
        }
        let key = Key::from("nest/schema.json");
        store.put(&key, SCHEMA_JSON.into()).await.unwrap();
    });
    let local = Engine::open_nest(tmp.path()).unwrap();
    let stored = futures::executor::block_on(Engine::open_store(store.clone(), "nest", 4)).unwrap();
    (tmp, local, stored, store)
}

fn rows(e: &Engine, sql: &str) -> Vec<Vec<String>> {
    let batches = e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err:?}"));
    let mut out = Vec::new();
    for b in &batches {
        let cols: Vec<ArrayRef> = b
            .columns()
            .iter()
            .map(|c| arrow::compute::cast(c, &DataType::Utf8).unwrap())
            .collect();
        for i in 0..b.num_rows() {
            out.push(
                cols.iter()
                    .map(|c| {
                        let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                        if s.is_null(i) {
                            "NULL".into()
                        } else {
                            s.value(i).to_string()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

fn plan(e: &Engine, sql: &str) -> String {
    rows(e, &format!("EXPLAIN {sql}"))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n")
}

const FOLD: &str = r#"SELECT addr, SUM(d) AS net FROM (
    SELECT "to" AS addr, CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
    UNION ALL
    SELECT "from" AS addr, -CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#;

#[test]
fn a_stored_nest_answers_as_its_directory_does() {
    let (_tmp, local, stored, _) = both();
    assert_eq!(local.tables(), stored.tables());
    for sql in [
        FOLD,
        r#"SELECT COUNT(*), COUNT("value") FROM transfer"#,
        r#"SELECT "from", "to" FROM transfer WHERE "value" = '2'"#,
        "SELECT * FROM approval",
    ] {
        assert_eq!(rows(&local, sql), rows(&stored, sql), "{sql}");
    }
}

/// The owned fold opens segment files itself. Over a store it has none to open, so the store's plan
/// is DataFusion's aggregate, and the directory's is still the fold.
#[test]
fn the_owned_fold_stays_with_local_files() {
    let (_tmp, local, stored, _) = both();
    assert!(plan(&local, FOLD).contains("OwnedSignedFold"));
    assert!(!plan(&stored, FOLD).contains("OwnedSignedFold"));
}

/// Bound as nuthatch binds a table, a declared column no segment carries included, from the store
/// and from the directory: the same answers.
#[test]
fn store_facts_bind_as_local_facts_do() {
    let (tmp, _, _, store) = both();
    let declared: Vec<(String, String)> = ["from", "to", "value", "memo"]
        .iter()
        .map(|c| (c.to_string(), "text".to_string()))
        .collect();
    let mut local_files: Vec<(std::path::PathBuf, u64)> =
        std::fs::read_dir(tmp.path().join("segments"))
            .unwrap()
            .map(|e| {
                let p = e.unwrap().path();
                let n = std::fs::metadata(&p).unwrap().len();
                (p, n)
            })
            .collect();
    local_files.sort();
    let keys: Vec<(String, u64)> = local_files
        .iter()
        .map(|(p, n)| {
            let name = p.file_name().unwrap().to_string_lossy();
            (format!("nest/segments/{name}"), *n)
        })
        .collect();

    let mut local = Engine::open_empty().unwrap();
    local
        .register_facts("t", &declared, local_files, &[], (None, None))
        .unwrap();
    let mut remote = Engine::open_empty().unwrap();
    remote.attach_store(store).unwrap();
    futures::executor::block_on(remote.register_store_facts(
        "t",
        &declared,
        keys,
        &[],
        (None, None),
    ))
    .unwrap();
    for sql in [
        r#"SELECT "from", "to", "value", memo FROM t ORDER BY "from", "to""#,
        r#"SELECT COUNT(*), COUNT(memo) FROM t"#,
    ] {
        assert_eq!(rows(&local, sql), rows(&remote, sql), "{sql}");
    }
}
