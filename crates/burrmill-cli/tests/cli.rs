//! The binary as a user runs it: tables named from paths, each output format, strict decimals,
//! receipts made and verified, and what it refuses.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn write(path: &Path, first: i64, rows: i64) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(first..first + rows)),
            Arc::new(StringArray::from_iter(
                (first..first + rows).map(|i| (i % 4 != 0).then(|| format!("k{}", i % 3))),
            )),
        ],
    )
    .unwrap();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
}

fn data() -> tempfile::TempDir {
    let t = tempfile::tempdir().unwrap();
    write(&t.path().join("data/trades.parquet"), 0, 10);
    write(&t.path().join("data/quotes/part-0.parquet"), 0, 5);
    write(&t.path().join("data/quotes/part-1.parquet"), 5, 5);
    write(&t.path().join("lake/day=2024-01-01/a.parquet"), 0, 3);
    write(&t.path().join("lake/day=2024-01-02/a.parquet"), 3, 4);
    t
}

fn burrmill(args: &[&str]) -> (i32, String, String) {
    let o = Command::new(env!("CARGO_BIN_EXE_burrmill"))
        .args(args)
        .output()
        .unwrap();
    (
        o.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&o.stdout).into_owned(),
        String::from_utf8_lossy(&o.stderr).into_owned(),
    )
}

#[test]
fn a_directory_names_its_tables() {
    let t = data();
    let d = t.path().join("data");
    let lake = t.path().join("lake");
    let (code, out, err) = burrmill(&[
        "q",
        "SELECT (SELECT COUNT(*) FROM trades) AS a, (SELECT COUNT(*) FROM quotes) AS b, \
         (SELECT COUNT(*) FROM lake WHERE day = DATE '2024-01-02') AS c, \
         (SELECT COUNT(*) FROM named) AS d",
        d.to_str().unwrap(),
        lake.to_str().unwrap(),
        &format!("named={}", d.join("trades.parquet").display()),
    ]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(out, "a,b,c,d\n10,10,4,10\n");
}

#[test]
fn every_format_writes_what_it_reads() {
    let t = data();
    let d = t.path().join("data");
    let sql = "SELECT id, k FROM trades ORDER BY id LIMIT 3";
    let (_, json, _) = burrmill(&["q", sql, d.to_str().unwrap(), "--format", "json"]);
    assert_eq!(
        json,
        r#"[{"id":0,"k":null},{"id":1,"k":"k1"},{"id":2,"k":"k2"}]"#
    );
    for (format, read) in [("parquet", true), ("arrow", false)] {
        let out = t.path().join(format!("out.{format}"));
        let (code, _, err) = burrmill(&[
            "q",
            sql,
            d.to_str().unwrap(),
            "--format",
            format,
            "--out",
            out.to_str().unwrap(),
        ]);
        assert_eq!(code, 0, "{err}");
        let rows: usize = if read {
            let f = std::fs::File::open(&out).unwrap();
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum()
        } else {
            let f = std::fs::File::open(&out).unwrap();
            arrow::ipc::reader::StreamReader::try_new(f, None)
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum()
        };
        assert_eq!(rows, 3, "{format}");
    }
}

#[test]
fn a_receipt_is_made_verified_and_an_edit_refused() {
    let t = data();
    let d = t.path().join("data");
    let receipt = t.path().join("r.json");
    let sql = "SELECT k, SUM(id) AS s FROM quotes GROUP BY k";
    let (code, out, err) = burrmill(&[
        "q",
        sql,
        d.to_str().unwrap(),
        "--receipt",
        receipt.to_str().unwrap(),
    ]);
    assert_eq!(code, 0, "{err}");
    // Deterministic, so the rows come in one order.
    assert_eq!(out, "k,s\n,12\nk0,18\nk1,8\nk2,7\n");
    let (code, _, err) = burrmill(&["verify", receipt.to_str().unwrap(), d.to_str().unwrap()]);
    assert_eq!(code, 0, "{err}");
    write(&d.join("quotes/part-1.parquet"), 6, 5);
    let (code, _, err) = burrmill(&["verify", receipt.to_str().unwrap(), d.to_str().unwrap()]);
    assert_eq!(code, 1);
    assert!(err.contains("receipt does not verify"), "{err}");
}

#[test]
fn strict_decimals_and_bad_arguments_are_refused() {
    let t = data();
    let d = t.path().join("data");
    let (code, _, err) = burrmill(&["q", "SELECT 1 / 2", d.to_str().unwrap(), "--strict"]);
    assert_eq!(code, 1);
    assert!(err.contains("div("), "{err}");
    let (code, out, _) = burrmill(&["q", "SELECT 1 / 2 AS h", d.to_str().unwrap()]);
    assert_eq!((code, out.as_str()), (0, "h\n0.5\n"));
    assert_eq!(burrmill(&[]).0, 2);
    assert_eq!(
        burrmill(&["q", "SELECT 1", d.to_str().unwrap(), "--format", "xml"]).0,
        2
    );
    let (code, _, err) = burrmill(&[
        "q",
        "SELECT * FROM read_parquet('/etc/passwd')",
        d.to_str().unwrap(),
    ]);
    assert_eq!(code, 1, "{err}");
}
