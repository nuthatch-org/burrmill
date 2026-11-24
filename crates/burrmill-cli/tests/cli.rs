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
    assert_eq!(burrmill(&["--help"]).0, 2);
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

fn text(dir: &Path, name: &str, body: &str) -> String {
    let p = dir.join(name);
    std::fs::write(&p, body).unwrap();
    p.to_str().unwrap().to_string()
}

#[test]
fn csv_json_and_ndjson_files_are_tables() {
    let t = tempfile::tempdir().unwrap();
    let csv = text(t.path(), "Trades.CSV", "id,amount\n1,1.50\n2,2.25\n");
    let json = text(
        t.path(),
        "who.json",
        r#"[{"id": 1, "who": "a"}, {"id": 2, "who": "b"}]"#,
    );
    let ndjson = text(
        t.path(),
        "events.ndjson",
        "{\"id\": 2, \"n\": 3}\n{\"id\": 2, \"n\": 4}\n",
    );
    let jsonl = text(t.path(), "more.jsonl", "{\"id\": 1}\n");
    let sql = "SELECT w.who, SUM(t.amount) AS amount, (SELECT SUM(n) FROM events) AS n, \
               (SELECT COUNT(*) FROM more) AS m, (SELECT COUNT(*) FROM named) AS c \
               FROM Trades t JOIN who w ON t.id = w.id GROUP BY w.who ORDER BY w.who";
    let named = format!("named={csv}");
    let (code, out, err) = burrmill(&["q", sql, &csv, &json, &ndjson, &jsonl, &named]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(out, "who,amount,n,m,c\na,1.50,7,1,2\nb,2.25,7,1,2\n");
    let (code, out, err) = burrmill(&["q", "SELECT n FROM events ORDER BY n", &ndjson]);
    assert_eq!((code, out.as_str()), (0, "n\n3\n4\n"), "{err}");
}

#[test]
fn a_csv_receipt_verifies_and_an_edit_to_the_file_is_refused() {
    let t = tempfile::tempdir().unwrap();
    let body = "k,v\na,1\nb,2\na,3\n";
    let csv = text(t.path(), "kv.csv", body);
    let receipt = t.path().join("r.json");
    let (code, out, err) = burrmill(&[
        "q",
        "SELECT k, SUM(v) AS s FROM kv GROUP BY k",
        &csv,
        "--receipt",
        receipt.to_str().unwrap(),
    ]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(out, "k,s\na,4\nb,2\n");
    let r = std::fs::read_to_string(&receipt).unwrap();
    // Named by its bytes, as a Parquet file is: `csv:<sha256>:<bytes>`.
    let input = r
        .split("\"inputs\":[\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    let parts: Vec<&str> = input.split(':').collect();
    assert_eq!(parts[0], "csv", "{r}");
    assert_eq!(parts[1].len(), 64, "{r}");
    assert_eq!(parts[2], body.len().to_string(), "{r}");
    let (code, _, err) = burrmill(&["verify", receipt.to_str().unwrap(), &csv]);
    assert_eq!(code, 0, "{err}");
    text(t.path(), "kv.csv", &body.replace("b,2", "b,5"));
    let (code, _, err) = burrmill(&["verify", receipt.to_str().unwrap(), &csv]);
    assert_eq!(code, 1);
    assert!(err.contains("receipt does not verify"), "{err}");
}

#[test]
fn a_malformed_or_oversized_file_is_refused_with_where() {
    let t = tempfile::tempdir().unwrap();
    let csv = text(t.path(), "bad.csv", "a,b,c\n1,2,3\n4,5\n6,7,8\n");
    let (code, _, err) = burrmill(&["q", "SELECT * FROM bad", &csv]);
    assert_eq!(code, 1);
    assert!(
        err.contains(
            "bad.csv: refused at row 2, column 3 (c): 2 fields where the widest row has 3"
        ),
        "{err}"
    );
    let ndjson = text(
        t.path(),
        "bad.ndjson",
        "{\"a\": 1}\n{\"a\": 2}\n{\"a\": nope}\n",
    );
    let (code, _, err) = burrmill(&["q", "SELECT * FROM bad", &ndjson]);
    assert_eq!(code, 1);
    assert!(
        err.contains("bad.ndjson: not a readable json file: line 3, column 8"),
        "{err}"
    );
    let ok = text(t.path(), "ok.csv", "a,b\n1,2\n3,4\n");
    let (code, _, err) = burrmill(&["q", "SELECT * FROM ok", &ok, "--memory", "40"]);
    assert_eq!(code, 1);
    assert!(
        err.contains("limit exceeded") && err.contains("ok.csv"),
        "{err}"
    );
}

/// `burrmill PATH... < script`, as CI drives it: no prompt, every dot-command, a statement over
/// lines, an error the session survives, and each `.mode`'s bytes.
#[test]
fn a_scripted_session_runs_its_statements_and_commands() {
    use std::io::Write as _;
    let t = tempfile::tempdir().unwrap();
    let csv = text(t.path(), "t.csv", "id,k\n1,a\n2,\n3,c\n");
    let json = text(t.path(), "j.ndjson", "{\"x\": 1.5}\n");
    text(
        t.path(),
        "more.sql",
        "SELECT 41 + 1\n  AS n;\n.mode table\n",
    );
    let script = "\
.help
.tables
.schema t
.schema nope
-- a comment, then a statement over two lines
SELECT id,
       k FROM t ORDER BY id;
.mode csv
SELECT * FROM t ORDER BY id; SELECT ';' AS semi;
.mode json
SELECT * FROM t ORDER BY id;
SELECT nope FROM t;
.mode xml
.timer on
SELECT x FROM j;
.timer off
.read more.sql
.schema
.frobnicate
.quit
SELECT 'never' AS z;
";
    let mut child = Command::new(env!("CARGO_BIN_EXE_burrmill"))
        .args([&csv, &json])
        .current_dir(t.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let o = child.wait_with_output().unwrap();
    let (out, err) = (
        String::from_utf8(o.stdout).unwrap(),
        String::from_utf8(o.stderr).unwrap(),
    );
    let help = out.split_once("line at the prompt.\n").map(|(h, _)| h);
    assert!(
        help.is_some_and(
            |h| [".tables", ".schema", ".timer", ".mode", ".read", ".quit"]
                .iter()
                .all(|c| h.contains(c))
        ),
        "{out}"
    );
    let rest = &out[help.unwrap().len() + "line at the prompt.\n".len()..];
    assert_eq!(
        rest,
        "\
+------------+
| table_name |
+------------+
| j          |
| t          |
+------------+
(2 rows)
+-------------+-----------+-------------+
| column_name | data_type | is_nullable |
+-------------+-----------+-------------+
| id          | BIGINT    | YES         |
| k           | VARCHAR   | YES         |
+-------------+-----------+-------------+
(2 rows)
+----+------+
| id | k    |
+----+------+
| 1  | a    |
| 2  | NULL |
| 3  | c    |
+----+------+
(3 rows)
id,k
1,a
2,
3,c
semi
;
[{\"id\":1,\"k\":\"a\"},{\"id\":2,\"k\":null},{\"id\":3,\"k\":\"c\"}]
[{\"x\":1.5}]
[{\"n\":42}]
+------------+-------------+--------------+-------------+
| table_name | column_name | data_type    | is_nullable |
+------------+-------------+--------------+-------------+
| j          | x           | DECIMAL(2,1) | YES         |
| t          | id          | BIGINT       | YES         |
| t          | k           | VARCHAR      | YES         |
+------------+-------------+--------------+-------------+
(3 rows)
"
    );
    let lines: Vec<&str> = err.lines().collect();
    assert_eq!(lines[0], "error: no table nope", "{err}");
    assert!(lines[1].starts_with("error: plan error: "), "{err}");
    assert!(
        err.contains("error: .mode table, .mode csv or .mode json\n"),
        "{err}"
    );
    assert!(err.contains("\ntime: "), "{err}");
    assert_eq!(err.matches("time: ").count(), 1, "{err}");
    assert!(
        err.ends_with("error: no command .frobnicate; .help lists them\n"),
        "{err}"
    );
    // An error does not end the session, but a script that had one says so.
    assert_eq!(o.status.code(), Some(1));
    let (code, out, err) = burrmill_in(&[&csv], "SELECT COUNT(*) AS n FROM t");
    assert_eq!(
        (code, out.as_str()),
        (0, "+---+\n| n |\n+---+\n| 3 |\n+---+\n(1 row)\n"),
        "{err}"
    );
    let (code, out, _) = burrmill_in(&[], ".mode csv\nSELECT 1 AS a;\n");
    assert_eq!((code, out.as_str()), (0, "a\n1\n"));
}

fn burrmill_in(args: &[&str], stdin: &str) -> (i32, String, String) {
    use std::io::Write as _;
    let mut child = Command::new(env!("CARGO_BIN_EXE_burrmill"))
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let o = child.wait_with_output().unwrap();
    (
        o.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&o.stdout).into_owned(),
        String::from_utf8_lossy(&o.stderr).into_owned(),
    )
}

/// Ctrl-C, as the signal: the statement running is cancelled within the engine's bound, and the
/// session goes on to the next.
#[cfg(unix)]
#[test]
fn ctrl_c_cancels_the_statement_running_and_not_the_session() {
    use std::io::{BufRead as _, Write as _};
    use std::time::{Duration, Instant};
    // The engine's join bound is 100 ms in release (#344); an unoptimised build on a loaded runner
    // misses it by tens, as crates/burrmill/tests/df_host.rs allows. The rest is margin.
    let bound = Duration::from_millis(if cfg!(debug_assertions) { 1000 } else { 250 });
    let t = tempfile::tempdir().unwrap();
    let body: String = std::iter::once("id\n".to_string())
        .chain((0..60_000).map(|i| format!("{i}\n")))
        .collect();
    let csv = text(t.path(), "big.csv", &body);
    let mut seen = Vec::new();
    // A shared runner can stall one attempt; a cancel that is not seen is late on every one.
    for _ in 0..3 {
        let mut child = Command::new(env!("CARGO_BIN_EXE_burrmill"))
            .args([&csv])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(
                b".mode csv\nSELECT 'go' AS x;\nSELECT sum(a.id * b.id) AS s FROM big a, big b;\n\
                  SELECT 'after' AS y;\n",
            )
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let cancelled = std::thread::spawn(move || {
            let mut line = String::new();
            std::io::BufReader::new(stderr)
                .read_line(&mut line)
                .unwrap();
            (Instant::now(), line)
        });
        let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut out = String::new();
        while !out.contains("go\n") {
            assert!(stdout.read_line(&mut out).unwrap() > 0, "{out}");
        }
        // Into the join, which runs for many seconds.
        std::thread::sleep(Duration::from_millis(300));
        let sent = Instant::now();
        let kill = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(kill.success());
        let (back, line) = cancelled.join().unwrap();
        assert_eq!(line, "cancelled\n");
        std::io::Read::read_to_string(&mut stdout, &mut out).unwrap();
        let status = child.wait().unwrap();
        assert_eq!(out, "x\ngo\ny\nafter\n");
        assert_eq!(status.code(), Some(1));
        eprintln!("back {:?} after the signal", back - sent);
        seen.push(back - sent);
        if back - sent < bound {
            break;
        }
    }
    assert!(seen.iter().any(|l| *l < bound), "late by {seen:?}");
}
