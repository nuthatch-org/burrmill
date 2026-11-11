//! JSON through the budgets CSV reads under (#253).

use burrmill_ingest::{
    Budget, BudgetBreach, Format, FormatHint, InferredType, IngestError, Table, ingest_bytes,
};

const INVALID: IngestError = IngestError::Invalid { format: "json" };

fn sniff(text: &str) -> Table {
    ingest_bytes(text.as_bytes(), FormatHint::Sniff, &Budget::untrusted()).unwrap()
}

fn breach(text: &str, budget: Budget) -> BudgetBreach {
    match ingest_bytes(text.as_bytes(), FormatHint::Sniff, &budget) {
        Err(IngestError::Budget(b)) => b,
        other => panic!("{other:?}"),
    }
}

fn cell(s: &str) -> Option<String> {
    Some(s.to_string())
}

const ARRAY: &str = r#"[
  {"n": 1, "amount": 1.50, "ok": true, "day": "2024-02-01", "who": "a", "tags": ["x", "y"]},
  {"n": 2, "amount": -0.05, "ok": false, "day": "2024-02-02", "who": "b \"q\" é",
   "tags": {"k": [1, 2]}, "extra": 7},
  {"who": "c", "n": null}
]"#;

#[test]
fn an_array_of_objects_opens_with_inferred_types() {
    let table = sniff(ARRAY);
    let report = table.report();
    assert_eq!(report.format, Format::Json);
    assert!(report.header);
    assert_eq!(report.rows, 3);
    let names: Vec<_> = report.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["n", "amount", "ok", "day", "who", "tags", "extra"]);
    let col = |name: &str| report.columns.iter().find(|c| c.name == name).unwrap();
    assert_eq!(col("n").inferred, InferredType::Integer);
    assert!(
        matches!(
            col("amount").inferred,
            InferredType::Decimal { scale: 2, .. }
        ),
        "{:?}",
        col("amount").inferred
    );
    assert_eq!(col("ok").inferred, InferredType::Boolean);
    assert_eq!(col("day").inferred, InferredType::Date);
    assert_eq!(col("who").inferred, InferredType::Text);
    assert_eq!(col("tags").inferred, InferredType::Text);
    assert_eq!(
        col("tags").note,
        Some("kept as text: a value is a JSON object or array")
    );
    assert_eq!(col("extra").inferred, InferredType::Integer);
    // A missing key and a null are both NULL, and neither is a ragged row.
    assert_eq!(col("extra").nulls, 2);
    assert_eq!(col("n").nulls, 1);
    assert_eq!(report.rejected, vec![]);

    let rows = table.preview(3);
    assert_eq!(
        rows[0],
        vec![
            cell("1"),
            // The digits as written: a float would have made this 1.5.
            cell("1.50"),
            cell("true"),
            cell("2024-02-01"),
            cell("a"),
            cell(r#"["x", "y"]"#),
            None,
        ]
    );
    assert_eq!(rows[1][1], cell("-0.05"));
    assert_eq!(rows[1][4], cell("b \"q\" \u{e9}"));
    assert_eq!(rows[1][5], cell(r#"{"k": [1, 2]}"#));
    assert_eq!(rows[1][6], cell("7"));
    assert_eq!(rows[2], vec![None, None, None, None, cell("c"), None, None]);
}

#[test]
fn objects_one_after_another_read_as_the_array_does() {
    let want = sniff(r#"[{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]"#).preview(9);
    for text in [
        "{\"a\": 1, \"b\": \"x\"}\n{\"a\": 2, \"b\": \"y\"}\n",
        "{\"a\": 1, \"b\": \"x\"}\r\n\r\n{\"a\": 2, \"b\": \"y\"}",
        "{\"a\": 1, \"b\": \"x\"}{\"a\": 2, \"b\": \"y\"}",
        "{\n  \"a\": 1,\n  \"b\": \"x\"\n}\n{\n  \"b\": \"y\",\n  \"a\": 2\n}\n",
        "\u{feff}  {\"a\": 1, \"b\": \"x\"}\n{\"a\": 2, \"b\": \"y\"}",
    ] {
        let table = sniff(text);
        assert_eq!(table.report().format, Format::Json, "{text:?}");
        assert_eq!(table.preview(9), want, "{text:?}");
    }
}

#[test]
fn json_is_held_to_each_budget() {
    let three = r#"[{"a": 1}, {"a": 2}, {"a": 3}]"#;
    assert_eq!(
        breach(three, Budget::untrusted().max_rows(2)),
        BudgetBreach {
            which: "max_rows",
            limit: 2,
            at: 3
        }
    );
    assert_eq!(sniff(r#"[{"a": 1}, {"a": 2}]"#).report().rows, 2);
    assert_eq!(
        breach(
            r#"{"a": 1, "b": 2}
{"c": 3}"#,
            Budget::untrusted().max_columns(2)
        ),
        BudgetBreach {
            which: "max_columns",
            limit: 2,
            at: 3
        }
    );
    assert_eq!(
        breach(
            r#"{"a": 1, "long key": 2}"#,
            Budget::untrusted().max_key_bytes(7)
        ),
        BudgetBreach {
            which: "max_key_bytes",
            limit: 7,
            at: 8
        }
    );
    assert_eq!(
        breach(r#"{"a": "abcde"}"#, Budget::untrusted().max_cell_bytes(4)),
        BudgetBreach {
            which: "max_cell_bytes",
            limit: 4,
            at: 5
        }
    );
    assert_eq!(
        breach(
            r#"{"a": {"b": [1, 2]}}"#,
            Budget::untrusted().max_cell_bytes(10)
        ),
        BudgetBreach {
            which: "max_cell_bytes",
            limit: 10,
            at: 13
        }
    );
    assert_eq!(
        breach(r#"{"a": [[["]]]"]]]}"#, Budget::untrusted().max_depth(2)),
        BudgetBreach {
            which: "max_depth",
            limit: 2,
            at: 3
        }
    );
    // Brackets inside a string, after an escaped quote too, are not depth.
    let quoted = r#"{"a": [["\"[[[", "[[["]]}"#;
    let two = Budget::untrusted().max_depth(2);
    assert!(ingest_bytes(quoted.as_bytes(), FormatHint::Sniff, &two).is_ok());
    assert_eq!(
        breach(three, Budget::untrusted().max_bytes(10)).which,
        "max_bytes"
    );

    // Rows are charged as they are read, not once the whole file has been.
    let many = "{\"a\": 1}\n".repeat(10_000);
    let limit = many.len() as u64 + 10_000;
    let b = breach(&many, Budget::untrusted().max_memory_bytes(limit));
    assert_eq!(b.which, "max_memory_bytes");
    assert!(b.at < limit + 100, "{b:?}");

    let len = three.len() as u64;
    let b = breach(three, Budget::untrusted().max_memory_bytes(len + 40));
    assert_eq!(b.which, "max_memory_bytes");
    let used = sniff(three).report().budget_used.memory_bytes;
    assert!(used > len + 40, "{used}");
    let fits = Budget::untrusted().max_memory_bytes(used);
    assert!(ingest_bytes(three.as_bytes(), FormatHint::Sniff, &fits).is_ok());
}

#[test]
fn a_value_nested_past_the_stack_is_refused_by_depth() {
    let deep = format!(
        "[{{\"a\": {}{}}}]",
        "[".repeat(200_000),
        "]".repeat(200_000)
    );
    assert_eq!(
        breach(&deep, Budget::untrusted()),
        BudgetBreach {
            which: "max_depth",
            limit: 64,
            at: 65
        }
    );
}

#[test]
fn a_csv_whose_first_cell_opens_a_bracket_is_still_csv() {
    for (text, first) in [
        ("[a],b\n1,2\n3,4\n", "[a]"),
        ("{x},y\n1,2\n3,4\n", "{x}"),
        ("  [1],b\n1,2\n", "[1]"),
        ("{\"a\": 1},note\n{\"a\": 2},second\n", "column_1"),
    ] {
        let table = sniff(text);
        assert_eq!(table.report().format, Format::Csv, "{text:?}");
        assert_eq!(table.report().columns[0].name, first, "{text:?}");
        assert_eq!(table.report().columns.len(), 2, "{text:?}");
        // Told it is JSON, the same bytes are not.
        let err = ingest_bytes(text.as_bytes(), FormatHint::Json, &Budget::untrusted());
        assert_eq!(err.unwrap_err(), INVALID, "{text:?}");
    }
    // A budget crossed while it still read as JSON is the answer, not a reason to try CSV.
    let b = breach(
        "{\"a\": 1}\n{\"a\": 2}\nnot json",
        Budget::untrusted().max_rows(1),
    );
    assert_eq!(b.which, "max_rows");
}

#[test]
fn what_is_not_records_is_refused_without_its_text() {
    for text in [
        r#"[{"a": "CANARY_VALUE_991"} 5]"#,
        r#"["CANARY_VALUE_991"]"#,
        r#"[{"a": 1}, "CANARY_VALUE_991"]"#,
        r#""CANARY_VALUE_991""#,
        r#"{"CANARY_VALUE_991": 1"#,
        r#"{"a": 1} CANARY_VALUE_991"#,
        r#"{"a": "CANARY_VALUE_991\q"}"#,
        r#"[{"a": 1}] [{"a": 2}]"#,
        r#"{"a": 01}"#,
    ] {
        let err =
            ingest_bytes(text.as_bytes(), FormatHint::Json, &Budget::untrusted()).unwrap_err();
        assert_eq!(err, INVALID, "{text}");
        assert!(!err.to_string().contains("CANARY"), "{err}");
    }
}

#[test]
fn keys_repeat_escape_and_go_missing() {
    let table = sniff(
        r#"{"kéy": 1, "": "blank", "a": 1, "a": 2}
{"a": 3, "a": 4, "a": 5}
{}"#,
    );
    let names: Vec<_> = table
        .report()
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["k\u{e9}y", "column_2", "a"]);
    assert_eq!(table.report().rows, 3);
    assert_eq!(
        table.preview(3),
        vec![
            vec![cell("1"), cell("blank"), cell("2")],
            vec![None, None, cell("5")],
            vec![None, None, None],
        ]
    );
}

#[test]
fn numbers_keep_what_was_written() {
    let table = sniff(
        r#"[{"big": 18446744073709551616, "exp": 1e400, "zero": "007", "n": -0},
            {"big": 1, "exp": 2.5E-3, "zero": "008", "n": 12}]"#,
    );
    let rows = table.preview(2);
    assert_eq!(rows[0][0], cell("18446744073709551616"));
    assert_eq!(rows[0][1], cell("1e400"));
    assert_eq!(rows[1][1], cell("2.5E-3"));
    assert_eq!(rows[0][2], cell("007"));
    assert_eq!(table.report().columns[1].inferred, InferredType::Text);
}

#[test]
fn an_empty_array_is_a_table_of_no_rows() {
    for text in ["[]", " [ ] \n"] {
        let table = sniff(text);
        assert_eq!(table.report().format, Format::Json);
        assert_eq!(table.report().rows, 0);
        assert!(table.batches().is_empty());
    }
}

#[test]
fn a_key_that_reads_as_a_number_is_still_a_name() {
    let table = sniff(r#"{"2024": "x", "1.5": "y"}"#);
    let names: Vec<_> = table
        .report()
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["2024", "1.5"]);
    assert_eq!(table.preview(2), vec![vec![cell("x"), cell("y")]]);
}

#[test]
fn a_record_does_not_see_the_cells_of_the_one_before() {
    let table =
        sniff("{\"a\": 1, \"b\": 2, \"c\": 3}\n{\"c\": 9}\n{\"a\": 7}\n{\"b\": 5, \"b\": 6}");
    assert_eq!(
        table.preview(4),
        vec![
            vec![cell("1"), cell("2"), cell("3")],
            vec![None, None, cell("9")],
            vec![cell("7"), None, None],
            vec![None, cell("6"), None],
        ]
    );
    assert_eq!(table.report().rejected, vec![]);
}
