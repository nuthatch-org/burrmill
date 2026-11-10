//! The slice-1 reader: budgets, a hostile corpus, and no cell values on the report.

use std::io::Read;
use std::path::PathBuf;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Date32Type, TimestampMicrosecondType};
use burrmill_ingest::{
    Budget, BudgetBreach, EncodingKind, FormatHint, InferredType, IngestError, Issue, Locale,
    MAX_ISSUES, ReadSource, Table, ingest, ingest_bytes,
};

fn corpus(name: &str) -> Vec<u8> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.pop();
    path.push("corpus");
    path.push("ingest");
    path.push(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn go(bytes: &[u8]) -> Table {
    let table = ingest_bytes(bytes, FormatHint::Sniff, &Budget::untrusted())
        .unwrap_or_else(|e| panic!("ingest failed: {e}"));
    let rows: usize = table.batches().iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, table.report().rows as usize);
    if let Some(batch) = table.batches().first() {
        assert_eq!(batch.num_columns(), table.report().columns.len());
        for field in batch.schema().fields() {
            assert!(field.is_nullable());
            assert!(
                !matches!(
                    field.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ),
                "{}",
                field.data_type()
            );
        }
    }
    assert!(table.report().rejected.len() <= MAX_ISSUES);
    table
}

fn text(s: &str) -> Table {
    go(s.as_bytes())
}

fn shown(table: &Table) -> String {
    format!("{table:?}\n{}", table.report())
}

fn breach(err: IngestError) -> BudgetBreach {
    match err {
        IngestError::Budget(b) => b,
        other => panic!("expected a budget error, got {other}"),
    }
}

fn col_type(table: &Table, i: usize) -> &InferredType {
    &table.report().columns[i].inferred
}

/// RFC defaults, so a later edit that "tunes" a limit has to say so here.
#[test]
fn untrusted_budget_matches_the_rfc() {
    let b = Budget::untrusted();
    assert_eq!(b.max_bytes, 200 << 20);
    assert_eq!(b.max_rows, 5_000_000);
    assert_eq!(b.max_columns, 1_024);
    assert_eq!(b.max_cell_bytes, 1 << 20);
    assert_eq!(b.max_memory_bytes, 512 << 20);
    assert_eq!(b.sniff_bytes, 64 << 10);
    assert_eq!(b.max_millis, None);
}

/// Logging a table is the report. The value stays behind `preview`.
#[test]
fn the_report_does_not_contain_cell_values() {
    let table = go(&corpus("canary.csv"));
    let dump = shown(&table);
    assert!(!dump.contains("CANARY_VALUE_991"), "{dump}");
    assert!(!dump.contains("12.50"), "{dump}");
    assert_eq!(
        table.preview(4),
        vec![vec![
            Some("CANARY_VALUE_991".to_string()),
            Some("12.50".to_string())
        ]]
    );
    assert!(matches!(
        col_type(&table, 1),
        InferredType::Decimal { scale: 2, .. }
    ));
}

#[test]
fn every_corpus_file_finishes_without_leaking() {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.pop();
    dir.pop();
    dir.push("corpus");
    dir.push("ingest");
    let mut n = 0;
    for ent in std::fs::read_dir(&dir).unwrap() {
        let path = ent.unwrap().path();
        if !path.is_file() {
            continue;
        }
        n += 1;
        let bytes = std::fs::read(&path).unwrap();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if name.starts_with("magic-") {
            let err = ingest_bytes(&bytes, FormatHint::Sniff, &Budget::untrusted()).unwrap_err();
            let msg = err.to_string();
            assert!(!msg.contains("CANARY_VALUE_991"), "{name}: {msg}");
            assert!(
                matches!(err, IngestError::Unsupported { .. }),
                "{name}: {err}"
            );
        } else {
            let table = go(&bytes);
            let dump = shown(&table);
            assert!(!dump.contains("CANARY_VALUE_991"), "{name} leaked\n{dump}");
        }
    }
    assert!(n >= 14, "corpus shrank to {n}");
}

#[test]
fn parquet_zip_and_gzip_are_refused_before_parse() {
    let cases = [
        ("magic-parquet.bin", "parquet"),
        ("magic-zip.bin", "zip"),
        ("magic-gzip.bin", "gzip"),
    ];
    for (name, format) in cases {
        let err = ingest_bytes(&corpus(name), FormatHint::Sniff, &Budget::untrusted()).unwrap_err();
        assert_eq!(err, IngestError::Unsupported { format }, "{name}");
    }
    // A caller who already knows it is CSV gets text, not a container open.
    let table = ingest_bytes(
        &corpus("magic-parquet.bin"),
        FormatHint::Csv,
        &Budget::untrusted(),
    )
    .unwrap();
    assert!(!shown(&table).contains("CANARY_VALUE_991"));
    assert_eq!(
        table.preview(1)[0][0].as_deref(),
        Some("PAR1CANARY_VALUE_991")
    );
}

#[test]
fn broken_quoting_is_one_field_and_a_row_number() {
    let table = go(&corpus("broken-quoting.csv"));
    assert!(table.report().header);
    assert_eq!(
        table.preview(2),
        vec![vec![Some("Ivan,Sofia".into()), None]]
    );
    assert!(
        table
            .report()
            .rejected
            .iter()
            .any(|i| i.reason == Issue::UnclosedQuote && i.row == 1)
    );
    assert!(table.report().rejected.iter().any(|i| {
        i.reason
            == Issue::Ragged {
                expected: 2,
                got: 1,
            }
            && i.column.as_deref() == Some("city")
    }));
    assert!(!shown(&table).contains("Ivan"));
}

#[test]
fn a_quoted_newline_stays_inside_the_field() {
    let table = go(&corpus("embedded-newline.csv"));
    assert_eq!(
        table.preview(2),
        vec![vec![Some("hello\nworld".into()), Some("7".into())]]
    );
    assert!(matches!(col_type(&table, 1), InferredType::Integer));
    assert!(!shown(&table).contains("hello"));
}

#[test]
fn a_short_row_is_padded_and_reported_once() {
    let table = go(&corpus("ragged.csv"));
    assert_eq!(
        table.preview(5),
        vec![
            vec![Some("1".into()), Some("2".into()), Some("3".into())],
            vec![Some("4".into()), Some("5".into()), None],
            vec![Some("6".into()), Some("7".into()), Some("8".into())],
        ]
    );
    let ragged: Vec<_> = table
        .report()
        .rejected
        .iter()
        .filter(|i| matches!(i.reason, Issue::Ragged { .. }))
        .collect();
    assert_eq!(ragged.len(), 1);
    assert_eq!(ragged[0].row, 2);
    assert_eq!(ragged[0].column.as_deref(), Some("c"));
    assert_eq!(
        ragged[0].reason,
        Issue::Ragged {
            expected: 3,
            got: 2
        }
    );
}

#[test]
fn a_mid_file_bom_is_data_and_a_leading_bom_is_not() {
    let table = go(&corpus("mid-file-bom.csv"));
    assert_eq!(table.preview(1)[0][1].as_deref(), Some("\u{feff}Sofia"));
    assert!(!shown(&table).contains("Sofia"));

    let table = go(b"\xef\xbb\xbfa,b\n1,2\n");
    assert_eq!(table.report().columns[0].name, "a");
    assert_eq!(
        table.preview(1),
        vec![vec![Some("1".into()), Some("2".into())]]
    );
}

#[test]
fn html_and_a_formula_are_text() {
    let html = go(&corpus("html-as-csv.csv"));
    assert!(
        html.preview(8)
            .iter()
            .flatten()
            .flatten()
            .any(|c| c.contains("CANARY_VALUE_991"))
    );
    assert!(!shown(&html).contains("CANARY_VALUE_991"));

    let formula = go(&corpus("formula.csv"));
    assert_eq!(formula.preview(1)[0][0].as_deref(), Some("rent"));
    assert_eq!(
        formula.preview(1)[0][1].as_deref(),
        Some("=cmd|'/C calc'!A0")
    );
    assert!(matches!(col_type(&formula, 1), InferredType::Text));
    assert!(!shown(&formula).contains("calc"));
    assert!(!shown(&formula).contains("rent"));
}

#[test]
fn a_bulgarian_export_keeps_cents_and_dotted_dates() {
    let table = go(&corpus("comma-decimal.csv"));
    let d = &table.report().dialect;
    assert_eq!(d.delimiter, b';');
    assert_eq!(d.locale, Locale::Comma);
    assert_eq!(d.encoding, EncodingKind::Utf8);
    assert!(table.report().header);
    assert!(matches!(col_type(&table, 0), InferredType::Date));
    assert!(matches!(col_type(&table, 1), InferredType::Text));
    assert!(
        matches!(col_type(&table, 2), InferredType::Decimal { scale: 2, precision } if *precision >= 6)
    );
    assert_eq!(
        table.preview(5),
        vec![
            vec![
                Some("2024-02-01".into()),
                Some("rent".into()),
                Some("1234.56".into())
            ],
            vec![
                Some("2024-04-03".into()),
                Some("food".into()),
                Some("2.50".into())
            ],
            vec![
                Some("2024-06-05".into()),
                Some("pay".into()),
                Some("-500.00".into())
            ],
        ]
    );
    let dump = shown(&table);
    for leaked in ["rent", "food", "pay", "1234", "1.234"] {
        assert!(!dump.contains(leaked), "{leaked} in\n{dump}");
    }
    let days = table.batches()[0]
        .column(0)
        .as_primitive::<Date32Type>()
        .value(0);
    let again = text("d\n1970-01-01\n");
    assert_eq!(
        again.batches()[0]
            .column(0)
            .as_primitive::<Date32Type>()
            .value(0),
        0
    );
    assert!(days > 0);
}

#[test]
fn windows_1251_semicolon_and_windows_1252_cafe() {
    let table = go(&corpus("semicolon-1251.csv"));
    let d = &table.report().dialect;
    assert_eq!(d.encoding, EncodingKind::Windows1251);
    assert_eq!(d.delimiter, b';');
    assert_eq!(d.locale, Locale::Comma);
    assert_eq!(
        table.preview(4),
        vec![
            vec![
                Some("Иван".into()),
                Some("София".into()),
                Some("1234.56".into())
            ],
            vec![
                Some("Мария".into()),
                Some("Пловдив".into()),
                Some("2.50".into())
            ],
        ]
    );
    let dump = shown(&table);
    for leaked in ["Иван", "София", "Мария", "Пловдив", "1234"] {
        assert!(!dump.contains(leaked), "{leaked} in\n{dump}");
    }

    let cafe = go(b"name\ncaf\xe9\n");
    assert_eq!(cafe.report().dialect.encoding, EncodingKind::Windows1252);
    assert_eq!(cafe.preview(1)[0][0].as_deref(), Some("café"));
    assert!(!shown(&cafe).contains("caf"));
}

/// Byte 0x98 is undefined in the Python codec and is U+0098 on the WHATWG page.
#[test]
fn undefined_1251_bytes_decode_as_themselves() {
    // One row, so the cell stays a value. A second text row would be read as a header.
    let table = go(b"\xc8\xe2\xe0\xed\x98");
    assert_eq!(table.report().dialect.encoding, EncodingKind::Windows1251);
    assert_eq!(table.preview(1)[0][0].as_deref(), Some("Иван\u{0098}"));
    assert!(!table.report().to_string().contains("Иван"));
}

#[test]
fn an_empty_file_and_a_header_only_file() {
    let empty = go(&corpus("empty.csv"));
    assert_eq!(empty.report().rows, 0);
    assert!(!empty.report().header);
    assert!(empty.batches().is_empty());
    assert!(empty.preview(3).is_empty());

    let only = go(&corpus("header-only.csv"));
    assert!(!only.report().header);
    assert_eq!(only.report().rows, 1);
    assert_eq!(only.report().columns[0].name, "column_1");
    assert_eq!(only.report().columns[1].name, "column_2");
    assert_eq!(
        only.preview(1),
        vec![vec![Some("name".into()), Some("city".into())]]
    );
    let display = only.report().to_string();
    assert!(!display.contains("name"), "{display}");
    assert!(!display.contains("city"), "{display}");
    let debug = format!("{only:?}");
    assert!(
        !debug.contains("\"name\"") && !debug.contains("\"city\""),
        "{debug}"
    );
}

#[test]
fn one_text_row_is_data_and_two_text_rows_are_a_header() {
    let one = text("Ivan,Sofia\n");
    assert!(!one.report().header);
    assert_eq!(
        one.preview(1),
        vec![vec![Some("Ivan".into()), Some("Sofia".into())]]
    );

    let two = text("name,city\nIvan,Sofia\n");
    assert!(two.report().header);
    assert_eq!(two.report().columns[0].name, "name");
    assert_eq!(
        two.preview(1),
        vec![vec![Some("Ivan".into()), Some("Sofia".into())]]
    );

    let nums = text("1,2\n3,4\n");
    assert!(!nums.report().header);
    assert_eq!(nums.report().columns[0].name, "column_1");
    assert_eq!(nums.preview(2)[1], vec![Some("3".into()), Some("4".into())]);
}

#[test]
fn names_fill_blanks_and_suffix_duplicates() {
    let table = text("a,a,\n1,2,3\n");
    let names: Vec<_> = table
        .report()
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, vec!["a", "a_2", "column_3"]);
    assert_eq!(
        table.preview(1),
        vec![vec![Some("1".into()), Some("2".into()), Some("3".into())]]
    );

    let blanks = text("a,,c\n1,2,3\n");
    let names: Vec<_> = blanks
        .report()
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, vec!["a", "column_2", "c"]);
}

#[test]
fn booleans_are_words_and_zero_one_stay_integers() {
    let flags = text("flag\nyes\nNO\ntrue\nfalse\n");
    assert!(matches!(col_type(&flags, 0), InferredType::Boolean));
    assert_eq!(
        flags.preview(4),
        vec![
            vec![Some("true".into())],
            vec![Some("false".into())],
            vec![Some("true".into())],
            vec![Some("false".into())],
        ]
    );

    let bits = text("n\n0\n1\n0\n");
    assert!(matches!(col_type(&bits, 0), InferredType::Integer));
    assert_eq!(
        bits.preview(3),
        vec![
            vec![Some("0".into())],
            vec![Some("1".into())],
            vec![Some("0".into())]
        ]
    );
}

#[test]
fn a_column_keeps_a_type_at_ninety_percent() {
    let mut s = String::from("n\n");
    for _ in 0..9 {
        s.push_str("1\n");
    }
    s.push_str("x\n");
    let table = text(&s);
    assert!(matches!(col_type(&table, 0), InferredType::Integer));
    assert_eq!(table.preview(10).last().unwrap(), &vec![None]);
    assert_eq!(table.report().rejected.len(), 1);
    assert_eq!(table.report().rejected[0].reason, Issue::TypeMismatch);
    assert_eq!(table.report().rejected[0].row, 10);

    let mut s = String::from("n\n");
    for _ in 0..8 {
        s.push_str("1\n");
    }
    s.push_str("x\ny\n");
    let table = text(&s);
    assert!(matches!(col_type(&table, 0), InferredType::Text));
    assert!(table.report().rejected.is_empty());
    assert_eq!(table.preview(10).last().unwrap()[0].as_deref(), Some("y"));
}

#[test]
fn an_integer_beside_a_decimal_becomes_a_decimal() {
    let table = text("v;n\n10;a\n20;b\n1,50;c\n");
    assert_eq!(table.report().dialect.locale, Locale::Comma);
    assert!(matches!(
        col_type(&table, 0),
        InferredType::Decimal { scale: 2, .. }
    ));
    assert_eq!(
        table.preview(3),
        vec![
            vec![Some("10.00".into()), Some("a".into())],
            vec![Some("20.00".into()), Some("b".into())],
            vec![Some("1.50".into()), Some("c".into())],
        ]
    );
}

#[test]
fn quoted_grouped_numbers_in_either_locale() {
    let euro = text("amount\n\"1.234,56\"\n\"2,50\"\n");
    assert_eq!(euro.report().dialect.locale, Locale::Comma);
    assert_eq!(euro.report().columns.len(), 1);
    assert_eq!(
        euro.preview(2),
        vec![vec![Some("1234.56".into())], vec![Some("2.50".into())]]
    );

    let us = text("name,amount\nrent,\"1,234.56\"\n");
    assert_eq!(us.report().dialect.locale, Locale::Point);
    assert_eq!(us.preview(1)[0][1].as_deref(), Some("1234.56"));
}

#[test]
fn dates_and_timestamps_share_an_epoch() {
    let dates = text("d\n2024-02-01\n01.02.2024\n");
    assert!(matches!(col_type(&dates, 0), InferredType::Date));
    assert_eq!(
        dates.preview(2),
        vec![
            vec![Some("2024-02-01".into())],
            vec![Some("2024-02-01".into())]
        ]
    );

    let stamps = text("t\n1970-01-01T00:00:00\n1970-01-01 00:00:01.5\n");
    assert!(matches!(col_type(&stamps, 0), InferredType::Timestamp));
    let col = stamps.batches()[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>();
    assert_eq!(col.value(0), 0);
    assert_eq!(col.value(1), 1_500_000);
    assert_eq!(
        stamps.preview(2),
        vec![vec![Some("0".into())], vec![Some("1500000".into())]]
    );
}

#[test]
fn blank_lines_go_and_a_row_of_commas_stays() {
    let table = text("a,b\n\n1,2\n\n");
    assert_eq!(table.report().rows, 1);
    assert_eq!(
        table.preview(2),
        vec![vec![Some("1".into()), Some("2".into())]]
    );

    let kept = text("a,b\n,,\n");
    assert_eq!(kept.report().rows, 1);
    assert_eq!(kept.report().columns.len(), 3);
    assert_eq!(kept.preview(1), vec![vec![None, None, None]]);
}

#[test]
fn quotes_commas_and_crlf() {
    let doubled = text("a,b\n\"say \"\"hi\"\"\",1\n");
    assert_eq!(
        doubled.preview(1),
        vec![vec![Some("say \"hi\"".into()), Some("1".into())]]
    );

    let mid = text("a\"b,c\n");
    assert_eq!(
        mid.preview(1),
        vec![vec![Some("a\"b".into()), Some("c".into())]]
    );

    let crlf = go(b"a,b\r\n1,2\r\n");
    assert_eq!(
        crlf.preview(1),
        vec![vec![Some("1".into()), Some("2".into())]]
    );

    let tabs = go(b"a\tb\n1\t2\n");
    assert_eq!(tabs.report().dialect.delimiter, b'\t');
    assert_eq!(
        tabs.preview(1),
        vec![vec![Some("1".into()), Some("2".into())]]
    );

    let singles = text("name,note\n'hello, world',1\n");
    assert_eq!(singles.report().dialect.quote, b'\'');
    assert_eq!(singles.preview(1)[0][0].as_deref(), Some("hello, world"));

    let trimmed = text("a,b\n  1 , 2 \n");
    assert_eq!(
        trimmed.preview(1),
        vec![vec![Some("1".into()), Some("2".into())]]
    );
}

#[test]
fn utf16_bom_roundtrips() {
    let text = "name,city\nIvan,Sofia\n";
    let mut le = vec![0xFF, 0xFE];
    le.extend(text.encode_utf16().flat_map(|u| u.to_le_bytes()));
    let table = go(&le);
    assert_eq!(table.report().dialect.encoding, EncodingKind::Utf16Le);
    assert_eq!(
        table.preview(1),
        vec![vec![Some("Ivan".into()), Some("Sofia".into())]]
    );

    let mut be = vec![0xFE, 0xFF];
    be.extend(text.encode_utf16().flat_map(|u| u.to_be_bytes()));
    let table = go(&be);
    assert_eq!(table.report().dialect.encoding, EncodingKind::Utf16Be);
    assert_eq!(table.preview(1)[0][1].as_deref(), Some("Sofia"));
}

#[test]
fn a_short_sniff_window_does_not_panic_on_a_bad_tail() {
    let mut bytes = b"a,b\n1,2\n".to_vec();
    let budget = Budget::untrusted().sniff_bytes(bytes.len() as u64);
    bytes.push(0xFF);
    let table = ingest_bytes(&bytes, FormatHint::Sniff, &budget).unwrap();
    assert_eq!(table.report().rows, 2);
    assert_eq!(table.report().dialect.encoding, EncodingKind::Utf8);
}

#[test]
fn issue_storage_stops_at_sixty_four() {
    let mut s = String::from("a,b,c\n");
    for _ in 0..70 {
        s.push_str("1,2\n");
    }
    let table = text(&s);
    assert_eq!(table.report().rejected.len(), MAX_ISSUES);
    assert_eq!(table.report().rejected_omitted, 6);
    assert!(table.report().rows > MAX_ISSUES as u64);
}

#[test]
fn crossing_a_budget_returns_no_table() {
    let canary = b"CANARY_VALUE_991 extra";
    let err =
        ingest_bytes(canary, FormatHint::Sniff, &Budget::untrusted().max_bytes(4)).unwrap_err();
    assert!(!err.to_string().contains("CANARY_VALUE_991"), "{err}");
    let b = breach(err);
    assert_eq!(b.which, "max_bytes");
    assert_eq!(b.at, canary.len() as u64);

    let err = ingest_bytes(
        b"abcd",
        FormatHint::Sniff,
        &Budget::untrusted().max_memory_bytes(3),
    )
    .unwrap_err();
    assert_eq!(breach(err).which, "max_memory_bytes");

    let err = ingest_bytes(
        b"abcd",
        FormatHint::Sniff,
        &Budget::untrusted().max_cell_bytes(3),
    )
    .unwrap_err();
    let b = breach(err);
    assert_eq!(b.which, "max_cell_bytes");
    assert_eq!(b.at, 4);

    let err = ingest_bytes(
        b"a,b,c\n",
        FormatHint::Sniff,
        &Budget::untrusted().max_columns(2),
    )
    .unwrap_err();
    assert_eq!(breach(err).which, "max_columns");

    let err = ingest_bytes(
        b"1\n2\n",
        FormatHint::Sniff,
        &Budget::untrusted().max_rows(1),
    )
    .unwrap_err();
    let b = breach(err);
    assert_eq!(b.which, "max_rows");
    assert_eq!(b.at, 2);

    let err = ingest_bytes(
        b"h\n1\n2\n",
        FormatHint::Sniff,
        &Budget::untrusted().max_rows(1),
    )
    .unwrap_err();
    assert_eq!(breach(err).at, 3);

    let ok = ingest_bytes(
        b"h\n1\n",
        FormatHint::Sniff,
        &Budget::untrusted().max_rows(1),
    )
    .unwrap();
    assert!(ok.report().header);
    assert_eq!(ok.report().rows, 1);

    let mut wide = Vec::new();
    for i in 0..1024 {
        if i > 0 {
            wide.push(b',');
        }
        wide.extend(b"1");
    }
    assert!(ingest_bytes(&wide, FormatHint::Sniff, &Budget::untrusted()).is_ok());
    wide.push(b',');
    wide.extend(b"1");
    assert_eq!(
        breach(ingest_bytes(&wide, FormatHint::Sniff, &Budget::untrusted()).unwrap_err()).which,
        "max_columns"
    );

    let huge = vec![b'a'; (1 << 20) + 1];
    let err = ingest_bytes(&huge, FormatHint::Sniff, &Budget::untrusted()).unwrap_err();
    assert_eq!(breach(err).which, "max_cell_bytes");
}

#[test]
fn chunks_past_sixty_four_kib_and_a_slow_clock_still_finish() {
    let mut s = String::from("n\n");
    for _ in 0..8_000 {
        s.push_str("123456789\n");
    }
    let budget = Budget::untrusted().max_millis(u64::MAX);
    let table = ingest_bytes(s.as_bytes(), FormatHint::Sniff, &budget).unwrap();
    assert_eq!(table.report().rows, 8_000);
    assert_eq!(table.preview(1)[0][0].as_deref(), Some("123456789"));
    assert_eq!(table.preview(8_000).len(), 8_000);
}

struct Trap;

impl Read for Trap {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        panic!("hint must be charged before the first read");
    }
}

struct Once {
    data: &'static [u8],
    pos: usize,
    tripped: bool,
}

impl Read for Once {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.tripped {
            self.tripped = true;
            return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "once"));
        }
        if self.pos >= self.data.len() || buf.is_empty() {
            return Ok(0);
        }
        buf[0] = self.data[self.pos];
        self.pos += 1;
        Ok(1)
    }
}

struct Boom;

impl Read for Boom {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("disk gone"))
    }
}

#[test]
fn readers_honour_the_hint_an_interrupt_and_a_failure() {
    let err = ingest(
        &mut ReadSource::new(Trap).with_hint(100),
        FormatHint::Sniff,
        &Budget::untrusted().max_bytes(10),
    )
    .unwrap_err();
    assert_eq!(breach(err).which, "max_bytes");
    assert_eq!(
        breach(
            ingest(
                &mut ReadSource::new(Trap).with_hint(100),
                FormatHint::Sniff,
                &Budget::untrusted().max_bytes(10),
            )
            .unwrap_err()
        )
        .at,
        100
    );

    let src = Once {
        data: b"a,b\n1,2\n",
        pos: 0,
        tripped: false,
    };
    let table = ingest(
        &mut ReadSource::new(src),
        FormatHint::Sniff,
        &Budget::untrusted(),
    )
    .unwrap();
    assert_eq!(
        table.preview(1),
        vec![vec![Some("1".into()), Some("2".into())]]
    );

    let err = ingest(
        &mut ReadSource::new(Boom),
        FormatHint::Sniff,
        &Budget::untrusted(),
    )
    .unwrap_err();
    assert_eq!(err, IngestError::Source("disk gone".into()));
}

#[test]
fn random_bytes_do_not_panic() {
    let mut state = 0x5eed_0047_u64;
    for n in 0..400 {
        let len = (xorshift(&mut state) as usize) % 300;
        let mut data = vec![0u8; len];
        for b in &mut data {
            *b = xorshift(&mut state) as u8;
        }
        // A few shapes the generator is unlikely to hit on its own.
        if n == 0 {
            data = vec![b'"'; 40];
        } else if n == 1 {
            data = b"\"\"\"\"\"\"\"\"\"\"\n,\n".to_vec();
        }
        for hint in [FormatHint::Sniff, FormatHint::Csv] {
            let data = data.clone();
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ingest_bytes(&data, hint, &Budget::untrusted())
            }));
            match caught {
                Ok(Ok(table)) => {
                    assert!(table.report().rejected.len() <= MAX_ISSUES);
                    let _ = table.preview(2);
                    let dump = shown(&table);
                    assert_eq!(
                        dump.contains("CANARY_VALUE_991"),
                        data.windows(16).any(|w| w == b"CANARY_VALUE_991")
                    );
                }
                Ok(Err(err)) => assert!(!err.to_string().contains("CANARY_VALUE_991"), "{err}"),
                Err(_) => panic!(
                    "panic on {hint:?} case {n} {:02x?}",
                    &data[..data.len().min(48)]
                ),
            }
        }
    }
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}
