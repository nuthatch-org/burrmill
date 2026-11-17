//! `fuzz`: generated SQL on DuckDB and on `Engine`, answers compared (the differential corpus owed
//! since slice 1).
//!
//! Queries are drawn from a typed grammar over two small tables whose data is chosen to be awkward:
//! mixed-case and NULL text, negative and missing numbers, integer strings past 64 bits and strings
//! that are not numbers, repeated join keys. Every query is deterministic: rows are compared as a
//! multiset unless the query orders by every column it returns, and nothing sums a DOUBLE.
//!
//! Outcomes: `same`, both refuse, Burrmill alone refuses by the checked rule's design (a sum over
//! `TRY_CAST` that DuckDB computes by dropping what did not fit), Burrmill alone refuses otherwise
//! (stricter, allowed, listed by reason), DuckDB alone refuses
//! (looser, reported), and different answers (reported with the seed that reproduces them:
//! `SEED=<n> CASES=1 PRINT=1 burrmill-bench fuzz`).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use crate::generate::Rng;

/// How a table's rows are cut into segments, and each segment into row groups.
#[derive(Clone, Copy)]
struct Layout {
    segments: usize,
    group: usize,
}

const ONE_FILE: Layout = Layout {
    segments: 1,
    group: 1 << 20,
};

fn write(
    dir: &std::path::Path,
    name: &str,
    schema: Arc<Schema>,
    cols: Vec<ArrayRef>,
    layout: Layout,
) -> anyhow::Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), cols)?;
    let per = batch.num_rows().div_ceil(layout.segments).max(1);
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(layout.group))
        .build();
    for (i, at) in (0..batch.num_rows()).step_by(per).enumerate() {
        let part = batch.slice(at, per.min(batch.num_rows() - at));
        let f = std::fs::File::create(dir.join(format!("{name}-{:064x}.parquet", i + 1)))?;
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), Some(props.clone()))?;
        w.write(&part)?;
        w.close()?;
    }
    Ok(())
}

fn fixture(dir: &std::path::Path, layout: Layout) -> anyhow::Result<()> {
    let mut r = Rng(7);
    let addrs = [
        Some("0xa"),
        Some("0xA"),
        Some("0xb"),
        Some("0xc"),
        Some("0xd"),
        None,
    ];
    let values = [
        Some("10"),
        Some("4"),
        Some("0"),
        Some("-5"),
        Some("010"),
        Some("7"),
        Some("123456789"),
        Some("250000000000000000000"),
        Some("abc"),
        Some(""),
        None,
    ];
    let n = 120;
    let pick = |r: &mut Rng, xs: &[Option<&'static str>]| xs[r.below(xs.len())];
    let (mut bn, mut li, mut from, mut to, mut value, mut amount, mut flag, mut kind) = (
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
    );
    let (mut ts, mut memo, mut doc) = (vec![], vec![], vec![]);
    let docs = [
        Some(r#"{"a": 1, "b": "x", "c": [1, 2, {"d": "deep"}]}"#),
        Some(r#"{"a": "7", "b": null}"#),
        Some(r#"[10, "twenty", 30.5]"#),
        Some(r#"{"b": {"a": true}, "n": 12345678901234567890}"#),
        Some("not json"),
        Some("{}"),
        None,
    ];
    let memos = [
        Some("swap 12 GRT"),
        Some("Swap 7 grt"),
        Some("stake:alice"),
        Some("stake:Bob"),
        Some(""),
        Some("a,b,,c"),
        Some("  padded  "),
        Some("0xDEADbeef"),
        Some("naïve café"),
        None,
    ];
    for i in 0..n {
        bn.push(1 + (i / 3) as u64);
        li.push((i % 3) as u64);
        from.push(pick(&mut r, &addrs));
        to.push(pick(&mut r, &addrs));
        value.push(pick(&mut r, &values));
        amount.push(if r.below(8) == 0 {
            None
        } else {
            Some(r.below(200) as i64 - 60)
        });
        flag.push(pick(&mut r, &[Some("true"), Some("false"), None]));
        kind.push(pick(&mut r, &[Some("in"), Some("out")]));
        // Two years from 2023-11-14, with some rows on the exact day, month and year boundaries.
        ts.push(match r.below(6) {
            0 => 1_704_067_200 + (r.below(3) as u64) * 86_400,
            _ => 1_700_000_000 + (r.below(63_000_000) as u64),
        });
        memo.push(pick(&mut r, &memos));
        doc.push(pick(&mut r, &docs));
    }
    let s = |v: Vec<Option<&str>>| Arc::new(StringArray::from(v)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new("flag", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("ts", DataType::UInt64, false),
        Field::new("memo", DataType::Utf8, true),
        Field::new("doc", DataType::Utf8, true),
    ]));
    write(
        dir,
        "ev",
        schema,
        vec![
            Arc::new(UInt64Array::from(bn)),
            Arc::new(UInt64Array::from(li)),
            s(from),
            s(to),
            s(value),
            Arc::new(Int64Array::from(amount)),
            s(flag),
            s(kind),
            Arc::new(UInt64Array::from(ts)),
            s(memo),
            s(doc),
        ],
        layout,
    )?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("addr", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("weight", DataType::Int64, true),
    ]));
    write(
        dir,
        "lbl",
        schema,
        vec![
            s(vec![
                Some("0xa"),
                Some("0xb"),
                Some("0xb"),
                Some("0xC"),
                None,
                Some("0xe"),
            ]),
            s(vec![
                Some("alice"),
                Some("bob"),
                Some("Bob"),
                None,
                Some("nobody"),
                Some("eve"),
            ]),
            Arc::new(Int64Array::from(vec![
                Some(3),
                Some(-1),
                None,
                Some(10),
                Some(0),
                Some(7),
            ])),
        ],
        layout,
    )?;
    Ok(())
}

/// What the current query's FROM clause offers.
struct Scope {
    joined: bool,
}

struct Gen<'a> {
    r: &'a mut Rng,
}

impl Gen<'_> {
    fn one<'s>(&mut self, xs: &[&'s str]) -> &'s str {
        xs[self.r.below(xs.len())]
    }
    fn chance(&mut self, n: usize) -> bool {
        self.r.below(n) == 0
    }

    fn int(&mut self, sc: &Scope, d: usize) -> String {
        let leaf = d == 0 || self.chance(3);
        if leaf {
            let mut cols = vec!["e.block_number", "e.log_index", "e.amount", "e.ts"];
            if sc.joined {
                cols.push("l.weight");
            }
            return match self.r.below(4) {
                0 => format!("{}", self.r.below(120) as i64 - 20),
                _ => self.one(&cols).to_string(),
            };
        }
        match self.r.below(27) {
            // Unsigned targets take a magnitude: a negative there is DuckDB's error, not a test of the cast.
            20 => match self.one(&["INTEGER", "SMALLINT", "UBIGINT", "HUGEINT", "UINTEGER"]) {
                t @ ("UBIGINT" | "UINTEGER") => {
                    format!("CAST(abs({}) AS {t})", self.int(sc, d - 1))
                }
                t => format!("CAST({} AS {t})", self.int(sc, d - 1)),
            },
            0 => format!("({} + {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            1 => format!("({} - {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            2 => format!("({} * {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            3 => format!("({} // {})", self.int(sc, d - 1), 1 + self.r.below(7)),
            4 => format!("({} % {})", self.int(sc, d - 1), 1 + self.r.below(7)),
            5 => format!("abs({})", self.int(sc, d - 1)),
            6 => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.pred(sc, d - 1),
                self.int(sc, d - 1),
                self.int(sc, d - 1)
            ),
            7 => format!("COALESCE({}, {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            8 => format!("TRY_CAST({} AS BIGINT)", self.text(sc, d - 1)),
            9 => format!("length({})", self.text(sc, d - 1)),
            10 => format!("CAST({} AS BIGINT)", self.int(sc, d - 1)),
            11 => format!("greatest({}, {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            12 => format!(
                "{}({})",
                self.one(&["year", "month", "day", "hour", "dayofweek", "epoch"]),
                self.time(sc, d - 1)
            ),
            13 => format!(
                "extract({} FROM {})",
                self.one(&["year", "month", "minute", "doy", "quarter"]),
                self.time(sc, d - 1)
            ),
            14 => format!(
                "date_part('{}', {})",
                self.one(&["hour", "dow", "week", "second"]),
                self.time(sc, d - 1)
            ),
            15 => format!(
                "strpos({}, '{}')",
                self.text(sc, d - 1),
                self.one(&["a", "0x", ":", " "])
            ),
            16 => format!("NULLIF({}, {})", self.int(sc, d - 1), self.r.below(5)),
            17 => format!("sign({})", self.int(sc, d - 1)),
            18 => format!("CAST(floor({} / 7.0) AS BIGINT)", self.int(sc, d - 1)),
            21 => format!("least({}, {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            22 => format!("xor({}, {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            23 => match self.one(&["INTEGER", "BIGINT", "HUGEINT", "UBIGINT"]) {
                t @ "UBIGINT" => format!("abs({})::{t}", self.int(sc, d - 1)),
                t => format!("({})::{t}", self.int(sc, d - 1)),
            },
            24 => format!(
                "[{}, {}, {}][{}]",
                self.r.below(10) as i64 - 3,
                self.int(sc, 0),
                self.r.below(10),
                self.one(&["1", "2", "3", "-1", "0", "4"])
            ),
            25 => format!(
                "list_reduce([{}, {}, {}], lambda a, x: a + x)",
                self.r.below(20) as i64 - 5,
                self.r.below(20),
                self.r.below(20)
            ),
            // The lodestar hex fold: a comprehension over the characters, reduced base 16.
            26 => {
                let mut hex = String::new();
                for _ in 0..1 + self.r.below(4) {
                    hex.push_str(self.one(&["0", "1", "a", "b", "f", "e"]));
                }
                format!(
                    "list_reduce([CAST(strpos('0123456789abcdef', c) - 1 AS HUGEINT) FOR c IN string_split('{hex}', '')], lambda acc, d: acc * 16 + d)"
                )
            }
            _ => format!(
                "date_diff('{}', {}, {})",
                self.one(&["day", "hour", "month"]),
                self.time(sc, d - 1),
                self.time(sc, d - 1)
            ),
        }
    }

    /// A TIMESTAMP (WITH TIME ZONE, from `to_timestamp`) or DATE expression.
    fn time(&mut self, sc: &Scope, d: usize) -> String {
        let base = "to_timestamp(e.ts)";
        if d == 0 || self.chance(3) {
            return base.into();
        }
        match self.r.below(5) {
            0 => format!(
                "date_trunc('{}', {})",
                self.one(&["day", "hour", "month", "year", "week"]),
                self.time(sc, d - 1)
            ),
            1 => format!("CAST({} AS DATE)", self.time(sc, d - 1)),
            2 => format!(
                "({} + INTERVAL {} {})",
                self.time(sc, d - 1),
                self.r.below(40),
                self.one(&["DAY", "HOUR", "MINUTE"])
            ),
            3 => format!("to_timestamp(e.ts + {})", self.int(sc, d - 1)),
            _ => base.into(),
        }
    }

    fn text(&mut self, sc: &Scope, d: usize) -> String {
        if d == 0 || self.chance(3) {
            let mut cols = vec![
                "e.\"from\"",
                "e.\"to\"",
                "e.value",
                "e.flag",
                "e.kind",
                "e.memo",
            ];
            if sc.joined {
                cols.extend(["l.addr", "l.name"]);
            }
            return match self.r.below(5) {
                0 => format!(
                    "'{}'",
                    self.one(&["0xa", "0xA", "in", "", "x y", "Bob", "10"])
                ),
                _ => self.one(&cols).to_string(),
            };
        }
        match self.r.below(27) {
            0 => format!("lower({})", self.text(sc, d - 1)),
            1 => format!("upper({})", self.text(sc, d - 1)),
            2 => format!("({} || {})", self.text(sc, d - 1), self.text(sc, d - 1)),
            3 => format!(
                "substr({}, {}, {})",
                self.text(sc, d - 1),
                1 + self.r.below(3),
                self.r.below(4)
            ),
            4 => format!("COALESCE({}, 'z')", self.text(sc, d - 1)),
            5 => format!("CAST({} AS VARCHAR)", self.int(sc, d - 1)),
            6 => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.pred(sc, d - 1),
                self.text(sc, d - 1),
                self.text(sc, d - 1)
            ),
            7 => format!("replace({}, '0x', '')", self.text(sc, d - 1)),
            8 => format!(
                "split_part({}, '{}', {})",
                self.text(sc, d - 1),
                self.one(&[",", ":", " "]),
                1 + self.r.below(3)
            ),
            9 => format!(
                "{}({})",
                self.one(&["trim", "ltrim", "rtrim", "reverse"]),
                self.text(sc, d - 1)
            ),
            10 => format!(
                "{}({}, {})",
                self.one(&["left", "right"]),
                self.text(sc, d - 1),
                self.r.below(5)
            ),
            11 => format!(
                "{}({}, {}, '*')",
                self.one(&["lpad", "rpad"]),
                self.text(sc, d - 1),
                self.r.below(12)
            ),
            12 => format!(
                "regexp_replace({}, '{}', '{}')",
                self.text(sc, d - 1),
                self.one(&["[0-9]+", "^s", "[aeiou]", "(\\w+):(\\w+)"]),
                self.one(&["#", "", "\\2-\\1"])
            ),
            13 => format!(
                "regexp_extract({}, '{}')",
                self.text(sc, d - 1),
                self.one(&["[0-9]+", "[A-Za-z]+", "0x[0-9a-fA-F]+"])
            ),
            14 => format!(
                "strftime({}, '{}')",
                self.time(sc, d - 1),
                self.one(&["%Y-%m-%d", "%H:%M", "%Y-%m-%d %H:%M:%S", "%b %d"])
            ),
            15 => format!("CAST({} AS VARCHAR)", self.time(sc, d - 1)),
            16 => format!(
                "concat_ws('-', {}, {})",
                self.text(sc, d - 1),
                self.text(sc, d - 1)
            ),
            17 => format!("repeat({}, {})", self.text(sc, d - 1), self.r.below(3)),
            18 => format!(
                "TRY(json_extract_string(e.doc, '{}'))",
                self.one(&["$.a", "$.b", "$.c[2].d", "$[1]", "$.b.a", "$.n", "a", "$[#-1]"])
            ),
            19 => format!("TRY(e.doc ->> '{}')", self.one(&["a", "$.c[0]", "$.b"])),
            20 => format!(
                "TRY(json_type(e.doc, '{}'))",
                self.one(&["$.a", "$.c", "$.n", "$"])
            ),
            // A negative start counts from the end, and a negative length runs backwards.
            22 => format!(
                "substr({}, {}, {})",
                self.text(sc, d - 1),
                self.r.below(8) as i64 - 4,
                self.r.below(6) as i64 - 2
            ),
            23 => format!(
                "string_split({}, '{}')[{}]",
                self.text(sc, d - 1),
                self.one(&[",", ":", "", "x", " "]),
                self.one(&["1", "2", "-1", "0", "3"])
            ),
            // decode, so the result is text. A bare blob in a string operation is a different
            // type, and DuckDB refuses it where a cast to text would answer.
            24 => format!(
                "decode(from_hex('{}'))",
                self.one(&["", "61", "6162", "20", "0a"])
            ),
            25 => format!("({}::VARCHAR)", self.int(sc, d - 1)),
            26 => format!(
                "[c FOR c IN string_split(COALESCE({}, 'ab'), '{}') IF c <> ''][1]",
                self.one(&["e.kind", "e.flag", "e.memo"]),
                self.one(&["", ","])
            ),
            _ => format!("NULLIF({}, '')", self.text(sc, d - 1)),
        }
    }

    fn dec(&mut self, sc: &Scope, d: usize) -> String {
        match self.r.below(5) {
            0 => "TRY_CAST(e.value AS HUGEINT)".into(),
            1 => format!("CAST({} AS HUGEINT)", self.int(sc, d.saturating_sub(1))),
            2 => format!(
                "({} * {})",
                self.dec(sc, d.saturating_sub(1)),
                self.one(&["1.5", "0.25", "2", "-3"])
            ),
            3 => format!(
                "({} + {})",
                self.dec(sc, d.saturating_sub(1)),
                self.dec(sc, d.saturating_sub(1))
            ),
            _ => "TRY_CAST(e.value AS DECIMAL(20,2))".to_string(),
        }
    }

    /// A number of mixed type: an integer beside a decimal literal, a DOUBLE, or both.
    fn num(&mut self, sc: &Scope, d: usize) -> String {
        let d = d.max(1);
        match self.r.below(12) {
            0 => format!(
                "({} {} {})",
                self.int(sc, d - 1),
                self.one(&["+", "-", "*"]),
                self.one(&["1.5", "0.1", "2.25", "-0.5"])
            ),
            1 => format!(
                "({} / {})",
                self.int(sc, d - 1),
                self.one(&["3", "7", "2.5", "0.3"])
            ),
            2 => format!("CAST({} AS DOUBLE)", self.int(sc, d - 1)),
            3 => format!("round({} / 7, {})", self.int(sc, d - 1), self.r.below(3)),
            4 => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.pred(sc, d - 1),
                self.int(sc, d - 1),
                self.one(&["2.5", "0.125", "CAST(1 AS DOUBLE)"])
            ),
            5 => format!(
                "COALESCE({}, {})",
                self.int(sc, d - 1),
                self.one(&["0.5", "1e2"])
            ),
            6 => format!(
                "greatest({}, {})",
                self.int(sc, d - 1),
                self.one(&["1.5", "20.75"])
            ),
            7 => format!("({} + {})", self.dec(sc, d - 1), self.int(sc, d - 1)),
            8 => format!(
                "CAST({} AS DECIMAL({}, {}))",
                self.int(sc, d - 1),
                10 + self.r.below(9),
                self.r.below(4)
            ),
            10 => format!(
                "power({}, {})",
                self.one(&["2", "10", "-2", "1.5"]),
                self.r.below(5)
            ),
            11 => format!("({}::DOUBLE)", self.int(sc, d - 1)),
            _ => format!("({} - {})", self.num(sc, d - 1), self.int(sc, d - 1)),
        }
    }

    fn pred(&mut self, sc: &Scope, d: usize) -> String {
        let d = d.min(3);
        if d > 0 && self.chance(8) {
            return match self.r.below(3) {
                0 => format!(
                    "{} {} {}",
                    self.num(sc, d),
                    self.one(&["=", "<>", "<", ">", "<=", ">="]),
                    self.num(sc, d)
                ),
                1 => format!(
                    "{} {}IN ({}, NULL, {})",
                    self.int(sc, d),
                    self.one(&["", "NOT "]),
                    self.r.below(10),
                    self.r.below(60)
                ),
                _ => format!(
                    "{} {} {}",
                    self.int(sc, d),
                    self.one(&["<", ">=", "="]),
                    self.one(&["2.5", "10.0", "-0.5", "1e1"])
                ),
            };
        }
        match self.r.below(if d == 0 { 7 } else { 14 }) {
            0 => format!("{} {} {}", self.int(sc, d), self.one(&["=", "<>", "<", ">", "<=", ">="]), self.int(sc, d)),
            1 => format!("{} {} {}", self.text(sc, d), self.one(&["=", "<>", "<", ">="]), self.text(sc, d)),
            2 => format!("{} LIKE '{}'", self.text(sc, d), self.one(&["0x%", "%a", "_x%", "%"])),
            3 => format!("{} IS {}NULL", self.text(sc, d), self.one(&["", "NOT "])),
            4 => format!("{} IN ({}, {}, {})", self.int(sc, d), self.r.below(10), self.r.below(30), self.r.below(60)),
            5 => format!("{} BETWEEN {} AND {}", self.int(sc, d), self.r.below(20) as i64 - 5, self.r.below(80)),
            6 => format!("e.flag = '{}'", self.one(&["true", "false"])),
            7 => format!("NOT ({})", self.pred(sc, d - 1)),
            8 => format!("({} AND {})", self.pred(sc, d - 1), self.pred(sc, d - 1)),
            9 => format!("({} OR {})", self.pred(sc, d - 1), self.pred(sc, d - 1)),
            10 => format!("{} IS {}DISTINCT FROM {}", self.text(sc, d), self.one(&["", "NOT "]), self.text(sc, d)),
            11 if self.chance(3) => format!(
                "{}\"to\" {}IN (SELECT \"to\" FROM ev WHERE kind = '{}' AND log_index > {})",
                self.one(&["", "e."]),
                self.one(&["", "NOT "]),
                self.one(&["in", "out"]),
                self.r.below(3)
            ),
            11 if self.chance(2) => format!(
                "(SELECT count(*) FROM ev x WHERE x.\"from\" NOT IN (SELECT addr FROM lbl WHERE weight > {})) > {}",
                self.r.below(5) as i64 - 2,
                self.r.below(60)
            ),
            11 => format!("e.\"to\" IN (SELECT addr FROM lbl WHERE weight > {})", self.r.below(5) as i64 - 2),
            12 if self.chance(2) => format!("regexp_matches({}, '{}')", self.text(sc, d), self.one(&["^0x", "[0-9]{2}", "(?i)grt", "^$", "a|b"])),
            12 if self.chance(2) => format!("{}({}, '{}')", self.one(&["starts_with", "contains", "ends_with"]), self.text(sc, d), self.one(&["0x", "a", "GRT", ""])),
            12 if self.chance(2) => format!("{} {} TIMESTAMPTZ '2024-{:02}-01 00:00:00+00'", self.time(sc, d), self.one(&["<", ">=", "="]), 1 + self.r.below(12)),
            12 => format!("{} ILIKE '{}'", self.text(sc, d), self.one(&["%grt%", "STAKE%", "_x%"])),
            _ => format!(
                "{}EXISTS (SELECT 1 FROM lbl x WHERE x.addr = e.\"from\")",
                self.one(&["", "NOT "])
            ),
        }
    }

    fn any(&mut self, sc: &Scope, d: usize) -> String {
        match self.r.below(5) {
            0 => self.int(sc, d),
            1 if self.chance(2) => self.num(sc, d),
            1 => self.int(sc, d),
            2 => self.text(sc, d),
            3 => self.dec(sc, d),
            _ => self.pred(sc, d),
        }
    }

    fn from(&mut self, sc: &Scope) -> String {
        if sc.joined {
            let kind = self.one(&["JOIN", "LEFT JOIN"]);
            let on = self.one(&[
                "l.addr = e.\"to\"",
                "l.addr = e.\"from\"",
                "lower(l.addr) = lower(e.\"to\")",
            ]);
            format!("ev e {kind} lbl l ON {on}")
        } else {
            "ev e".into()
        }
    }

    /// A query and whether its row order is part of the answer.
    fn query(&mut self) -> (String, bool) {
        let sc = Scope {
            joined: self.chance(3),
        };
        let from = self.from(&sc);
        let filter = if self.chance(2) {
            format!(" WHERE {}", self.pred(&sc, 2))
        } else {
            String::new()
        };
        match self.r.below(17) {
            // Grouped over a derived table, keys and values computed inside.
            8 => {
                let key = if self.chance(2) { self.text(&sc, 1) } else { self.int(&sc, 1) };
                let v = self.int(&sc, 2);
                let having = if self.chance(2) { format!(" HAVING sum(v) {} {}", self.one(&[">", "<"]), self.r.below(200)) } else { String::new() };
                let order = if self.chance(2) { " ORDER BY ALL" } else { "" };
                (
                    format!("SELECT k, count(*) AS n, sum(v) AS s, max(v) - min(v) AS r FROM (SELECT {key} AS k, {v} AS v FROM {from}{filter}) d GROUP BY ALL{having}{order}"),
                    !order.is_empty(),
                )
            }
            // A window over grouped output, ordered by an aggregate and the unique key.
            9 => {
                let key = self.one(&["e.\"from\"", "e.kind", "e.block_number % 7", "lower(e.\"to\")"]);
                let agg = format!("sum({})", self.int(&sc, 1));
                let w = self.one(&["rank()", "dense_rank()", "row_number()", "sum(count(*))", "lag(count(*))"]);
                let dir = self.one(&["", " DESC"]);
                (
                    format!("SELECT {key} AS k, {agg} AS s, count(*) AS n, {w} OVER (ORDER BY {agg}{dir}, {key}) AS w FROM {from}{filter} GROUP BY 1"),
                    false,
                )
            }
            // A CTE joined to itself.
            10 => {
                let v = self.int(&sc, 1);
                (
                    format!(
                        "WITH s AS (SELECT e.\"from\" AS f, count(*) AS n, max({v}) AS m FROM {from}{filter} GROUP BY 1) SELECT a.f, a.n, b.f AS g, b.m FROM s a {} s b ON a.n {} b.n AND a.f IS DISTINCT FROM b.f",
                        self.one(&["JOIN", "LEFT JOIN"]),
                        self.one(&["=", "<", ">="])
                    ),
                    false,
                )
            }
            // QUALIFY and DISTINCT ON, keyed so that ties cannot choose.
            11 => {
                let part = self.one(&["e.kind", "e.\"from\"", "e.flag", "e.block_number % 3"]);
                if self.chance(2) {
                    let dir = self.one(&["", " DESC"]);
                    (
                        format!("SELECT e.block_number, e.log_index, {part} AS p FROM {from}{filter} QUALIFY row_number() OVER (PARTITION BY {part} ORDER BY e.block_number{dir}, e.log_index{dir}{}) <= {}",
                            if sc.joined { ", l.addr, l.name" } else { "" }, 1 + self.r.below(3)),
                        false,
                    )
                } else {
                    let v = self.int(&Scope { joined: false }, 1);
                    (
                        format!("SELECT DISTINCT ON ({part}) {part} AS p, {v} AS v FROM ev e{filter} ORDER BY {part}, e.block_number DESC, e.log_index DESC",
                            filter = if sc.joined { String::new() } else { filter.clone() }),
                        true,
                    )
                }
            }
            // Lists and JSON inside aggregates, over one relation so an ORDER BY inside cannot tie.
            12 if !sc.joined => {
                let key = self.text(&sc, 1);
                let a = self.one(&[
                    "array_length(list(e.amount ORDER BY e.block_number, e.log_index))",
                    "list(e.amount ORDER BY e.block_number, e.log_index)[1]",
                    "list_sort(list(e.log_index))[-1]",
                    "max(TRY(json_extract_string(e.doc, '$.a')))",
                    "count(DISTINCT TRY(e.doc ->> 'b'))",
                    "string_agg(DISTINCT e.kind, ',' ORDER BY e.kind)",
                    "min(len(e.memo))",
                ]);
                (format!("SELECT {key} AS k, {a} AS a, count(*) AS n FROM ev e{filter} GROUP BY 1"), false)
            }
            // FULL JOIN keeps the unmatched side, which an inner join drops.
            14 => {
                let on = self.one(&["l.addr = e.\"to\"", "l.addr = e.\"from\"", "lower(l.addr) = lower(e.\"to\")"]);
                (
                    format!(
                        "SELECT e.block_number, e.log_index, l.name AS n, l.weight AS w FROM ev e FULL JOIN lbl l ON {on}{filter}"
                    ),
                    false,
                )
            }
            // A literal row set joined on the log index.
            15 => (
                format!(
                    "SELECT e.block_number, e.log_index, v.n FROM ev e JOIN (VALUES (0), (1), (2), ({})) v(n) ON e.log_index = v.n",
                    self.r.below(4) as i64 - 1
                ),
                false,
            ),
            // unnest in the select list, which is where the views write it. `FROM unnest` is not
            // planned: the only table functions are range and generate_series.
            16 => (
                format!(
                    "SELECT e.block_number, e.log_index, unnest(string_split(COALESCE(e.memo, ''), '{}')) AS c FROM ev e WHERE e.block_number < {}",
                    self.one(&[",", ":", ""]),
                    4 + self.r.below(6)
                ),
                false,
            ),
            // The anti-join written as a LEFT JOIN, and IN over an ordered, limited subquery.
            12 | 13 => {
                if self.chance(2) {
                    let on = self.one(&["l.addr = e.\"to\"", "l.addr = e.\"from\"", "lower(l.addr) = lower(e.\"to\")"]);
                    (format!("SELECT e.block_number, e.log_index, {} AS v FROM ev e LEFT JOIN lbl l ON {on} WHERE l.addr IS NULL", self.int(&Scope { joined: false }, 1)), false)
                } else {
                    (
                        format!(
                            "SELECT e.block_number, e.log_index FROM ev e WHERE e.\"{}\" {}IN (SELECT addr FROM lbl ORDER BY weight {} NULLS LAST, addr LIMIT {})",
                            self.one(&["to", "from"]),
                            self.one(&["", "NOT "]),
                            self.one(&["ASC", "DESC"]),
                            1 + self.r.below(4)
                        ),
                        false,
                    )
                }
            }
            // Projection.
            0 | 1 => {
                let n = 1 + self.r.below(3);
                let items: Vec<String> = (0..n).map(|i| format!("{} AS c{i}", self.any(&sc, 3))).collect();
                let distinct = if self.chance(4) { "DISTINCT " } else { "" };
                let base = format!("SELECT {distinct}{} FROM {from}{filter}", items.join(", "));
                if self.chance(3) {
                    let order: Vec<String> = (1..=n)
                        .map(|i| format!("{i}{}{}", self.one(&["", " DESC"]), self.one(&["", " NULLS FIRST", " NULLS LAST"])))
                        .collect();
                    let offset = if self.chance(3) { format!(" OFFSET {}", self.r.below(10)) } else { String::new() };
                    (format!("{base} ORDER BY {} LIMIT {}{offset}", order.join(", "), 1 + self.r.below(20)), true)
                } else {
                    (base, false)
                }
            }
            // Grouped.
            2 | 3 => {
                let key = match self.r.below(3) {
                    0 => self.text(&sc, 1),
                    1 => self.int(&sc, 1),
                    _ => self.time(&sc, 2),
                };
                // arg_max over a key unique per row, and not over a join, which repeats rows:
                // among ties either engine may pick any.
                let tie_free = !sc.joined;
                let aggs = [
                    if tie_free { format!("arg_max({}, e.block_number * 10 + e.log_index)", self.text(&sc, 1)) } else { "count(*)".into() },
                    if tie_free { format!("arg_min({}, e.block_number * 10 + e.log_index)", self.int(&sc, 1)) } else { "count(*)".into() },
                    {
                        // The value itself breaks ties a join's repeated rows leave.
                        let t = self.text(&sc, 1);
                        // A literal as an ordering key is DuckDB's error; it cannot tie anyway.
                        let tiebreak = if t.starts_with('\'') { String::new() } else { format!(", {t}") };
                        format!("string_agg({t}, ',' ORDER BY e.block_number, e.log_index{tiebreak})")
                    },
                    format!("bool_and({})", self.pred(&sc, 1)),
                    format!("bool_or({})", self.pred(&sc, 1)),
                    format!("count(DISTINCT {})", self.int(&sc, 1)),
                    format!("min({})", self.time(&sc, 1)),
                    format!("max(strftime({}, '%Y-%m'))", self.time(&sc, 1)),
                    "count(*)".to_string(),
                    format!("count({})", self.text(&sc, 1)),
                    format!("sum({})", self.int(&sc, 2)),
                    format!("sum({})", self.dec(&sc, 2)),
                    format!("min({})", self.text(&sc, 1)),
                    format!("max({})", self.int(&sc, 2)),
                    format!("count(DISTINCT {})", self.text(&sc, 1)),
                    format!("avg({})", self.int(&sc, 1)),
                    format!("sum(CAST({} AS {}))", self.int(&sc, 1), self.one(&["INTEGER", "UBIGINT", "HUGEINT", "DOUBLE", "DECIMAL(12,2)"])),
                    format!("max(CAST({} AS VARCHAR))", self.int(&sc, 1)),
                    format!("avg({})", self.num(&sc, 1)),
                    format!("min({})", self.num(&sc, 2)),
                    format!("sum({}) FILTER (WHERE {})", self.int(&sc, 1), self.pred(&sc, 1)),
                    format!("count(*) FILTER (WHERE {})", self.pred(&sc, 1)),
                    format!("(sum({}) - min({}))", self.int(&sc, 1), self.int(&sc, 1)),
                    format!("(max({}) {} count(*))", self.int(&sc, 1), self.one(&["+", "*", "/", "//"])),
                ];
                let a = aggs[self.r.below(aggs.len())].clone();
                let b = aggs[self.r.below(aggs.len())].clone();
                let having = match self.r.below(5) {
                    0 => format!(" HAVING count(*) > {}", self.r.below(4)),
                    1 => {
                        let numeric = [
                            "count(*)".to_string(),
                            format!("sum({})", self.int(&sc, 1)),
                            format!("max({})", self.int(&sc, 1)),
                            format!("count(DISTINCT {})", self.text(&sc, 1)),
                            format!("avg({})", self.num(&sc, 1)),
                        ];
                        format!(" HAVING {} {} {}", numeric[self.r.below(numeric.len())], self.one(&[">", "<", "<>"]), self.r.below(50))
                    }
                    _ => String::new(),
                };
                if self.chance(4) {
                    let key2 = self.one(&["e.kind", "e.flag", "e.block_number % 3", "e.amount IS NULL"]);
                    (format!("SELECT {key} AS k, {key2} AS k2, {a} AS a, {b} AS b FROM {from}{filter} GROUP BY 1, 2{having}"), false)
                } else {
                    (format!("SELECT {key} AS k, {a} AS a, {b} AS b FROM {from}{filter} GROUP BY 1{having}"), false)
                }
            }
            // Derived table or CTE over a grouped or projected inner query.
            4 => {
                let inner = format!(
                    "SELECT e.\"from\" AS f, {} AS v, {} AS w FROM {from}{filter}",
                    self.int(&sc, 2),
                    self.text(&sc, 1)
                );
                if self.chance(2) {
                    (format!("SELECT f, sum(v) AS s, count(*) AS n FROM ({inner}) s GROUP BY f"), false)
                } else {
                    (format!("WITH s AS ({inner}) SELECT * FROM s WHERE v {} {}", self.one(&[">", "<", "="]), self.r.below(40)), false)
                }
            }
            // Windows, ordered by the unique key.
            5 => {
                let part = self.one(&["e.\"from\"", "e.kind", "e.flag", "e.block_number % 4", ""]);
                let sum_expr = format!("sum({})", self.int(&sc, 1));
                let f = self.one(&[
                    "row_number()",
                    "sum(e.amount)",
                    "count(*)",
                    "lag(e.amount)",
                    "lead(e.memo, 2)",
                    "first_value(e.value)",
                    "last_value(e.amount)",
                    "max(e.amount)",
                    "avg(e.amount)",
                    "ntile(3)",
                    sum_expr.as_str(),
                ]);
                let f = f.to_string();
                let frame = self.one(&[
                    "",
                    " ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
                    " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                    " ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
                ]);
                let frame = if matches!(f.as_str(), "row_number()" | "lag(e.amount)" | "lead(e.memo, 2)" | "ntile(3)") { "" } else { frame };
                // rank and dense_rank over a key with ties, which is where they differ.
                let (f, order) = if self.chance(4) {
                    (self.one(&["rank()", "dense_rank()", "percent_rank()"]).to_string(), "e.kind, e.block_number % 5".to_string())
                } else {
                    (f, "e.block_number, e.log_index".to_string())
                };
                (
                    format!(
                        "SELECT e.block_number, e.log_index, {f} OVER ({}ORDER BY {order}{frame}) AS w FROM {from}{filter}",
                        if part.is_empty() { String::new() } else { format!("PARTITION BY {part} ") }
                    ),
                    false,
                )
            }
            // Set operations.
            6 => {
                let op = self.one(&["UNION ALL", "UNION", "EXCEPT", "INTERSECT", "EXCEPT ALL", "INTERSECT ALL"]);
                let a = self.int(&sc, 2);
                let b = self.int(&sc, 2);
                let p = self.pred(&sc, 1);
                (format!("SELECT {a} AS x FROM {from}{filter} {op} SELECT {b} FROM {from} WHERE {p}"), false)
            }
            // A series joined to the events.
            7 if self.chance(2) => (
                format!(
                    "SELECT r.x, count(e.block_number) AS n FROM range({}, {}) r(x) LEFT JOIN ev e ON e.block_number = r.x GROUP BY 1",
                    self.r.below(10),
                    10 + self.r.below(40)
                ),
                false,
            ),
            // Correlated scalar subqueries in the select list.
            7 if self.chance(2) => {
                let corr = self.one(&[
                    "x.addr = e.\"to\"",
                    "lower(x.addr) = lower(e.\"from\")",
                    "x.weight = e.amount",
                    "x.weight > e.amount",
                    "x.addr = e.\"to\" AND x.weight > e.log_index",
                ]);
                let agg = self.one(&["count(*)", "max(x.name)", "sum(x.weight)", "min(x.weight)", "count(DISTINCT x.name)"]);
                let key = self.int(&sc, 1);
                (format!("SELECT e.block_number, e.log_index, {key} AS k, (SELECT {agg} FROM lbl x WHERE {corr}) AS s FROM {from}{filter}"), false)
            }
            // Scalar subquery and whole-table aggregates.
            _ => (
                format!(
                    "SELECT count(*) AS n, (SELECT count(*) FROM lbl WHERE weight > {}) AS m, sum({}) AS s FROM {from}{filter}",
                    self.r.below(5),
                    self.int(&sc, 2)
                ),
                false,
            ),
        }
    }
}

/// `sql` with each `TRY_CAST(... AS <64-bit or narrower integer>)` widened to HUGEINT: DuckDB's
/// answer had those casts not dropped what did not fit, which is what Burrmill's exact sums give.
fn widened(sql: &str) -> Option<String> {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    let mut changed = false;
    while let Some(i) = rest.find("TRY_CAST(") {
        out.push_str(&rest[..i + 9]);
        rest = &rest[i + 9..];
        let mut depth = 0usize;
        let mut end = None;
        for (j, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' if depth == 0 => {
                    end = Some(j);
                    break;
                }
                ')' => depth -= 1,
                _ => {}
            }
        }
        let j = end?;
        let inner = &rest[..j];
        let widened = ["BIGINT", "INTEGER", "SMALLINT", "UBIGINT"]
            .iter()
            .find_map(|t| inner.strip_suffix(&format!(" AS {t}")))
            .map(|body| format!("{body} AS HUGEINT"));
        changed |= widened.is_some();
        out.push_str(widened.as_deref().unwrap_or(inner));
        rest = &rest[j..];
    }
    out.push_str(rest);
    changed.then_some(out)
}

/// The same rows but for floats within a few units in the last place: a sum of DOUBLEs taken in
/// another order, which each engine's partitioning decides and neither answer gets wrong.
fn float_close(w: &[String], g: &[String], ordered: bool) -> bool {
    fn close(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Number(x), Value::Number(y)) if x.is_f64() || y.is_f64() => {
                let (x, y) = (
                    x.as_f64().unwrap_or(f64::NAN),
                    y.as_f64().unwrap_or(f64::NAN),
                );
                x == y || (x - y).abs() <= 1e-12 * x.abs().max(y.abs())
            }
            (Value::Object(x), Value::Object(y)) => {
                x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|u| close(v, u)))
            }
            (Value::Array(x), Value::Array(y)) => {
                x.len() == y.len() && x.iter().zip(y).all(|(a, b)| close(a, b))
            }
            (a, b) => a == b,
        }
    }
    let parse = |v: &[String]| {
        v.iter()
            .map(|r| serde_json::from_str::<Value>(r))
            .collect::<Result<Vec<_>, _>>()
    };
    let (Ok(w), Ok(g)) = (parse(w), parse(g)) else {
        return false;
    };
    if w.len() != g.len() {
        return false;
    }
    if ordered {
        return w.iter().zip(&g).all(|(a, b)| close(a, b));
    }
    let mut left: Vec<&Value> = w.iter().collect();
    g.iter()
        .all(|r| match left.iter().position(|x| close(x, r)) {
            Some(i) => {
                left.swap_remove(i);
                true
            }
            None => false,
        })
}

fn duck_rows(conn: &duckdb::Connection, sql: &str) -> Result<Vec<String>, String> {
    match crate::encode_parity::nuthatch_rows(conn, sql) {
        Ok(Value::Array(rows)) => Ok(rows.iter().map(|r| r.to_string()).collect()),
        Ok(v) => Ok(vec![v.to_string()]),
        Err(e) => Err(e.to_string().lines().next().unwrap_or("").to_string()),
    }
}

fn burrmill_rows(e: &burrmill::Engine, sql: &str) -> Result<Vec<String>, String> {
    let batches = e.sql(sql).map_err(|e| e.to_string().replace('\n', " | "))?;
    let mut out = Vec::new();
    for b in &batches {
        for r in burrmill::df::encode::rows(b).map_err(|e| e.to_string())? {
            out.push(r.to_string());
        }
    }
    Ok(out)
}

pub fn run() -> anyhow::Result<()> {
    let seed = crate::env_usize("SEED", 1) as u64;
    let cases = crate::env_usize("CASES", 2000);
    let print = crate::env_flag("PRINT");
    let tmp = tempfile::tempdir()?;
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs)?;
    fixture(&segs, ONE_FILE)?;
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(&format!(
        "CREATE VIEW ev AS SELECT * FROM read_parquet('{0}/ev-*.parquet');
         CREATE VIEW lbl AS SELECT * FROM read_parquet('{0}/lbl-*.parquet');",
        segs.display()
    ))?;
    let s2 = segs.clone();
    let engine = std::thread::spawn(move || burrmill::Engine::open_segments(&s2))
        .join()
        .expect("open")?;
    let engine = Arc::new(engine);

    // `SQL=<query>` runs that one query against the fixture and prints both answers.
    if let Ok(q) = std::env::var("SQL") {
        println!("duckdb   {:?}", duck_rows(&duck, &q));
        let e2 = Arc::clone(&engine);
        println!(
            "burrmill {:?}",
            std::thread::spawn(move || burrmill_rows(&e2, &q))
                .join()
                .expect("engine thread")
        );
        std::thread::spawn(move || drop(engine))
            .join()
            .expect("drop engine");
        return Ok(());
    }
    let (
        mut same,
        mut both,
        mut stricter,
        mut looser,
        mut differ,
        mut designed,
        mut overflow_order,
        mut designed_exact,
    ) = (0, 0, 0, 0, 0, 0, 0, 0);
    let mut float_order = 0;
    let mut stricter_why: std::collections::BTreeMap<String, usize> = Default::default();
    for i in 0..cases {
        let case_seed = seed + i as u64;
        let mut r = Rng(case_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let (sql, ordered) = Gen { r: &mut r }.query();
        let want = duck_rows(&duck, &sql);
        let e2 = Arc::clone(&engine);
        let q = sql.clone();
        let got = std::thread::spawn(move || burrmill_rows(&e2, &q))
            .join()
            .expect("engine thread");
        let tag = match (&want, &got) {
            (Ok(w), Ok(g)) => {
                let (mut w, mut g) = (w.clone(), g.clone());
                if !ordered {
                    w.sort();
                    g.sort();
                }
                let exact = || {
                    let alt = widened(&sql).and_then(|q| duck_rows(&duck, &q).ok());
                    alt.is_some_and(|mut a| {
                        if !ordered {
                            a.sort();
                        }
                        a == g
                    })
                };
                if w == g {
                    same += 1;
                    "SAME"
                } else if exact() {
                    designed_exact += 1;
                    "DESIGNED-EXACT"
                } else if float_close(&w, &g, ordered) {
                    float_order += 1;
                    "FLOAT-ORDER"
                } else {
                    differ += 1;
                    "DIFF"
                }
            }
            (Err(_), Err(_)) => {
                both += 1;
                "BOTH-REFUSE"
            }
            // One engine overflowed, or failed an out-of-range cast, where the other, having
            // rewritten or skipped the arithmetic (DataFusion unwraps `CAST(x AS UBIGINT) > 20` to
            // `x > 20`), did not: which errors surface is each optimiser's, and neither is wrong.
            (Ok(_), Err(e)) | (Err(e), Ok(_))
                if (e.contains("verflow")
                    || e.contains("out of range for the destination type")
                    || e.contains("Can't cast value")
                    || e.contains("Could not cast value")
                    || e.contains("too large to store"))
                    && !e.contains("refusing plan") =>
            {
                overflow_order += 1;
                "OVERFLOW-ORDER"
            }
            // The checked rule's refusals of a sum or bound that DuckDB would compute by dropping
            // values that did not fit: the 6.3 design, not a gap.
            (Ok(_), Err(e)) if e.contains("TRY_CAST value") => {
                designed += 1;
                "DESIGNED"
            }
            (Ok(_), Err(e)) => {
                stricter += 1;
                let key: String = e.chars().take(90).collect();
                *stricter_why.entry(key).or_default() += 1;
                "STRICTER"
            }
            // Nuthatch's DuckDB has no ICU. It cannot add an INTERVAL to a TIMESTAMPTZ, nor cast
            // one to DATE; Burrmill can, and that is the difference the build makes.
            (Err(e), Ok(_))
                if e.contains("(TIMESTAMP WITH TIME ZONE, INTERVAL)")
                    || e.contains("TIMESTAMP WITH TIME ZONE -> DATE") =>
            {
                designed += 1;
                "DESIGNED"
            }
            (Err(_), Ok(_)) => {
                looser += 1;
                "LOOSER"
            }
        };
        if print || matches!(tag, "DIFF" | "LOOSER") {
            println!("{tag} seed={case_seed}  {sql}");
            let clip = |r: &Result<Vec<String>, String>| -> String {
                match r {
                    Ok(v) => v.join(",").chars().take(400).collect(),
                    Err(e) => format!("ERROR {e}").chars().take(400).collect(),
                }
            };
            if let ("DIFF", Ok(w), Ok(g)) = (tag, &want, &got) {
                // The rows each side has that the other lacks, as multisets.
                let mut left: Vec<&String> = w.iter().collect();
                let mut right: Vec<&String> = Vec::new();
                for r in g {
                    match left.iter().position(|x| *x == r) {
                        Some(i) => {
                            left.swap_remove(i);
                        }
                        None => right.push(r),
                    }
                }
                let show = |v: &[&String]| {
                    v.iter()
                        .take(4)
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                };
                println!(
                    "    duckdb only   ({}) {}",
                    left.len(),
                    show(&left).chars().take(400).collect::<String>()
                );
                println!(
                    "    burrmill only ({}) {}",
                    right.len(),
                    show(&right).chars().take(400).collect::<String>()
                );
            } else if tag != "SAME" {
                println!("    duckdb   {}", clip(&want));
                println!("    burrmill {}", clip(&got));
            }
        }
    }
    println!("STRICTER reasons:");
    let mut why: Vec<_> = stricter_why.into_iter().collect();
    why.sort_by_key(|w| std::cmp::Reverse(w.1));
    for (k, n) in why.iter().take(12) {
        println!("  {n:>5}  {k}");
    }
    println!(
        "FUZZ\tseed={seed}\tcases={cases}\tsame={same}\tboth_refuse={both}\tdesigned_refusal={designed}\tdesigned_exact={designed_exact}\toverflow_order={overflow_order}\tfloat_order={float_order}\tstricter={stricter}\tlooser={looser}\tdiffer={differ}"
    );
    std::thread::spawn(move || drop(engine))
        .join()
        .expect("drop engine");
    Ok(())
}

/// `determinism`: generated SQL in deterministic mode at 1, 2, 8 and 32 threads over three
/// layouts of the same rows, each answer's Arrow IPC compared byte for byte (#340). A statement
/// refused anywhere must be refused everywhere. `SEED`, `CASES` (10,000) and `PRINT` as `fuzz`.
pub fn determinism() -> anyhow::Result<()> {
    let seed = crate::env_usize("SEED", 1) as u64;
    let cases = crate::env_usize("CASES", 10_000);
    let print = crate::env_flag("PRINT");
    // `DIR=<path>` keeps the fixture there, to rerun one statement by hand.
    let tmp = tempfile::tempdir()?;
    let root = std::env::var("DIR").map_or_else(|_| tmp.path().to_path_buf(), Into::into);
    let layouts = [
        ONE_FILE,
        Layout {
            segments: 3,
            group: 50,
        },
        Layout {
            segments: 11,
            group: 7,
        },
    ];
    let mut engines = Vec::new();
    for (i, layout) in layouts.iter().enumerate() {
        let dir = root.join(format!("layout{i}"));
        std::fs::create_dir_all(&dir)?;
        fixture(&dir, *layout)?;
        for threads in [1, 2, 8, 32] {
            let e = burrmill::Engine::builder()
                .segments(&dir)
                .budget(burrmill::Budget {
                    memory_bytes: 1 << 30,
                    threads,
                    spill: None,
                })
                .open()?;
            e.set_deterministic(true);
            engines.push((
                format!("{} segments, {threads} threads", layout.segments),
                e,
            ));
        }
    }
    let (mut same, mut refused, mut differ) = (0usize, 0usize, 0usize);
    let mut why: std::collections::BTreeMap<String, usize> = Default::default();
    for i in 0..cases {
        let case_seed = seed + i as u64;
        let mut r = Rng(case_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let sql = determinism_query(&mut r);
        let answers: Vec<Result<Vec<u8>, String>> = engines
            .iter()
            .map(|(_, e)| e.sql_ipc(&sql).map_err(|e| e.to_string()))
            .collect();
        let tag = match &answers[0] {
            Ok(want) if answers.iter().all(|a| a.as_ref().is_ok_and(|g| g == want)) => {
                same += 1;
                "SAME"
            }
            Err(e) if answers.iter().all(|a| a.is_err()) => {
                refused += 1;
                *why.entry(
                    e.rsplit("planning: ")
                        .next()
                        .unwrap_or(e)
                        .chars()
                        .take(110)
                        .collect(),
                )
                .or_default() += 1;
                "REFUSED"
            }
            _ => {
                differ += 1;
                let first = answers.iter().position(|a| a != &answers[0]).unwrap_or(0);
                println!(
                    "DIFF seed={case_seed}: {} and {} disagree: {sql}",
                    engines[0].0, engines[first].0
                );
                "DIFF"
            }
        };
        if print {
            println!("{tag} {sql}");
        }
    }
    println!(
        "{cases} cases over {} engines: {same} byte-identical, {refused} refused everywhere, \
         {differ} differ",
        engines.len()
    );
    for (e, n) in why.iter().rev().take(12) {
        println!("  refused {n:>5}  {e}");
    }
    std::thread::spawn(move || drop(engines))
        .join()
        .expect("drop engines");
    anyhow::ensure!(differ == 0, "{differ} statements answered differently");
    Ok(())
}

/// The fuzz grammar's statements with their ORDER BY left as written, and some wrapped in a LIMIT
/// with none, or over float aggregates, which is where the order of rows would show.
fn determinism_query(r: &mut Rng) -> String {
    let (sql, _) = Gen { r }.query();
    match r.below(6) {
        0 => format!("SELECT * FROM ({sql}) d LIMIT {}", 1 + r.below(20)),
        1 => {
            let f = [
                "sum",
                "avg",
                "min",
                "max",
                "stddev",
                "var_pop",
                "array_agg",
                "first_value",
            ][r.below(8)];
            let x = ["CAST(e.amount AS DOUBLE) / 7", "e.amount * 0.1", "e.amount"][r.below(3)];
            format!("SELECT e.kind, {f}({x}) AS a, count(*) AS n FROM ev e GROUP BY e.kind")
        }
        2 => format!(
            "SELECT e.\"from\", e.amount, row_number() OVER (PARTITION BY e.kind ORDER BY \
             e.amount) AS n FROM ev e LIMIT {}",
            5 + r.below(40)
        ),
        _ => sql,
    }
}

/// `asof`: generated `ASOF [LEFT] JOIN` statements over random trades and prices, on DuckDB and on
/// `Engine`, rows compared as multisets (#345). A price's `ts` is unique, so which right row is
/// nearest never rests on a tie neither engine defines. `SEED`, `CASES` (2,000) and `PRINT`.
pub fn asof() -> anyhow::Result<()> {
    let seed = crate::env_usize("SEED", 1) as u64;
    let cases = crate::env_usize("CASES", 2000);
    let print = crate::env_flag("PRINT");
    let duck = duckdb::Connection::open_in_memory()?;
    let engine = std::thread::spawn(|| burrmill::Engine::builder().open())
        .join()
        .expect("open")?;
    let engine = Arc::new(engine);
    let (mut same, mut both, mut differ, mut one) = (0, 0, 0, 0);
    for i in 0..cases {
        let case_seed = seed + i as u64;
        let mut r = Rng(case_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let sql = asof_case(&mut r);
        let want = duck_rows(&duck, &sql);
        let e2 = Arc::clone(&engine);
        let q = sql.clone();
        let got = std::thread::spawn(move || burrmill_rows(&e2, &q))
            .join()
            .expect("engine thread");
        let tag = match (want, got) {
            (Ok(mut w), Ok(mut g)) => {
                w.sort();
                g.sort();
                if w == g {
                    same += 1;
                    "SAME"
                } else {
                    differ += 1;
                    println!("DIFF seed={case_seed}: {sql}\n  duckdb   {w:?}\n  burrmill {g:?}");
                    "DIFF"
                }
            }
            (Err(_), Err(_)) => {
                both += 1;
                "BOTH-REFUSE"
            }
            (w, g) => {
                one += 1;
                println!("ONE-REFUSES seed={case_seed}: {sql}\n  duckdb   {w:?}\n  burrmill {g:?}");
                "ONE-REFUSES"
            }
        };
        if print {
            println!("{tag} {sql}");
        }
    }
    println!("{cases} cases: {same} same, {both} both refuse, {one} one refuses, {differ} differ");
    std::thread::spawn(move || drop(engine))
        .join()
        .expect("drop engine");
    anyhow::ensure!(
        differ == 0 && one == 0,
        "{differ} differ, {one} refused by one engine"
    );
    Ok(())
}

fn asof_case(r: &mut Rng) -> String {
    let syms = ["'a'", "'b'", "'c'", "CAST(NULL AS VARCHAR)"];
    let int = |r: &mut Rng, n: usize| {
        if r.below(9) == 0 {
            "CAST(NULL AS INTEGER)".to_string()
        } else {
            r.below(n).to_string()
        }
    };
    let trades: Vec<String> = (0..1 + r.below(25))
        .map(|i| format!("({}, {}, {i})", syms[r.below(4)], int(r, 40)))
        .collect();
    let mut stamps: Vec<usize> = (0..40).collect();
    for i in (1..stamps.len()).rev() {
        stamps.swap(i, r.below(i + 1));
    }
    let prices: Vec<String> = stamps[..1 + r.below(25)]
        .iter()
        .enumerate()
        .map(|(i, ts)| {
            let ts = if r.below(12) == 0 {
                "CAST(NULL AS INTEGER)".to_string()
            } else {
                ts.to_string()
            };
            format!("({}, {ts}, {})", syms[r.below(4)], 100 + i)
        })
        .collect();
    let with = format!(
        "WITH t(sym, ts, q) AS (VALUES {}), p(sym, ts, px) AS (VALUES {}) ",
        trades.join(", "),
        prices.join(", ")
    );
    let kind = ["ASOF JOIN", "ASOF LEFT JOIN"][r.below(2)];
    // Each side as a name, an alias or a derived table, whose spans the parser starts differently.
    let side = |r: &mut Rng, table: &str, alias: &str| match r.below(3) {
        0 => (table.to_string(), table.to_string()),
        1 => (format!("{table} AS {alias}"), alias.to_string()),
        _ => (
            format!("(SELECT * FROM {table}) {alias}"),
            alias.to_string(),
        ),
    };
    let (lrel, l) = side(r, "t", "tt");
    let (rrel, rr) = side(r, "p", "pp");
    let cond = match r.below(5) {
        0 => ["USING (sym, ts)", "USING (ts)"][r.below(2)].to_string(),
        _ => {
            let op = [">=", ">", "<=", "<"][r.below(4)];
            let flip = |op: &str| {
                match op {
                    ">=" => "<=",
                    ">" => "<",
                    "<=" => ">=",
                    _ => ">",
                }
                .to_string()
            };
            let ineq = if r.below(2) == 0 {
                format!("{l}.ts {op} {rr}.ts")
            } else {
                format!("{rr}.ts {} {l}.ts", flip(op))
            };
            match r.below(3) {
                0 => format!("ON {ineq}"),
                1 => format!("ON {l}.sym = {rr}.sym AND {ineq}"),
                _ => format!("ON {ineq} AND {rr}.sym = {l}.sym"),
            }
        }
    };
    let filter = if r.below(3) == 0 {
        format!(" WHERE {l}.q > {}", r.below(10))
    } else {
        String::new()
    };
    let body = format!(
        "{l}.sym, {l}.ts, {l}.q, {rr}.ts AS pts, {rr}.px FROM {lrel} {kind} {rrel} {cond}{filter}"
    );
    match r.below(4) {
        0 => format!(
            "{with}SELECT count(*) AS n, sum(px) AS s, count(pts) AS m FROM (SELECT {body}) x"
        ),
        _ => format!("{with}SELECT {body}"),
    }
}
