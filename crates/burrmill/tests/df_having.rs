//! A HAVING with no aggregate on either side answers as DuckDB does, and a panic below the engine
//! reaches its caller as an error rather than unwinding into it.

use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use burrmill::{BurrmillError, Engine};

const T: &str = "(VALUES ('x'), ('y'), ('x')) AS t(a)";

fn rows(engine: &Engine, sql: &str) -> Vec<String> {
    let mut out: Vec<String> = engine
        .sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

fn grouped_having(having: &str) -> Vec<String> {
    let engine = Engine::open_empty().unwrap();
    rows(
        &engine,
        &format!("SELECT a FROM {T} GROUP BY a HAVING {having}"),
    )
}

#[test]
fn having_a_function_of_the_key() {
    assert_eq!(grouped_having("upper(a) = 'X'"), ["x"]);
}

#[test]
fn having_a_comparison_of_literals() {
    assert_eq!(grouped_having("'x' > ''"), ["x", "y"]);
}

#[test]
fn having_a_volatile_function() {
    assert_eq!(grouped_having("random() >= 0"), ["x", "y"]);
}

#[test]
fn having_a_function_of_a_literal() {
    assert_eq!(grouped_having("upper('x') = 'X'"), ["x", "y"]);
}

#[test]
fn having_with_an_aggregate_still_answers() {
    assert_eq!(grouped_having("true"), ["x", "y"]);
    assert_eq!(grouped_having("count(*) > 1"), ["x"]);
    let engine = Engine::open_empty().unwrap();
    let sql = format!("SELECT a, count(*) FROM {T} GROUP BY a HAVING upper(a) = 'X'");
    assert_eq!(rows(&engine, &sql), ["x"]);
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PlansBadly(datafusion_expr::Signature);

impl datafusion_expr::ScalarUDFImpl for PlansBadly {
    fn name(&self) -> &str {
        "plans_badly"
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        &self.0
    }
    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        panic!("plans_badly while planning");
    }
    fn invoke_with_args(
        &self,
        _: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        unreachable!()
    }
}

#[test]
fn a_panic_while_planning_is_an_internal_error() {
    let mut engine = Engine::open_empty().unwrap();
    let sig = datafusion_expr::Signature::exact(
        vec![DataType::Utf8],
        datafusion_expr::Volatility::Immutable,
    );
    engine.register_scalar_udf(Arc::new(datafusion_expr::ScalarUDF::from(PlansBadly(sig))));
    let r = engine.sql(&format!("SELECT plans_badly(a) FROM {T}"));
    assert!(
        matches!(&r, Err(BurrmillError::Internal(m)) if m.contains("plans_badly while planning")),
        "{r:?}"
    );
    assert_eq!(
        rows(&engine, &format!("SELECT a FROM {T} WHERE a = 'y'")),
        ["y"]
    );
}

#[test]
fn a_panic_while_running_is_an_internal_error() {
    let mut engine = Engine::open_empty().unwrap();
    engine.register_text_function(
        "runs_badly",
        1,
        Arc::new(|_| panic!("runs_badly while running")),
    );
    let r = engine.sql(&format!("SELECT runs_badly(a) FROM {T}"));
    assert!(
        matches!(&r, Err(BurrmillError::Internal(m)) if m.contains("runs_badly while running")),
        "{r:?}"
    );
    assert_eq!(
        rows(&engine, &format!("SELECT a FROM {T} WHERE a = 'y'")),
        ["y"]
    );
}
