//! DataFusion's functions with a sort property read from their argument's range, as they are but
//! for the error a nested call gets (#367).
//!
//! A function's range is unbounded and typed NULL unless it says otherwise, and `abs`, `sqrt`, `ln`
//! and thirteen others refuse a NULL-typed range of their argument. One error
//! anywhere in a sort key makes DataFusion forget what it knows of the whole key, the ordering it
//! just sorted by included: `abs(abs(x))` sorted per partition did not satisfy its own merge, and
//! the sanity check refused the statement at two threads and above. Here such an error is "no
//! order known", and the key is still matched against the orderings the plan has.

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{ExprSchema, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping,
};

/// `f` as it is where its sort properties never fail, else wrapped so they cannot.
pub(super) fn ordering_safe(f: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
    let unknown = vec![ExprProperties::new_unknown(); 3];
    let ranges: Vec<&Interval> = unknown.iter().map(|p| &p.range).collect();
    let fails = f.output_ordering(&unknown).is_err()
        || f.preserves_lex_ordering(&unknown).is_err()
        || f.strictly_order_preserving(&unknown).is_err()
        || f.evaluate_bounds(&ranges).is_err();
    match fails {
        true => Arc::new(ScalarUDF::from(OrderingSafe(f.as_ref().clone()))),
        false => f,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct OrderingSafe(ScalarUDF);

#[warn(clippy::missing_trait_methods)]
impl ScalarUDFImpl for OrderingSafe {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        #[expect(deprecated)]
        self.0.inner().display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.0.inner().schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.0.inner().return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.0.inner().return_field_from_args(args)
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[expect(deprecated)]
        self.0.inner().is_nullable(args, schema)
    }

    fn is_strict(&self) -> bool {
        self.0.inner().is_strict()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.0.inner().invoke_with_args(args)
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.0
            .inner()
            .with_updated_config(config)
            .map(|f| ScalarUDF::from(OrderingSafe(f)))
    }

    fn aliases(&self) -> &[String] {
        self.0.aliases()
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.0.inner().simplify(args, info)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        self.0.inner().preimage(args, lit_expr, info)
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.0.inner().conditional_arguments(args)
    }

    fn short_circuits(&self) -> bool {
        self.0.inner().short_circuits()
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        self.0
            .inner()
            .evaluate_bounds(input)
            .or_else(|_| Interval::make_unbounded(&DataType::Null))
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.0.inner().propagate_constraints(interval, inputs)
    }

    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.0.inner().struct_field_mapping(literal_args)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        Ok(self
            .0
            .inner()
            .output_ordering(inputs)
            .unwrap_or(SortProperties::Unordered))
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        Ok(self
            .0
            .inner()
            .preserves_lex_ordering(inputs)
            .unwrap_or(false))
    }

    fn strictly_order_preserving(&self, inputs: &[ExprProperties]) -> Result<bool> {
        Ok(self
            .0
            .inner()
            .strictly_order_preserving(inputs)
            .unwrap_or(false))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.0.inner().coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.0.inner().documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.0.inner().placement(args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::{
        EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr, ScalarFunctionExpr,
    };

    use super::ordering_safe;

    /// Whether data sorted by `abs(abs(x))` is known to be sorted by it.
    fn sorted_by_itself(abs: Arc<datafusion_expr::ScalarUDF>) -> bool {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::UInt64, false)]));
        let config = Arc::new(ConfigOptions::default());
        let call = |arg: Arc<dyn PhysicalExpr>| -> Arc<dyn PhysicalExpr> {
            Arc::new(
                ScalarFunctionExpr::try_new(
                    Arc::clone(&abs),
                    vec![arg],
                    &schema,
                    Arc::clone(&config),
                )
                .unwrap(),
            )
        };
        let key = call(call(col("x", &schema).unwrap()));
        let order = LexOrdering::new([PhysicalSortExpr::new(key, SortOptions::default())]).unwrap();
        let mut eq = EquivalenceProperties::new(Arc::clone(&schema));
        eq.add_ordering(order.clone());
        eq.ordering_satisfy(order).unwrap()
    }

    #[test]
    fn a_nested_abs_satisfies_the_order_it_was_sorted_by() {
        assert!(!sorted_by_itself(datafusion_functions::math::abs()));
        assert!(sorted_by_itself(ordering_safe(
            datafusion_functions::math::abs()
        )));
    }
}
