//! `array_agg(x ORDER BY k)` / `list(x ORDER BY k)` as a groups accumulator.
//!
//! DataFusion 55 has none for the ordered form: every group gets its own row accumulator holding a
//! `Vec<ScalarValue>`, and each struct value there is a one-row slice whose `size()` counts the
//! whole array behind it. On `lodestar_delegator_stakes` (484k groups) that was one 6 GB
//! reservation in a 2.5 GB process, refused under any budget. Here the values stay in the batches
//! they arrived in, each kept row is a `(group, batch, row)` entry, the keys are row-encoded, and a
//! group is sorted once when it is emitted. State is the built-in's, so either side can merge it.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::interleave;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::Result;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct OrderedArrayAgg {
    inner: Arc<AggregateUDF>,
}

impl OrderedArrayAgg {
    pub fn udaf(inner: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self { inner }))
    }
}

impl AggregateUDFImpl for OrderedArrayAgg {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }
    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        self.inner.inner().order_sensitivity()
    }
    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        // The built-in answers with a bare `ArrayAgg`, which would drop this wrapper from the plan.
        let inner = Arc::clone(self.inner.inner()).with_beneficial_ordering(beneficial_ordering)?;
        Ok(inner.map(|i| {
            Arc::new(Self {
                inner: Arc::new(AggregateUDF::new_from_shared_impl(i)),
            }) as _
        }))
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(args)
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::NotSupported
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if args.order_bys.is_empty() {
            return self.inner.create_groups_accumulator(args);
        }
        let value_type = args.expr_fields[0].data_type().clone();
        let mut keys = Vec::with_capacity(args.order_bys.len());
        let mut sort = Vec::with_capacity(args.order_bys.len());
        for o in args.order_bys {
            let t = o.expr.data_type(args.schema)?;
            keys.push(Arc::new(Field::new(o.expr.to_string(), t.clone(), true)));
            sort.push(SortField::new_with_options(t, o.options));
        }
        Ok(Box::new(Ordered {
            item: Arc::new(Field::new_list_field(value_type, true)),
            keys: Fields::from(keys),
            converter: RowConverter::new(sort)?,
            ignore_nulls: args.ignore_nulls && args.expr_fields[0].is_nullable(),
            values: Vec::new(),
            rows: Vec::new(),
            entries: Vec::new(),
            held: 0,
            groups: 0,
        }))
    }
    fn supports_null_handling_clause(&self) -> bool {
        self.inner.inner().supports_null_handling_clause()
    }
}

/// `string_agg(x, sep ORDER BY k)`. The built-in sorts what it keeps and still has DataFusion sort
/// its input, which every other aggregate of the statement then reads: an unordered `list(x)`
/// beside it came out sorted, where DuckDB keeps source order. Asking for no input order, it sorts
/// alone.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SelfSortingStringAgg {
    inner: Arc<AggregateUDF>,
    limits: Arc<super::bound::Limits>,
}

impl SelfSortingStringAgg {
    pub fn udaf(inner: Arc<AggregateUDF>, limits: &Arc<super::bound::Limits>) -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self {
            inner,
            limits: Arc::clone(limits),
        }))
    }
}

impl AggregateUDFImpl for SelfSortingStringAgg {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }
    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        datafusion_expr::utils::AggregateOrderSensitivity::Beneficial
    }
    fn with_beneficial_ordering(
        self: Arc<Self>,
        _beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(self))
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let delimiter = super::bound::delimiter_len(&args);
        Ok(Box::new(super::bound::BoundedAgg::new(
            self.inner.accumulator(args)?,
            delimiter,
            &self.limits,
        )))
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::NotSupported
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let delimiter = super::bound::delimiter_len(&args);
        Ok(Box::new(super::bound::BoundedGroups::new(
            self.inner.create_groups_accumulator(args)?,
            delimiter,
            &self.limits,
        )))
    }
}

struct Ordered {
    item: FieldRef,
    keys: Fields,
    converter: RowConverter,
    ignore_nulls: bool,
    values: Vec<ArrayRef>,
    rows: Vec<Rows>,
    /// `(group, batch, row)` for every value kept, in arrival order.
    entries: Vec<(u32, u32, u32)>,
    /// Bytes of `values` and `rows`, summed as they are pushed.
    held: usize,
    groups: usize,
}

impl Ordered {
    fn push(&mut self, values: ArrayRef, keys: &[ArrayRef]) -> Result<u32> {
        let rows = self.converter.convert_columns(keys)?;
        self.held += values.get_array_memory_size() + rows.size();
        self.values.push(values);
        self.rows.push(rows);
        Ok((self.values.len() - 1) as u32)
    }

    /// The entries of the groups `emit_to` names, sorted by group then key, arrival breaking ties;
    /// the groups left behind are renumbered from zero.
    fn take(&mut self, emit_to: EmitTo) -> (usize, Vec<(u32, u32, u32)>) {
        let n = match emit_to {
            EmitTo::All => self.groups,
            EmitTo::First(n) => n,
        };
        let (mut taken, kept): (Vec<_>, Vec<_>) =
            self.entries.iter().partition(|e| (e.0 as usize) < n);
        self.entries = kept
            .into_iter()
            .map(|(g, b, r)| (g - n as u32, b, r))
            .collect();
        self.groups -= n;
        let rows = &self.rows;
        taken.sort_by(|a, b| {
            a.0.cmp(&b.0).then_with(|| {
                rows[a.1 as usize]
                    .row(a.2 as usize)
                    .cmp(&rows[b.1 as usize].row(b.2 as usize))
            })
        });
        (n, taken)
    }

    /// Once nothing points into the batches, they go.
    fn release(&mut self) {
        if self.entries.is_empty() {
            self.values.clear();
            self.rows.clear();
            self.held = 0;
        }
    }

    /// One list per group, NULL where a group kept nothing, as the built-in answers.
    fn lists(
        &self,
        n: usize,
        taken: &[(u32, u32, u32)],
    ) -> Result<(OffsetBuffer<i32>, Option<NullBuffer>)> {
        let mut offsets = Vec::with_capacity(n + 1);
        let mut valid = Vec::with_capacity(n);
        offsets.push(0i32);
        let mut i = 0;
        for g in 0..n as u32 {
            let start = i;
            while i < taken.len() && taken[i].0 == g {
                i += 1;
            }
            offsets.push(i as i32);
            valid.push(i > start);
        }
        Ok((
            OffsetBuffer::new(offsets.into()),
            Some(NullBuffer::from(valid)),
        ))
    }

    fn gather(&self, taken: &[(u32, u32, u32)]) -> Result<ArrayRef> {
        let at: Vec<(usize, usize)> = taken.iter().map(|e| (e.1 as usize, e.2 as usize)).collect();
        if at.is_empty() {
            return Ok(arrow::array::new_empty_array(self.item.data_type()));
        }
        let parts: Vec<&dyn Array> = self.values.iter().map(|a| a.as_ref()).collect();
        Ok(interleave(&parts, &at)?)
    }

    fn gather_keys(&self, taken: &[(u32, u32, u32)]) -> Result<ArrayRef> {
        let columns = if taken.is_empty() {
            self.keys
                .iter()
                .map(|f| arrow::array::new_empty_array(f.data_type()))
                .collect()
        } else {
            let rows = taken
                .iter()
                .map(|e| self.rows[e.1 as usize].row(e.2 as usize));
            self.converter.convert_rows(rows)?
        };
        Ok(Arc::new(StructArray::try_new(
            self.keys.clone(),
            columns,
            None,
        )?))
    }

    fn keep(&self, values: &dyn Array, i: usize, filter: Option<&BooleanArray>) -> bool {
        filter.is_none_or(|f| f.is_valid(i) && f.value(i))
            && !(self.ignore_nulls && values.is_null(i))
    }
}

impl GroupsAccumulator for Ordered {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.groups = self.groups.max(total_num_groups);
        let b = self.push(Arc::clone(&values[0]), &values[1..])?;
        for (i, &g) in group_indices.iter().enumerate() {
            if self.keep(values[0].as_ref(), i, opt_filter) {
                self.entries.push((g as u32, b, i as u32));
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.groups = self.groups.max(total_num_groups);
        let lists = values[0].as_list::<i32>();
        let keys = values[1].as_list::<i32>();
        let (start, end) = (
            lists.value_offsets()[0] as usize,
            lists.value_offsets()[lists.len()] as usize,
        );
        let items = lists.values().slice(start, end - start);
        let k0 = keys.value_offsets()[0] as usize;
        let key_struct = keys.values().slice(k0, end - start);
        let b = self.push(items, key_struct.as_struct().columns())?;
        for (i, &g) in group_indices.iter().enumerate() {
            if lists.is_null(i) {
                continue;
            }
            let (s, e) = (
                lists.value_offsets()[i] as usize - start,
                lists.value_offsets()[i + 1] as usize - start,
            );
            for r in s..e {
                self.entries.push((g as u32, b, r as u32));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (n, taken) = self.take(emit_to);
        let (offsets, nulls) = self.lists(n, &taken)?;
        let items = self.gather(&taken)?;
        self.release();
        Ok(Arc::new(ListArray::try_new(
            Arc::clone(&self.item),
            offsets,
            items,
            nulls,
        )?))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (n, taken) = self.take(emit_to);
        let (offsets, nulls) = self.lists(n, &taken)?;
        let items = self.gather(&taken)?;
        let keys = self.gather_keys(&taken)?;
        self.release();
        let key_item = Arc::new(Field::new_list_field(
            DataType::Struct(self.keys.clone()),
            true,
        ));
        Ok(vec![
            Arc::new(ListArray::try_new(
                Arc::clone(&self.item),
                offsets.clone(),
                items,
                nulls.clone(),
            )?),
            // Declared non-nullable by the built-in: a group that kept nothing has an empty list.
            Arc::new(ListArray::try_new(key_item, offsets, keys, None)?),
        ])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let n = values[0].len();
        let valid: Vec<bool> = (0..n)
            .map(|i| self.keep(values[0].as_ref(), i, opt_filter))
            .collect();
        let offsets = OffsetBuffer::from_lengths(valid.iter().map(|&v| v as usize));
        let nulls = Some(NullBuffer::from(valid.clone()));
        let keep = BooleanArray::from(valid);
        let items = arrow::compute::filter(&values[0], &keep)?;
        let keys = StructArray::try_new(self.keys.clone(), values[1..].to_vec(), None)?;
        let keys = arrow::compute::filter(&keys, &keep)?;
        let key_item = Arc::new(Field::new_list_field(
            DataType::Struct(self.keys.clone()),
            true,
        ));
        Ok(vec![
            Arc::new(ListArray::try_new(
                Arc::clone(&self.item),
                offsets.clone(),
                items,
                nulls.clone(),
            )?),
            // Declared non-nullable by the built-in: a group that kept nothing has an empty list.
            Arc::new(ListArray::try_new(key_item, offsets, keys, None)?),
        ])
    }

    fn size(&self) -> usize {
        self.held + self.entries.capacity() * std::mem::size_of::<(u32, u32, u32)>()
    }
}
