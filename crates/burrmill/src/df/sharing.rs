//! `ShareRepeats`: a subquery that appears more than once is planned and computed once.
//!
//! DataFusion inlines CTEs and views, so `per_epoch` used twice in `epoch_boundaries` aggregated
//! its half-million rows twice, and a view joined twice is planned twice. Identical aliased
//! subtrees that occur more than once, and contain an aggregate or a window, are defined once under
//! a [`SharedDefs`] node at the root, and each occurrence becomes a [`SharedRef`] reading one
//! cache: the first consumer computes the batches and the others read them. Nothing passes a
//! reference from above, so every reader sees the same answer. What a cache holds is charged to the
//! pool; one that does not fit is not kept, and each reader computes the subquery itself.
//!
//! Every copy had been analysed, optimised and planned again, so planning grew with how often a view
//! was reached rather than with the statement (#92). The analyzer's rules after this one run through
//! [`OverShared`], a definition at a time, the ones inside first; the ones before it still see every
//! copy, because they rewrite a subquery from outside it, as the checked sums trace a value to its
//! cast.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{Column, DFSchemaRef, DataFusionError, Result, ScalarValue, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::logical_plan::{Extension, Filter, Projection, SubqueryAlias};
use datafusion_expr::utils::disjunction;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::optimize_projections::OptimizeProjections;
use datafusion_optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_session::{ExtensionPlanner, PhysicalOptimizerRule, PhysicalPlanner};

use super::sequential::execute_stream;
use futures::StreamExt;

/// One shared subquery's plan, set when the statement starts, and its batches once computed.
#[derive(Default)]
struct State {
    plan: Mutex<Option<Arc<dyn ExecutionPlan>>>,
    batches: tokio::sync::OnceCell<Arc<Held>>,
    /// The columns projection pruning asked of the references, for [`PruneShared`].
    wanted: Mutex<BTreeSet<usize>>,
    /// The disjunction of the readers' filters already put on the definition, so a later pass
    /// does not put it there again.
    pushed: Mutex<Option<Expr>>,
}

/// What the first reader kept for the others.
enum Held {
    /// The reservation is held for as long as the batches are, and released with them.
    Batches {
        batches: Vec<RecordBatch>,
        _charged: MemoryReservation,
    },
    /// Over what the pool would give: each reader runs the plan itself.
    TooBig,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "State")
    }
}

type Shared = Arc<State>;

#[derive(Debug, Default)]
pub struct ShareRepeats;

/// Per node, bottom up: a key that identical subtrees share, and what decides whether an alias over
/// it is a candidate.
#[derive(Clone, Copy)]
struct Summary {
    key: u64,
    /// An aggregate or a window: work a reader would otherwise repeat.
    costly: bool,
    outer: bool,
}

/// The aliases worth sharing, by the address of their input: over an aggregate or a window, not
/// correlated, and not shared yet.
#[derive(Default)]
struct Candidates {
    stack: Vec<Summary>,
    marks: Vec<usize>,
    found: Vec<(*const LogicalPlan, u64)>,
}

/// Already shared, through any aliases of aliases. A window a filter above turns into an aggregate
/// (`TopPerGroup`, `PartitionExtreme`) has been turned by then: both run before this rule.
fn not_a_candidate(input: &LogicalPlan) -> bool {
    let mut under = input;
    loop {
        under = match under {
            LogicalPlan::SubqueryAlias(s) => s.input.as_ref(),
            LogicalPlan::Projection(p) => p.input.as_ref(),
            _ => break,
        };
    }
    matches!(under, LogicalPlan::Extension(e) if e.node.as_any().is::<SharedNode>())
}

/// What identifies `p` apart from its children. Collisions only cost an equality check.
fn local(p: &LogicalPlan, h: &mut rustc_hash::FxHasher) {
    std::mem::discriminant(p).hash(h);
    let _ = p.apply_expressions(|e| {
        e.hash(h);
        Ok(TreeNodeRecursion::Continue)
    });
    match p {
        LogicalPlan::TableScan(t) => {
            t.table_name.hash(h);
            t.projection.hash(h);
            t.fetch.hash(h);
        }
        LogicalPlan::SubqueryAlias(s) => s.alias.hash(h),
        LogicalPlan::Join(j) => j.join_type.hash(h),
        LogicalPlan::Sort(s) => s.fetch.hash(h),
        LogicalPlan::Extension(e) => e.node.dyn_hash(h),
        _ => {}
    }
}

impl<'n> TreeNodeVisitor<'n> for Candidates {
    type Node = LogicalPlan;

    fn f_down(&mut self, _n: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        self.marks.push(self.stack.len());
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, n: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        let mark = self.marks.pop().expect("f_down marked");
        // Subqueries are visited before children, so the children are the last entries.
        let below = self.stack.split_off(mark);
        let inputs = &below[below.len() - n.inputs().len()..];
        let mut h = rustc_hash::FxHasher::default();
        local(n, &mut h);
        below.iter().for_each(|s| s.key.hash(&mut h));
        let summary = Summary {
            key: h.finish(),
            costly: matches!(n, LogicalPlan::Aggregate(_) | LogicalPlan::Window(_))
                || inputs.iter().any(|s| s.costly),
            outer: n.contains_outer_reference() || inputs.iter().any(|s| s.outer),
        };
        if let LogicalPlan::SubqueryAlias(s) = n
            && let [input] = inputs
            && input.costly
            && !input.outer
            && !not_a_candidate(&s.input)
        {
            self.found.push((Arc::as_ptr(&s.input), input.key));
        }
        self.stack.push(summary);
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Whether a repeated candidate sits beneath each repeated candidate, bottom up.
struct Beneath<'a> {
    repeated: &'a HashSet<*const LogicalPlan>,
    stack: Vec<bool>,
    marks: Vec<usize>,
    innermost: HashSet<*const LogicalPlan>,
}

impl<'n> TreeNodeVisitor<'n> for Beneath<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, _n: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        self.marks.push(self.stack.len());
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, n: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        let mark = self.marks.pop().expect("f_down marked");
        let below = self.stack.split_off(mark).into_iter().any(|b| b);
        let repeat = match n {
            LogicalPlan::SubqueryAlias(s) => {
                let input = Arc::as_ptr(&s.input);
                let repeat = self.repeated.contains(&input);
                if repeat && !below {
                    self.innermost.insert(input);
                }
                repeat
            }
            _ => false,
        };
        self.stack.push(repeat || below);
        Ok(TreeNodeRecursion::Continue)
    }
}

/// This pass's aliases to share, by their input's address: repeated, with no repeat beneath them.
fn innermost_repeats(plan: &LogicalPlan) -> Result<HashMap<*const LogicalPlan, u64>> {
    let mut c = Candidates::default();
    plan.visit_with_subqueries(&mut c)?;
    let mut counts: HashMap<u64, usize> = HashMap::new();
    for (_, k) in &c.found {
        *counts.entry(*k).or_default() += 1;
    }
    let repeated: HashSet<*const LogicalPlan> = c
        .found
        .iter()
        .filter(|(_, k)| counts[k] >= 2)
        .map(|(p, _)| *p)
        .collect();
    if repeated.is_empty() {
        return Ok(HashMap::new());
    }
    let mut b = Beneath {
        repeated: &repeated,
        stack: Vec::new(),
        marks: Vec::new(),
        innermost: HashSet::new(),
    };
    plan.visit_with_subqueries(&mut b)?;
    Ok(c.found
        .into_iter()
        .filter(|(p, _)| b.innermost.contains(p))
        .collect())
}

/// Each repeat wrapped in a [`SharedNode`], innermost first, one layer per pass. Sharing an outer
/// subquery whole hid every repeat inside it from the other occurrences, which then computed it
/// again: a view read by four views that are themselves repeated was scanned once per copy (#91).
fn mark(mut plan: LogicalPlan) -> Result<LogicalPlan> {
    let mut firsts: HashMap<u64, (LogicalPlan, Shared)> = HashMap::new();
    loop {
        let chosen = innermost_repeats(&plan)?;
        if chosen.is_empty() {
            return Ok(plan);
        }
        // Top down, so an alias is seen before anything under it moves and its input's address
        // is still the one counted.
        let pass = plan.transform_down_with_subqueries(|n| {
            let LogicalPlan::SubqueryAlias(s) = &n else {
                return Ok(Transformed::no(n));
            };
            let Some(&k) = chosen.get(&Arc::as_ptr(&s.input)) else {
                return Ok(Transformed::no(n));
            };
            let (first, state) = firsts
                .entry(k)
                .or_insert_with(|| (s.input.as_ref().clone(), Shared::default()))
                .clone();
            if first != *s.input {
                return Ok(Transformed::no(n));
            }
            let shared = LogicalPlan::Extension(Extension {
                node: Arc::new(SharedNode {
                    input: first,
                    state,
                    id: k,
                }),
            });
            let alias = SubqueryAlias::try_new(Arc::new(shared), s.alias.clone())?;
            Ok(Transformed::new(
                LogicalPlan::SubqueryAlias(alias),
                true,
                TreeNodeRecursion::Jump,
            ))
        })?;
        if !pass.transformed {
            return Ok(pass.data);
        }
        plan = pass.data;
    }
}

impl AnalyzerRule for ShareRepeats {
    fn name(&self) -> &str {
        "share_repeats"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let shared = |p: LogicalPlan| mark(p).and_then(hoist).map(Transformed::yes);
        match plan {
            // EXPLAIN ANALYZE reports the plan under it, definitions included.
            LogicalPlan::Analyze(_) => plan.map_children(shared).map(|t| t.data),
            plan => shared(plan).map(|t| t.data),
        }
    }
}

/// One occurrence of a shared subquery while the repeats are found, before [`hoist`].
struct SharedNode {
    input: LogicalPlan,
    state: Shared,
    id: u64,
}

impl fmt::Debug for SharedNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Shared {:016x}", self.id)
    }
}
impl PartialEq for SharedNode {
    fn eq(&self, o: &Self) -> bool {
        self.id == o.id && Arc::ptr_eq(&self.state, &o.state)
    }
}
impl Eq for SharedNode {}
impl Hash for SharedNode {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.id.hash(h);
    }
}
impl PartialOrd for SharedNode {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&o.id)
    }
}

impl UserDefinedLogicalNodeCore for SharedNode {
    fn name(&self) -> &str {
        "Shared"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Shared: computed once, id={:016x}", self.id)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs.swap_remove(0),
            state: Arc::clone(&self.state),
            id: self.id,
        })
    }
}

/// Each marked subquery once, under [`SharedDefs`] at the root, and a [`SharedRef`] where each
/// occurrence was. Bottom up, so a definition holds references to the ones inside it, not copies,
/// and comes after them.
fn hoist(plan: LogicalPlan) -> Result<LogicalPlan> {
    let mut defs: Vec<(u64, Shared, LogicalPlan)> = Vec::new();
    let mut index: HashMap<u64, usize> = HashMap::new();
    let body = plan.transform_up_with_subqueries(|n| {
        let LogicalPlan::Extension(e) = &n else {
            return Ok(Transformed::no(n));
        };
        let Some(s) = e.node.as_any().downcast_ref::<SharedNode>() else {
            return Ok(Transformed::no(n));
        };
        let i = *index.entry(s.id).or_insert_with(|| {
            defs.push((s.id, Arc::clone(&s.state), s.input.clone()));
            defs.len() - 1
        });
        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(SharedRef {
                id: s.id,
                state: Arc::clone(&defs[i].1),
                schema: Arc::clone(defs[i].2.schema()),
            }),
        })))
    })?;
    if defs.is_empty() {
        return Ok(body.data);
    }
    let mut node = SharedDefs {
        ids: Vec::new(),
        states: Vec::new(),
        schemas: Vec::new(),
        defs: Vec::new(),
        body: body.data,
    };
    for (id, state, def) in defs {
        node.ids.push(id);
        node.states.push(state);
        node.schemas.push(Arc::clone(def.schema()));
        node.defs.push(def);
    }
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}

/// Subqueries a rule after sharing defines once and reads in several places.
pub(super) struct Definitions {
    added: Vec<(u64, Shared, LogicalPlan)>,
}

impl Definitions {
    /// `plan`, computed once, and a reference to it with its columns.
    pub(super) fn define(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let mut h = rustc_hash::FxHasher::default();
        plan.hash(&mut h);
        self.added.len().hash(&mut h);
        let (id, state) = (h.finish(), Shared::default());
        let reference = LogicalPlan::Extension(Extension {
            node: Arc::new(SharedRef {
                id,
                state: Arc::clone(&state),
                schema: Arc::clone(plan.schema()),
            }),
        });
        self.added.push((id, state, plan));
        reference
    }
}

/// `f` over the statement and each existing definition, with what it defines placed beside them.
pub(super) fn with_definitions(
    plan: LogicalPlan,
    f: &mut dyn FnMut(LogicalPlan, &mut Definitions) -> Result<LogicalPlan>,
) -> Result<LogicalPlan> {
    if let LogicalPlan::Analyze(_) = plan {
        return plan
            .map_children(|p| with_definitions(p, f).map(Transformed::yes))
            .map(|t| t.data);
    }
    let mut new = Definitions { added: Vec::new() };
    let mut node = match &plan {
        LogicalPlan::Extension(e) => match e.node.as_any().downcast_ref::<SharedDefs>() {
            Some(d) => SharedDefs {
                ids: d.ids.clone(),
                states: d.states.clone(),
                schemas: d.schemas.clone(),
                defs: d.defs.clone(),
                body: d.body.clone(),
            },
            None => SharedDefs::around(plan),
        },
        _ => SharedDefs::around(plan),
    };
    for def in &mut node.defs {
        *def = f(std::mem::take(def), &mut new)?;
    }
    node.body = f(std::mem::take(&mut node.body), &mut new)?;
    for (id, state, def) in new.added {
        node.ids.push(id);
        node.states.push(state);
        node.schemas.push(Arc::clone(def.schema()));
        node.defs.push(def);
    }
    if node.defs.is_empty() {
        return Ok(node.body);
    }
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}

/// An analyzer rule run over a statement with shared subqueries a definition at a time, the ones
/// inside first, then the statement. A rule that changes a definition's columns, as the checked
/// sums do, has the references to it carry the new ones before it reaches their readers, as it
/// would have had the subquery been inline.
#[derive(Debug)]
pub struct OverShared(pub Arc<dyn AnalyzerRule + Send + Sync>);

impl AnalyzerRule for OverShared {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let rule = self.0.as_ref();
        match plan {
            LogicalPlan::Analyze(_) => plan
                .map_children(|p| over(rule, p, config).map(Transformed::yes))
                .map(|t| t.data),
            plan => over(rule, plan, config),
        }
    }
}

fn over(
    rule: &(dyn AnalyzerRule + Send + Sync),
    plan: LogicalPlan,
    config: &ConfigOptions,
) -> Result<LogicalPlan> {
    let LogicalPlan::Extension(e) = &plan else {
        return rule.analyze(plan, config);
    };
    let Some(d) = e.node.as_any().downcast_ref::<SharedDefs>() else {
        return rule.analyze(plan, config);
    };
    let mut defs = d.defs.clone();
    let mut schemas = d.schemas.clone();
    let mut body = d.body.clone();
    for i in 0..defs.len() {
        let def = std::mem::take(&mut defs[i]);
        defs[i] = rule.analyze(def, config)?;
        if *defs[i].schema() == schemas[i] {
            continue;
        }
        schemas[i] = Arc::clone(defs[i].schema());
        let (id, schema) = (d.ids[i], &schemas[i]);
        for later in &mut defs[i + 1..] {
            *later = refresh(std::mem::take(later), id, schema)?;
        }
        body = refresh(body, id, schema)?;
    }
    let body = rule.analyze(body, config)?;
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(SharedDefs {
            ids: d.ids.clone(),
            states: d.states.clone(),
            schemas,
            defs,
            body,
        }),
    }))
}

/// `plan` with each reference to `id` carrying `schema`, and every node above one rebuilt for it.
fn refresh(plan: LogicalPlan, id: u64, schema: &DFSchemaRef) -> Result<LogicalPlan> {
    let mut reads = false;
    plan.apply_with_subqueries(|n| {
        reads = matches!(n, LogicalPlan::Extension(e)
            if e.node.as_any().downcast_ref::<SharedRef>().is_some_and(|r| r.id == id));
        Ok(match reads {
            true => TreeNodeRecursion::Stop,
            false => TreeNodeRecursion::Continue,
        })
    })?;
    if !reads {
        return Ok(plan);
    }
    plan.transform_up_with_subqueries(|n| {
        if let LogicalPlan::Extension(e) = &n
            && let Some(r) = e.node.as_any().downcast_ref::<SharedRef>()
            && r.id == id
        {
            return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(SharedRef {
                    id,
                    state: Arc::clone(&r.state),
                    schema: Arc::clone(schema),
                }),
            })));
        }
        n.recompute_schema().map(Transformed::yes)
    })
    .map(|t| t.data)
}

/// Where a shared subquery was: its batches, computed by whichever reader comes first.
pub struct SharedRef {
    id: u64,
    state: Shared,
    schema: DFSchemaRef,
}

impl fmt::Debug for SharedRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedRef {:016x}", self.id)
    }
}
impl PartialEq for SharedRef {
    fn eq(&self, o: &Self) -> bool {
        self.id == o.id && Arc::ptr_eq(&self.state, &o.state) && self.schema == o.schema
    }
}
impl Eq for SharedRef {}
impl Hash for SharedRef {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.id.hash(h);
    }
}
impl PartialOrd for SharedRef {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&o.id)
    }
}

impl UserDefinedLogicalNodeCore for SharedRef {
    fn name(&self) -> &str {
        "SharedRef"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Shared: computed once, id={:016x}", self.id)
    }
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        lock(&self.state.wanted).extend(output_columns);
        Some(vec![])
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            id: self.id,
            state: Arc::clone(&self.state),
            schema: Arc::clone(&self.schema),
        })
    }
}

/// The statement, `body`, and each shared subquery it reads, once. `schemas` are the columns the
/// references to each carry.
pub struct SharedDefs {
    ids: Vec<u64>,
    states: Vec<Shared>,
    schemas: Vec<DFSchemaRef>,
    defs: Vec<LogicalPlan>,
    body: LogicalPlan,
}

impl SharedDefs {
    fn around(body: LogicalPlan) -> Self {
        Self {
            ids: Vec::new(),
            states: Vec::new(),
            schemas: Vec::new(),
            defs: Vec::new(),
            body,
        }
    }
}

impl fmt::Debug for SharedDefs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedDefs {}", self.ids.len())
    }
}
impl PartialEq for SharedDefs {
    fn eq(&self, o: &Self) -> bool {
        self.ids == o.ids
            && self
                .states
                .iter()
                .zip(&o.states)
                .all(|(a, b)| Arc::ptr_eq(a, b))
            && self.defs == o.defs
            && self.body == o.body
    }
}
impl Eq for SharedDefs {}
impl Hash for SharedDefs {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.ids.hash(h);
        self.defs.hash(h);
        self.body.hash(h);
    }
}
impl PartialOrd for SharedDefs {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        self.ids.partial_cmp(&o.ids)
    }
}

impl UserDefinedLogicalNodeCore for SharedDefs {
    fn name(&self) -> &str {
        "SharedDefs"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.defs.iter().chain([&self.body]).collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        self.body.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ids: Vec<String> = self.ids.iter().map(|i| format!("{i:016x}")).collect();
        write!(
            f,
            "SharedDefs: ids=[{}], then the statement",
            ids.join(", ")
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let Some(body) = inputs.pop() else {
            return internal_err!("SharedDefs without its statement");
        };
        if inputs.len() != self.ids.len() {
            return internal_err!("SharedDefs lost a definition");
        }
        Ok(Self {
            ids: self.ids.clone(),
            states: self.states.clone(),
            schemas: self.schemas.clone(),
            defs: inputs,
            body,
        })
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The optimizer's last rule: each shared subquery cut to the columns its readers take, and to the
/// rows, where every reader filters.
///
/// Sharing is decided in the analyzer, before projections are pruned or filters pushed, and neither
/// reaches through a reference, so a view read twice computed every column and row it has whatever
/// either reader took (#97). Each reference records what pruning asks of it; a definition is cut to
/// the union, and each reference handed back its old columns, the unread ones as constants, so
/// nothing above it changes shape and the next pass prunes them away. Where every reference sits
/// under a filter, the definition is filtered by their disjunction, which the next pass pushes in;
/// each reader keeps its own.
#[derive(Debug, Default)]
pub struct PruneShared;

impl OptimizerRule for PruneShared {
    fn name(&self) -> &str {
        "prune_shared"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
    fn supports_rewrite(&self) -> bool {
        true
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Analyze(_) => plan.map_children(|p| prune(p, config)),
            plan => prune(plan, config),
        }
    }
}

fn prune(plan: LogicalPlan, config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Extension(e) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let Some(d) = e.node.as_any().downcast_ref::<SharedDefs>() else {
        return Ok(Transformed::no(plan));
    };
    d.states.iter().for_each(|s| lock(&s.wanted).clear());
    let rule = OptimizeProjections::new();
    let body = rule.rewrite(d.body.clone(), config)?.data;
    // A definition's readers may sit in a definition after it or before it, so until nothing grows.
    let mut cut: Vec<Option<(BTreeSet<usize>, LogicalPlan)>> = vec![None; d.defs.len()];
    loop {
        let mut grew = false;
        for (i, def) in d.defs.iter().enumerate().rev() {
            let wanted = lock(&d.states[i].wanted).clone();
            if cut[i].as_ref().is_some_and(|(w, _)| *w == wanted) {
                continue;
            }
            // A reader that takes no column, as `count(*)` does, still needs the rows.
            let keep: Vec<usize> = match wanted.is_empty() {
                true => vec![0],
                false => wanted.iter().copied().collect(),
            };
            let narrowed = match keep.len() == def.schema().fields().len() {
                true => def.clone(),
                false => LogicalPlan::Projection(Projection::try_new(
                    keep.iter()
                        .map(|&c| Expr::Column(Column::from(def.schema().qualified_field(c))))
                        .collect(),
                    Arc::new(def.clone()),
                )?),
            };
            cut[i] = Some((wanted, rule.rewrite(narrowed, config)?.data));
            grew = true;
        }
        if !grew {
            break;
        }
    }
    let mut narrowed: HashMap<u64, DFSchemaRef> = HashMap::new();
    let mut defs = d.defs.clone();
    let mut schemas = d.schemas.clone();
    for (i, c) in cut.into_iter().enumerate() {
        let (_, def) = c.expect("every definition was cut");
        if def.schema().fields().len() == d.defs[i].schema().fields().len() {
            continue;
        }
        narrowed.insert(d.ids[i], Arc::clone(def.schema()));
        schemas[i] = Arc::clone(def.schema());
        defs[i] = def;
    }
    let readers = reader_filters(std::iter::once(&d.body).chain(&d.defs))?;
    let mut filtered = false;
    for (i, def) in defs.iter_mut().enumerate() {
        let Some(Some(predicates)) = readers.get(&d.ids[i]) else {
            continue;
        };
        let Some(any) = disjunction(predicates.iter().cloned()) else {
            continue;
        };
        if !any.column_refs().iter().all(|c| def.schema().has_column(c)) {
            continue;
        }
        let mut pushed = lock(&d.states[i].pushed);
        if pushed.as_ref() == Some(&any) {
            continue;
        }
        *pushed = Some(any.clone());
        *def = LogicalPlan::Filter(Filter::try_new(any, Arc::new(std::mem::take(def)))?);
        filtered = true;
    }
    if narrowed.is_empty() && !filtered {
        return Ok(Transformed::no(plan));
    }
    let defs = defs
        .into_iter()
        .map(|p| widen(p, &narrowed))
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
        node: Arc::new(SharedDefs {
            ids: d.ids.clone(),
            states: d.states.clone(),
            schemas,
            defs,
            body: widen(body, &narrowed)?,
        }),
    })))
}

/// Per definition, the distinct predicates its references are read under, or `None` where any
/// reference is read unfiltered or under a predicate that cannot move.
fn reader_filters<'a>(
    plans: impl Iterator<Item = &'a LogicalPlan>,
) -> Result<HashMap<u64, Option<Vec<Expr>>>> {
    let mut out: HashMap<u64, Option<Vec<Expr>>> = HashMap::new();
    for plan in plans {
        plan.apply_with_subqueries(|n| {
            let (id, predicate) = match n {
                LogicalPlan::Filter(f) => match reference_under(&f.input) {
                    Some(id) => (id, movable(&f.predicate)?.then(|| f.predicate.clone())),
                    None => return Ok(TreeNodeRecursion::Continue),
                },
                LogicalPlan::Extension(e) => match e.node.as_any().downcast_ref::<SharedRef>() {
                    Some(r) => (r.id, None),
                    None => return Ok(TreeNodeRecursion::Continue),
                },
                _ => return Ok(TreeNodeRecursion::Continue),
            };
            let seen = out.entry(id).or_insert_with(|| Some(Vec::new()));
            match (seen.as_mut(), predicate) {
                (Some(all), Some(p)) if !all.contains(&p) => all.push(p),
                (Some(_), Some(_)) => {}
                (_, None) => *seen = None,
                (None, Some(_)) => {}
            }
            // The reference under the filter is accounted for.
            Ok(TreeNodeRecursion::Jump)
        })?;
    }
    Ok(out)
}

/// The shared reference `p` reads, directly or through the projection [`widen`] puts over it.
fn reference_under(p: &LogicalPlan) -> Option<u64> {
    let under = match p {
        LogicalPlan::Projection(p) => p.input.as_ref(),
        p => p,
    };
    match under {
        LogicalPlan::Extension(e) => e.node.as_any().downcast_ref::<SharedRef>().map(|r| r.id),
        _ => None,
    }
}

/// A predicate the same over the definition as over one reader: no subquery, nothing volatile.
fn movable(e: &Expr) -> Result<bool> {
    let reaches = e.exists(|e| {
        Ok(matches!(
            e,
            Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::OuterReferenceColumn(..)
        ))
    })?;
    Ok(!reaches && !e.is_volatile())
}

/// Each reference to a cut definition reading the cut columns, with the old ones around them.
fn widen(plan: LogicalPlan, narrowed: &HashMap<u64, DFSchemaRef>) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|n| {
        let LogicalPlan::Extension(e) = &n else {
            return Ok(Transformed::no(n));
        };
        let Some(r) = e.node.as_any().downcast_ref::<SharedRef>() else {
            return Ok(Transformed::no(n));
        };
        let Some(schema) = narrowed.get(&r.id) else {
            return Ok(Transformed::no(n));
        };
        let exprs = r
            .schema
            .iter()
            .map(|(q, f)| {
                let c = Column::new(q.cloned(), f.name());
                match schema.has_column(&c) {
                    true => Ok(Expr::Column(c)),
                    false => unread(q, f),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let reference = LogicalPlan::Extension(Extension {
            node: Arc::new(SharedRef {
                id: r.id,
                state: Arc::clone(&r.state),
                schema: Arc::clone(schema),
            }),
        });
        Projection::try_new(exprs, Arc::new(reference))
            .map(|p| Transformed::yes(LogicalPlan::Projection(p)))
    })
    .map(|t| t.data)
}

/// A column no reader takes, since pruning asked for every one a reader uses. Nullable only where
/// it was, so the nodes above keep their schemas.
fn unread(
    q: Option<&datafusion_common::TableReference>,
    f: &arrow::datatypes::Field,
) -> Result<Expr> {
    let v = match f.is_nullable() {
        true => ScalarValue::try_new_null(f.data_type())?,
        false => ScalarValue::new_default(f.data_type())?,
    };
    Ok(Expr::Literal(v, None).alias_qualified(q.cloned(), f.name()))
}

#[derive(Debug)]
pub struct SharedPlanner;

#[async_trait]
impl ExtensionPlanner for SharedPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _ctx: &datafusion_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(n) = node.as_any().downcast_ref::<SharedRef>() {
            let schema: SchemaRef = Arc::new(n.schema.as_arrow().clone());
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ));
            return Ok(Some(Arc::new(SharedExec {
                schema,
                state: Arc::clone(&n.state),
                properties,
            })));
        }
        if let Some(n) = node.as_any().downcast_ref::<SharedDefs>() {
            let Some((body, defs)) = physical_inputs.split_last() else {
                return internal_err!("SharedDefs without its statement");
            };
            // The statement's ORDER BY, recorded as DataFusion records it at the root, where it
            // stops at a node of more than one child and would otherwise drop the sort.
            let body = OutputRequirements::new_add_mode()
                .optimize(Arc::clone(body), session.config_options())?;
            let exec = SharedDefsExec {
                states: n.states.clone(),
                defs: defs.to_vec(),
                body,
            };
            exec.publish();
            return Ok(Some(Arc::new(exec)));
        }
        Ok(None)
    }
}

#[derive(Debug)]
struct SharedDefsExec {
    states: Vec<Shared>,
    defs: Vec<Arc<dyn ExecutionPlan>>,
    body: Arc<dyn ExecutionPlan>,
}

impl SharedDefsExec {
    /// Each definition where its references find it: as planned, for the rules that measure a
    /// reference's input, and as finally optimised, when the statement starts.
    fn publish(&self) {
        for (state, def) in self.states.iter().zip(&self.defs) {
            *state.plan.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(def));
        }
    }
}

/// The physical optimizer's last rule: each definition as finally optimised, before anything runs.
/// A scalar subquery above the statement runs its subqueries before `SharedDefsExec` executes.
#[derive(Debug)]
pub(super) struct PublishShared;

impl PhysicalOptimizerRule for PublishShared {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.apply(|p| {
            if let Some(d) = p.downcast_ref::<SharedDefsExec>() {
                d.publish();
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "publish_shared"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether a hash join should build on `p`: it is the rows a statement's limit kept, or their keys
/// ([`super::narrow`]). Only those: a plan the narrowing did not touch keeps its build sides.
pub(super) fn few_rows(p: &Arc<dyn ExecutionPlan>) -> bool {
    if p.is::<super::narrow::NarrowedExec>() {
        return true;
    }
    match p.children().as_slice() {
        [c] if passes_rows(p) => few_rows(c),
        _ => false,
    }
}

/// An operator that answers as many rows as its one input or fewer.
fn passes_rows(p: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::repartition::RepartitionExec;
    p.is::<ProjectionExec>()
        || p.is::<FilterExec>()
        // Grouping sets answer a row per set for each group.
        || p.downcast_ref::<datafusion_physical_plan::aggregates::AggregateExec>()
            .is_some_and(|a| a.group_expr().is_single())
        || p.is::<datafusion_physical_plan::sorts::sort::SortExec>()
        || p.is::<RepartitionExec>()
        || p.is::<CoalescePartitionsExec>()
        || matches!(
            p.name(),
            "CastViewsExec" | "CompactViewsExec" | "CancelExec" | "GatherExec"
        )
}

/// The plan a shared reference reads, as planned so far.
pub(super) fn definition(p: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    p.downcast_ref::<SharedExec>()?
        .state
        .plan
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

impl DisplayAs for SharedDefsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SharedDefsExec: {} shared, then the statement",
            self.defs.len()
        )
    }
}

impl ExecutionPlan for SharedDefsExec {
    fn name(&self) -> &str {
        "SharedDefsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.body.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.defs.iter().chain([&self.body]).collect()
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        let mut m = vec![false; self.defs.len()];
        m.push(true);
        m
    }
    // The statement's output is what the caller reads; spreading it over partitions undid its order.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.defs.len() + 1]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(body) = children.pop() else {
            return internal_err!("SharedDefsExec without its statement");
        };
        let exec = SharedDefsExec {
            states: self.states.clone(),
            defs: children,
            body,
        };
        exec.publish();
        Ok(Arc::new(exec))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The plans as the physical optimizer left them, before anything can read one.
        self.publish();
        self.body.execute(partition, ctx)
    }
}

#[derive(Debug)]
struct SharedExec {
    schema: SchemaRef,
    state: Shared,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for SharedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SharedExec: computed once")
    }
}

impl ExecutionPlan for SharedExec {
    fn name(&self) -> &str {
        "SharedExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let state = Arc::clone(&self.state);
        let stream = futures::stream::once(async move {
            let plan = state
                .plan
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .ok_or_else(|| {
                    DataFusionError::Internal("a shared subquery read before it was defined".into())
                })?;
            let held = state
                .batches
                .get_or_try_init(|| hold(Arc::clone(&plan), Arc::clone(&ctx)))
                .await
                .map_err(|e| DataFusionError::Context("shared subquery".into(), Box::new(e)))?;
            let rows: SendableRecordBatchStream = match held.as_ref() {
                Held::Batches { batches, .. } => Box::pin(RecordBatchStreamAdapter::new(
                    plan.schema(),
                    futures::stream::iter(batches.clone().into_iter().map(Ok)),
                )),
                Held::TooBig => execute_stream(plan, ctx)?,
            };
            Ok::<_, DataFusionError>(rows)
        })
        .flat_map(|r| match r {
            Ok(s) => s.left_stream(),
            Err(e) => futures::stream::once(async move { Err(e) }).right_stream(),
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// The plan's batches, kept for the other readers if the pool gives room for them.
async fn hold(plan: Arc<dyn ExecutionPlan>, ctx: Arc<TaskContext>) -> Result<Arc<Held>> {
    let reservation = MemoryConsumer::new("SharedExec").register(ctx.memory_pool());
    let mut stream = execute_stream(plan, ctx)?;
    let mut batches = Vec::new();
    while let Some(b) = stream.next().await {
        let b = b?;
        if reservation.try_grow(b.get_array_memory_size()).is_err() {
            return Ok(Arc::new(Held::TooBig));
        }
        batches.push(b);
    }
    Ok(Arc::new(Held::Batches {
        batches,
        _charged: reservation,
    }))
}

#[cfg(test)]
mod tests {
    use datafusion_expr::logical_plan::Projection;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};

    use super::*;

    /// Turns the one `1 AS x` it finds from a BIGINT into an INT, as a rule that retypes a
    /// definition's column does.
    #[derive(Debug)]
    struct Narrow;

    impl AnalyzerRule for Narrow {
        fn name(&self) -> &str {
            "narrow"
        }
        fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
            plan.transform_up(|n| match &n {
                LogicalPlan::Projection(p) if p.expr == vec![lit(1i64).alias("x")] => {
                    Projection::try_new(vec![lit(1i32).alias("x")], Arc::clone(&p.input))
                        .map(|p| Transformed::yes(LogicalPlan::Projection(p)))
                }
                _ => Ok(Transformed::no(n)),
            })
            .map(|t| t.data)
        }
    }

    /// Two readers of one shared subquery under a pool of `pool` bytes: the rows each read, and
    /// what the pool still holds while the subquery is alive.
    fn read_twice(pool: usize) -> (usize, usize, usize) {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::UInt64Array::from_iter_values(
                0..10_000,
            ))],
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> =
            datafusion_datasource::memory::MemorySourceConfig::try_new_exec(
                &[vec![batch.clone(), batch]],
                Arc::clone(&schema),
                None,
            )
            .unwrap();
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool)))
            .build_arc()
            .unwrap();
        let ctx = Arc::new(TaskContext::default().with_runtime(Arc::clone(&runtime)));
        let state = Shared::default();
        *state.plan.lock().unwrap() = Some(plan);
        let reader = || SharedExec {
            schema: Arc::clone(&schema),
            state: Arc::clone(&state),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let rows = |r: SharedExec| {
            let batches = rt
                .block_on(datafusion_physical_plan::common::collect(
                    r.execute(0, Arc::clone(&ctx)).unwrap(),
                ))
                .unwrap();
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        };
        let (a, b) = (rows(reader()), rows(reader()));
        (a, b, runtime.memory_pool.reserved())
    }

    #[test]
    fn a_shared_subquery_is_held_against_the_pool() {
        let (a, b, held) = read_twice(64 << 20);
        assert_eq!((a, b), (20_000, 20_000));
        assert!(held >= 20_000 * 8, "{held} held for 160 KB of rows");
    }

    #[test]
    fn a_shared_subquery_the_pool_cannot_hold_is_computed_by_each_reader() {
        let (a, b, held) = read_twice(64 << 10);
        assert_eq!((a, b), (20_000, 20_000));
        assert_eq!(held, 0);
    }

    #[test]
    fn a_reference_takes_the_columns_a_rule_gives_its_definition() {
        let def = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("x")])
            .unwrap()
            .build()
            .unwrap();
        let state = Shared::default();
        let reference = LogicalPlan::Extension(Extension {
            node: Arc::new(SharedRef {
                id: 7,
                state: Arc::clone(&state),
                schema: Arc::clone(def.schema()),
            }),
        });
        let body = LogicalPlanBuilder::from(reference)
            .project(vec![col("x").alias("y")])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(SharedDefs {
                ids: vec![7],
                states: vec![state],
                schemas: vec![Arc::clone(def.schema())],
                defs: vec![def],
                body,
            }),
        });
        let out = OverShared(Arc::new(Narrow))
            .analyze(plan, &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            out.schema().field(0).data_type(),
            &arrow::datatypes::DataType::Int32,
            "{out}"
        );
    }
}
