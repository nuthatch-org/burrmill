//! `TransitiveKeys`: a table equated with a filtered dimension's key only through another table
//! reads only the keys that dimension keeps (#433).
//!
//! TPC-H Q5 joins `customer`, `orders`, `lineitem`, `supplier`, `nation` and `region`, with
//! `c_nationkey = s_nationkey`, `s_nationkey = n_nationkey` and `r_name = 'ASIA'`. The joins are
//! planned in the order written, so the five nations of `ASIA` meet `supplier`, through the
//! dynamic filter of the join on `s_nationkey`, but never `customer`: the join that equates
//! `c_nationkey` with `s_nationkey` is on two columns, `l_suppkey` too, and its dynamic filter
//! cannot be split between its probe's scans. At SF10 the joins below it then carried all 2.28M
//! orders of the year and 9.10M of their `lineitem` rows, where DuckDB 2.0.1 joins the nations to
//! `customer` first and carries 456.8K and 1.83M.
//!
//! Over a tree of inner joins, the rule puts the columns its equalities join in classes. A leaf
//! `n`, a table scan or what the tree does not look into, is a dimension where it is filtered or
//! joined to a filtered leaf, `r`: every row out of the tree holds an `n` row that met such an `r`
//! row, so its value of any other column of `n` in a class is one of `n ⋉ r`'s. A leaf `t` with a
//! column `c` in that class keeps only its rows whose `c` is among them, with a semi join on `n ⋉
//! r`'s column, built on the dimension, whose dynamic filter then reaches `t`'s scan. A row it
//! drops meets no row of `n` on that equality, so the tree drops it too; NULL meets nothing in
//! either.
//!
//! Only where `t` is not joined to `n` itself, whose own join's dynamic filter can reach it, and
//! where the dimension reads at most a sixteenth of what `t` does, by the catalogue's file sizes,
//! as it is read again; unmeasured, the plan is kept. Joins with NULL equal to NULL, or null-aware,
//! end the tree, and a leaf with a volatile expression is never a dimension, a second scan not
//! repeating it. A semi join added once is marked by its alias, so the optimizer's later passes do
//! not add another. In TPC-H only Q5 has such a key, `c_nationkey`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, NullEquality, Result};
use datafusion_expr::logical_plan::{Join, JoinConstraint, Projection, SubqueryAlias};
use datafusion_expr::{Expr, JoinType, LogicalPlan};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

/// The alias of an added semi join's keys, and its mark.
const KEYS: &str = "__transitive_keys";
const KEY: &str = "__transitive_key";

/// How many times what a dimension reads the table it filters must read at least.
const SMALLER: u64 = 16;

#[derive(Debug, Default)]
pub struct TransitiveKeys;

impl OptimizerRule for TransitiveKeys {
    fn name(&self) -> &str {
        "transitive_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !inner(&plan) {
            return Ok(Transformed::no(plan));
        }
        let mut tree = Tree::default();
        tree.walk(&plan);
        let Some((t, c, keys)) = tree.reduction(config)? else {
            return Ok(Transformed::no(plan));
        };
        let mut seen = 0;
        replace(&plan, t, &mut seen, &mut |leaf| {
            let key = Column::from(keys.schema().qualified_field(0));
            Ok(LogicalPlan::Join(Join::try_new(
                Arc::new(keys.clone()),
                Arc::new(leaf.clone()),
                vec![(Expr::Column(key), Expr::Column(c.clone()))],
                None,
                JoinType::RightSemi,
                JoinConstraint::On,
                NullEquality::NullEqualsNothing,
                false,
            )?))
        })
        .map(Transformed::yes)
    }
}

/// An inner equijoin the tree looks into.
fn inner(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Join(j) if j.join_type == JoinType::Inner
        && j.null_equality == NullEquality::NullEqualsNothing
        && !j.null_aware)
}

/// A projection that only passes columns through, which the tree looks through.
fn passes(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Projection(p) if p.expr.iter().all(|e| matches!(e, Expr::Column(_))) => {
            Some(&p.input)
        }
        _ => None,
    }
}

/// The leaves of a tree of inner joins, in the order [`replace`] meets them, and the columns its
/// equalities join.
#[derive(Default)]
struct Tree<'a> {
    leaves: Vec<&'a LogicalPlan>,
    pairs: Vec<(Column, Column)>,
}

impl<'a> Tree<'a> {
    fn walk(&mut self, plan: &'a LogicalPlan) {
        if let Some(input) = passes(plan)
            && (inner(input) || passes(input).is_some())
        {
            return self.walk(input);
        }
        let LogicalPlan::Join(j) = plan else {
            self.leaves.push(plan);
            return;
        };
        if !inner(plan) {
            self.leaves.push(plan);
            return;
        }
        for (l, r) in &j.on {
            if let (Expr::Column(l), Expr::Column(r)) = (l, r) {
                self.pairs.push((l.clone(), r.clone()));
            }
        }
        self.walk(&j.left);
        self.walk(&j.right);
    }

    /// The one leaf whose output holds `c`.
    fn leaf_of(&self, c: &Column) -> Option<usize> {
        let mut found = None;
        for (i, l) in self.leaves.iter().enumerate() {
            if l.schema().has_column(c) {
                if found.is_some() {
                    return None;
                }
                found = Some(i);
            }
        }
        found
    }

    /// The first leaf to reduce, its column, and the dimension's keys to reduce it to.
    fn reduction(&self, config: &dyn OptimizerConfig) -> Result<Option<(usize, Column, LogicalPlan)>> {
        // Each pair's leaves, and the classes of columns the equalities join.
        let mut class = Classes::default();
        let mut edges = Vec::new();
        for (l, r) in &self.pairs {
            let (Some(a), Some(b)) = (self.leaf_of(l), self.leaf_of(r)) else {
                continue;
            };
            if a == b {
                continue;
            }
            class.union(l, r);
            edges.push((a, l, b, r));
        }
        for (n, leaf) in self.leaves.iter().enumerate() {
            if marked(leaf) || volatile(leaf) {
                continue;
            }
            // The filtered leaves `n` is joined to, and on which of its columns.
            let filters: Vec<(usize, &Column, &Column)> = edges
                .iter()
                .filter_map(|&(a, ca, b, cb)| {
                    if a == n {
                        Some((b, ca, cb))
                    } else if b == n {
                        Some((a, cb, ca))
                    } else {
                        None
                    }
                })
                .filter(|&(r, _, _)| filtered(self.leaves[r]) && !volatile(self.leaves[r]))
                .collect();
            if filters.is_empty() && !filtered(leaf) {
                continue;
            }
            let neighbours: Vec<usize> = edges
                .iter()
                .filter_map(|&(a, _, b, _)| (a == n).then_some(b).or((b == n).then_some(a)))
                .collect();
            // Each column of `n` that some equality joins, but not one it is filtered on.
            for &(a, ca, b, cb) in &edges {
                let k = match (a == n, b == n) {
                    (true, _) => ca,
                    (_, true) => cb,
                    _ => continue,
                };
                if filters.iter().any(|(_, on, _)| *on == k) {
                    continue;
                }
                let dimension = self.dimension(n, k, &filters, config)?;
                let Some(read) = bytes(&dimension) else {
                    continue;
                };
                for (t, c) in class.members(k).filter_map(|c| Some((self.leaf_of(c)?, c))) {
                    if t == n
                        || neighbours.contains(&t)
                        || filters.iter().any(|(r, _, _)| *r == t)
                        || marked(self.leaves[t])
                    {
                        continue;
                    }
                    let (Ok(kf), Ok(cf)) = (
                        leaf.schema().qualified_field_from_column(k),
                        self.leaves[t].schema().qualified_field_from_column(c),
                    ) else {
                        continue;
                    };
                    if kf.1.data_type() != cf.1.data_type() {
                        continue;
                    }
                    match bytes(self.leaves[t]) {
                        Some(b) if read.saturating_mul(SMALLER) <= b => {
                            return Ok(Some((t, c.clone(), dimension)));
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(None)
    }

    /// `n`'s column `k` over the rows of `n` that meet each of its filtered leaves, aliased and
    /// marked.
    fn dimension(
        &self,
        n: usize,
        k: &Column,
        filters: &[(usize, &Column, &Column)],
        config: &dyn OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut plan = self.leaves[n].clone();
        for &(r, on, other) in filters {
            plan = LogicalPlan::Join(Join::try_new(
                Arc::new(plan),
                Arc::new(self.leaves[r].clone()),
                vec![(Expr::Column(on.clone()), Expr::Column(other.clone()))],
                None,
                JoinType::LeftSemi,
                JoinConstraint::On,
                NullEquality::NullEqualsNothing,
                false,
            )?);
        }
        let keys = LogicalPlan::Projection(Projection::try_new(
            vec![Expr::Column(k.clone()).alias(KEY)],
            Arc::new(plan),
        )?);
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(keys),
            config.alias_generator().next(KEYS),
        )?))
    }
}

/// Classes of columns, by union and find.
#[derive(Default)]
struct Classes {
    parent: HashMap<Column, Column>,
}

impl Classes {
    fn find(&self, c: &Column) -> Column {
        let mut c = c.clone();
        while let Some(p) = self.parent.get(&c) {
            if *p == c {
                break;
            }
            c = p.clone();
        }
        c
    }

    fn union(&mut self, a: &Column, b: &Column) {
        for c in [a, b] {
            self.parent.entry(c.clone()).or_insert_with(|| c.clone());
        }
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.parent.insert(ra, rb);
        }
    }

    /// The columns in `c`'s class, in a fixed order.
    fn members(&self, c: &Column) -> impl Iterator<Item = &Column> {
        let root = self.find(c);
        let mut all: Vec<&Column> = self
            .parent
            .keys()
            .filter(|m| self.find(m) == root)
            .collect();
        all.sort_by_key(|m| m.flat_name());
        all.into_iter()
    }
}

/// `plan` with its `t`th leaf, in [`Tree::walk`]'s order, replaced by `with`'s of it.
fn replace(
    plan: &LogicalPlan,
    t: usize,
    seen: &mut usize,
    with: &mut dyn FnMut(&LogicalPlan) -> Result<LogicalPlan>,
) -> Result<LogicalPlan> {
    let descend = match passes(plan) {
        Some(input) => inner(input) || passes(input).is_some(),
        None => inner(plan),
    };
    if !descend {
        let i = *seen;
        *seen += 1;
        return if i == t { with(plan) } else { Ok(plan.clone()) };
    }
    let inputs = plan
        .inputs()
        .into_iter()
        .map(|i| replace(i, t, seen, with))
        .collect::<Result<Vec<_>>>()?;
    plan.with_new_exprs(plan.expressions(), inputs)
}

/// Whether `plan` filters its rows: a filter or a scan's own filters, beneath anything.
fn filtered(plan: &LogicalPlan) -> bool {
    plan.exists(|p| {
        Ok(match p {
            LogicalPlan::Filter(_) => true,
            LogicalPlan::TableScan(s) => !s.filters.is_empty(),
            _ => false,
        })
    })
    .unwrap_or(false)
}

/// Whether any expression in `plan` is volatile.
fn volatile(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|p| {
        p.apply_expressions(|e| {
            found |= e.is_volatile();
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        if let LogicalPlan::TableScan(s) = p {
            found |= s.filters.iter().any(Expr::is_volatile);
        }
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

/// Whether `plan` already holds a semi join this rule added.
fn marked(plan: &LogicalPlan) -> bool {
    plan.exists(|p| {
        Ok(matches!(p, LogicalPlan::SubqueryAlias(s) if s.alias.table().starts_with(KEYS)))
    })
    .unwrap_or(true)
}

fn bytes(plan: &LogicalPlan) -> Option<u64> {
    super::joinedgroups::bytes(plan)
}
