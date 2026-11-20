//! Walks that keep a plan's shared subtrees shared.
//!
//! A view is inlined as a copy of its root over the same `Arc`s, so every reference to it shares
//! what is below: the network nest's dearest statement is 1,080 distinct nodes in a tree of 6,458
//! (#347). DataFusion's own walks rebuild every node they pass, shared or not, and the copies are
//! analysed one by one from then on. These do the work once for each distinct node, and keep a
//! subtree that nothing changed as the `Arc` it was.
//!
//! What they are given must be a function of the node it is given and nothing else, the way a
//! bottom-up rule over `transform_up_with_subqueries` usually is: a node met again is not asked
//! again, it gets the answer its first meeting got.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::expr::{Exists, InSubquery, SetComparison};
use datafusion_expr::logical_plan::{Distinct, EmptyRelation, Subquery};
use datafusion_expr::{Expr, LogicalPlan};

/// Whether any expression of `p` holds a sub-expression `f` picks: a read, so a rule can pass over
/// a node it would not change without building anything for it.
pub(crate) fn holds(p: &LogicalPlan, f: impl Fn(&Expr) -> bool) -> bool {
    let mut found = false;
    let _ = p.apply_expressions(|e| {
        found = e.exists(|x| Ok(f(x)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

/// The schema `p`'s expressions are typed against: its one input's, shared, or its inputs' merged.
pub(crate) fn input_schema(p: &LogicalPlan) -> DFSchemaRef {
    match p.inputs().as_slice() {
        [one] => Arc::clone(one.schema()),
        all => {
            let mut schema = DFSchema::empty();
            for i in all {
                schema.merge(i.schema());
            }
            Arc::new(schema)
        }
    }
}

/// `f` over every node of `plan` and of the subqueries in its expressions, bottom up, as
/// `LogicalPlan::transform_up_with_subqueries`: a node is given to `f` with its inputs and
/// subqueries already done. `f` is also told whether any of those came back with another schema.
pub(crate) fn transform_up(
    plan: LogicalPlan,
    f: impl FnMut(LogicalPlan, bool) -> Result<Transformed<LogicalPlan>>,
) -> Result<Transformed<LogicalPlan>> {
    let memo = Memo::default();
    let mut walk = Up {
        f,
        subqueries: true,
        memo: &memo,
    };
    let (plan, transformed) = walk.node(plan)?;
    Ok(Transformed::new_transformed(plan, transformed))
}

/// As [`transform_up`], over the inputs alone, as `LogicalPlan::transform_up`: a subquery in an
/// expression is `f`'s to walk, and walks it again with `memo` it shares the answers so far.
pub(crate) fn transform_up_inputs(
    plan: LogicalPlan,
    memo: &Memo,
    f: impl FnMut(LogicalPlan, bool) -> Result<Transformed<LogicalPlan>>,
) -> Result<Transformed<LogicalPlan>> {
    let mut walk = Up {
        f,
        subqueries: false,
        memo,
    };
    let (plan, transformed) = walk.node(plan)?;
    Ok(Transformed::new_transformed(plan, transformed))
}

/// `f` over every node, top down, as `LogicalPlan::transform_down_with_subqueries`; a node `f`
/// answers with [`TreeNodeRecursion::Jump`] has its inputs left as `f` made them.
pub(crate) fn transform_down(
    plan: LogicalPlan,
    f: impl FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
) -> Result<Transformed<LogicalPlan>> {
    let memo = Memo::default();
    let mut walk = Down { f, memo: &memo };
    let (plan, transformed) = walk.node(plan)?;
    Ok(Transformed::new_transformed(plan, transformed))
}

/// The answer for each shared node met, by its address. The original is held so that address
/// is not given to another node while the walk runs.
#[derive(Default)]
pub(crate) struct Memo(RefCell<HashMap<*const LogicalPlan, Answer>>);

/// A shared node as it was, and as the walk left it.
type Answer = (Arc<LogicalPlan>, Arc<LogicalPlan>);

pub(crate) static PLACEHOLDER: LazyLock<Arc<LogicalPlan>> = LazyLock::new(|| {
    Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(datafusion_common::DFSchema::empty()),
    }))
});

/// The inputs of `p` as the `Arc`s it holds, in `LogicalPlan::inputs` order; `None` for a node
/// whose inputs are not reachable that way.
pub(crate) fn inputs_mut(p: &mut LogicalPlan) -> Option<Vec<&mut Arc<LogicalPlan>>> {
    use LogicalPlan as P;
    Some(match p {
        P::Projection(x) => vec![&mut x.input],
        P::Filter(x) => vec![&mut x.input],
        P::Window(x) => vec![&mut x.input],
        P::Aggregate(x) => vec![&mut x.input],
        P::Sort(x) => vec![&mut x.input],
        P::Join(x) => vec![&mut x.left, &mut x.right],
        P::Repartition(x) => vec![&mut x.input],
        P::Union(x) => x.inputs.iter_mut().collect(),
        P::Subquery(x) => vec![&mut x.subquery],
        P::SubqueryAlias(x) => vec![&mut x.input],
        P::Limit(x) => vec![&mut x.input],
        P::Explain(x) => vec![&mut x.plan],
        P::Analyze(x) => vec![&mut x.input],
        P::Distinct(Distinct::All(input)) => vec![input],
        P::Distinct(Distinct::On(x)) => vec![&mut x.input],
        P::Dml(x) => vec![&mut x.input],
        P::Copy(x) => vec![&mut x.input],
        P::Unnest(x) => vec![&mut x.input],
        P::RecursiveQuery(x) => vec![&mut x.static_term, &mut x.recursive_term],
        P::TableScan(_) | P::EmptyRelation(_) | P::Values(_) | P::DescribeTable(_) => vec![],
        P::Extension(_) | P::Statement(_) | P::Ddl(_) => return None,
    })
}

/// `p` with the plan of each subquery in its expressions passed through `walk`, which is given
/// it as a `LogicalPlan::Subquery`, as `LogicalPlan::map_subqueries` gives it.
fn map_subqueries(
    p: LogicalPlan,
    mut walk: impl FnMut(Subquery) -> Result<(Subquery, bool)>,
) -> Result<Transformed<LogicalPlan>> {
    let mut found = false;
    p.apply_expressions(|e| {
        found = e.exists(|e| {
            Ok(matches!(
                e,
                Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::SetComparison(_)
                    | Expr::ScalarSubquery(_)
            ))
        })?;
        Ok(match found {
            true => TreeNodeRecursion::Stop,
            false => TreeNodeRecursion::Continue,
        })
    })?;
    if !found {
        return Ok(Transformed::no(p));
    }
    p.map_expressions(|e| {
        e.transform_down(|e| {
            Ok(match e {
                Expr::ScalarSubquery(s) => {
                    let (s, t) = walk(s)?;
                    Transformed::new_transformed(Expr::ScalarSubquery(s), t)
                }
                Expr::Exists(Exists { subquery, negated }) => {
                    let (subquery, t) = walk(subquery)?;
                    Transformed::new_transformed(Expr::Exists(Exists { subquery, negated }), t)
                }
                Expr::InSubquery(InSubquery {
                    expr,
                    subquery,
                    negated,
                }) => {
                    let (subquery, t) = walk(subquery)?;
                    let e = Expr::InSubquery(InSubquery {
                        expr,
                        subquery,
                        negated,
                    });
                    Transformed::new_transformed(e, t)
                }
                Expr::SetComparison(SetComparison {
                    expr,
                    subquery,
                    op,
                    quantifier,
                }) => {
                    let (subquery, t) = walk(subquery)?;
                    let e = Expr::SetComparison(SetComparison {
                        expr,
                        subquery,
                        op,
                        quantifier,
                    });
                    Transformed::new_transformed(e, t)
                }
                e => Transformed::no(e),
            })
        })
    })
}

fn as_subquery(p: LogicalPlan) -> Result<Subquery> {
    match p {
        LogicalPlan::Subquery(s) => Ok(s),
        _ => datafusion_common::internal_err!("a subquery's walk must return a subquery"),
    }
}

trait Walk {
    fn memo(&self) -> &Memo;
    fn node(&mut self, p: LogicalPlan) -> Result<(LogicalPlan, bool)>;

    /// `input` as [`Walk::node`] leaves it, and whether that is another plan; a shared one is
    /// done once.
    fn arc(&mut self, input: Arc<LogicalPlan>) -> Result<(Arc<LogicalPlan>, bool)> {
        let key = Arc::as_ptr(&input);
        if let Some((held, done)) = self.memo().0.borrow().get(&key) {
            return Ok((Arc::clone(done), !Arc::ptr_eq(held, done)));
        }
        match Arc::try_unwrap(input) {
            Ok(p) => {
                let (p, transformed) = self.node(p)?;
                Ok((Arc::new(p), transformed))
            }
            Err(held) => {
                let (p, transformed) = self.node(held.as_ref().clone())?;
                let done = match transformed {
                    true => Arc::new(p),
                    false => Arc::clone(&held),
                };
                self.memo()
                    .0
                    .borrow_mut()
                    .insert(key, (held, Arc::clone(&done)));
                Ok((done, transformed))
            }
        }
    }

    /// `p` with each input walked, whether any came back changed, and whether any came back
    /// with another schema.
    fn inputs(&mut self, mut p: LogicalPlan) -> Result<(LogicalPlan, bool, bool)> {
        let (mut transformed, mut moved) = (false, false);
        match inputs_mut(&mut p) {
            Some(inputs) => {
                for input in inputs {
                    let held = std::mem::replace(input, Arc::clone(&PLACEHOLDER));
                    let before = Arc::clone(held.schema());
                    let (next, t) = self.arc(held)?;
                    transformed |= t;
                    moved |= t && *next.schema() != before;
                    *input = next;
                }
            }
            None => {
                let t = p.map_children(|c| {
                    let before = Arc::clone(c.schema());
                    let (c, t) = self.node(c)?;
                    moved |= t && *c.schema() != before;
                    Ok(Transformed::new_transformed(c, t))
                })?;
                transformed = t.transformed;
                p = t.data;
            }
        }
        Ok((p, transformed, moved))
    }
}

struct Up<'m, F> {
    f: F,
    subqueries: bool,
    memo: &'m Memo,
}

impl<F: FnMut(LogicalPlan, bool) -> Result<Transformed<LogicalPlan>>> Walk for Up<'_, F> {
    fn memo(&self) -> &Memo {
        self.memo
    }

    #[recursive::recursive]
    fn node(&mut self, p: LogicalPlan) -> Result<(LogicalPlan, bool)> {
        let mut below = false;
        let t = match self.subqueries {
            true => map_subqueries(p, |s| {
                let before = Arc::clone(s.subquery.schema());
                let (n, t) = self.node(LogicalPlan::Subquery(s))?;
                let s = as_subquery(n)?;
                below |= t && *s.subquery.schema() != before;
                Ok((s, t))
            })?,
            false => Transformed::no(p),
        };
        let (p, transformed, moved) = self.inputs(t.data)?;
        let u = (self.f)(p, below || moved)?;
        Ok((u.data, t.transformed || transformed || u.transformed))
    }
}

struct Down<'m, F> {
    f: F,
    memo: &'m Memo,
}

impl<F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>> Walk for Down<'_, F> {
    fn memo(&self) -> &Memo {
        self.memo
    }

    #[recursive::recursive]
    fn node(&mut self, p: LogicalPlan) -> Result<(LogicalPlan, bool)> {
        let t = (self.f)(p)?;
        match t.tnr {
            TreeNodeRecursion::Continue => {}
            TreeNodeRecursion::Jump => return Ok((t.data, t.transformed)),
            TreeNodeRecursion::Stop => {
                return datafusion_common::internal_err!("a shared walk does not stop");
            }
        }
        let s = map_subqueries(t.data, |s| {
            let (n, t) = self.node(LogicalPlan::Subquery(s))?;
            Ok((as_subquery(n)?, t))
        })?;
        let (p, transformed, _) = self.inputs(s.data)?;
        Ok((p, t.transformed || s.transformed || transformed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};

    /// A cross join of one shared subtree under two aliases.
    fn twice() -> LogicalPlan {
        let x = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1).alias("a")])
            .unwrap()
            .filter(col("a").gt(lit(0)))
            .unwrap()
            .build()
            .unwrap();
        let x = Arc::new(x);
        let side = |name: &str| {
            LogicalPlan::SubqueryAlias(
                datafusion_expr::logical_plan::SubqueryAlias::try_new(Arc::clone(&x), name)
                    .unwrap(),
            )
        };
        LogicalPlanBuilder::from(side("l"))
            .cross_join(side("r"))
            .unwrap()
            .build()
            .unwrap()
    }

    fn sides_share(p: &LogicalPlan) -> bool {
        let LogicalPlan::Join(j) = p else {
            panic!("{p}")
        };
        match (j.left.as_ref(), j.right.as_ref()) {
            (LogicalPlan::SubqueryAlias(l), LogicalPlan::SubqueryAlias(r)) => {
                Arc::ptr_eq(&l.input, &r.input)
            }
            _ => panic!("{p}"),
        }
    }

    #[test]
    fn a_shared_input_is_walked_once_and_stays_shared() {
        let mut seen = 0;
        let out = transform_up(twice(), |p, _| {
            seen += 1;
            Ok(Transformed::no(p))
        })
        .unwrap();
        // The join, its two aliases, and the filter, projection and relation under them once.
        assert_eq!(seen, 6);
        assert!(sides_share(&out.data));
    }

    #[test]
    fn a_shared_input_changed_is_one_input_still() {
        let out = transform_down(twice(), |p| match p {
            LogicalPlan::EmptyRelation(mut e) => {
                e.produce_one_row = false;
                Ok(Transformed::yes(LogicalPlan::EmptyRelation(e)))
            }
            p => Ok(Transformed::no(p)),
        })
        .unwrap();
        assert!(out.transformed);
        assert!(sides_share(&out.data));
    }

    /// What `docs/upstream/datafusion-walks-copy-shared-subtrees.md` says of DataFusion's walk.
    #[test]
    fn datafusion_walks_each_copy_and_unshares_them() {
        let mut seen = 0;
        let out = twice()
            .transform_up(|p| {
                seen += 1;
                Ok(Transformed::no(p))
            })
            .unwrap();
        assert_eq!(seen, 9);
        assert!(!sides_share(&out.data));
    }
}
