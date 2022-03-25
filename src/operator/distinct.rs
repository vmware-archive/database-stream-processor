//! Distinct operator.

use std::{borrow::Cow, marker::PhantomData, ops::Neg};

use crate::{
    algebra::{HasOne, HasZero, ZRingValue, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, NodeId, Scope, Stream,
    },
    circuit_cache_key,
    layers::{Builder, Cursor, TupleBuilder},
    operator::{BinaryOperatorAdapter, UnaryOperatorAdapter},
    NumEntries, SharedRef,
};

circuit_cache_key!(DistinctId<C, D>(NodeId => Stream<C, D>));
circuit_cache_key!(DistinctIncrementalId<C, D>(NodeId => Stream<C, D>));

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
{
    /// Apply [`Distinct`] operator to `self`.
    pub fn distinct(&self) -> Stream<Circuit<P>, <Z as SharedRef>::Target>
    where
        Z: SharedRef + 'static,
        <Z as SharedRef>::Target: ZSet,
    {
        self.circuit()
            .cache_get_or_insert_with(DistinctId::new(self.local_node_id()), || {
                self.circuit()
                    .add_unary_operator(UnaryOperatorAdapter::new(Distinct::new()), self)
            })
            .clone()
    }

    /// Incremental version of the [`Distinct`] operator.
    ///
    /// This is equivalent to `self.integrate().distinct().differentiate()`, but
    /// is more efficient.
    pub fn distinct_incremental(&self) -> Stream<Circuit<P>, <Z as SharedRef>::Target>
    where
        Z: SharedRef + 'static,
        <Z as SharedRef>::Target: NumEntries + ZSet + SharedRef<Target = Z::Target>,
    {
        self.circuit()
            .cache_get_or_insert_with(DistinctIncrementalId::new(self.local_node_id()), || {
                self.circuit().add_binary_operator(
                    BinaryOperatorAdapter::new(DistinctIncremental::new()),
                    self,
                    &self.integrate().delay(),
                )
            })
            .clone()
    }

    /// Incremental nested version of the [`Distinct`] operator.
    pub fn distinct_incremental_nested(&self) -> Stream<Circuit<P>, <Z as SharedRef>::Target>
    where
        Z: SharedRef + 'static,
        <Z as SharedRef>::Target: NumEntries + ZSet + SharedRef<Target = Z::Target>,
    {
        self.integrate_nested()
            .distinct_incremental()
            .differentiate_nested()
    }
}

/// `Distinct` operator changes all weights in the support of a Z-set to 1.
pub struct Distinct<Z> {
    _type: PhantomData<Z>,
}

impl<Z> Distinct<Z> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<Z> Default for Distinct<Z> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Z> Operator for Distinct<Z>
where
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Distinct")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<Z> UnaryOperator<Z, Z> for Distinct<Z>
where
    Z: ZSet,
{
    fn eval(&mut self, i: &Z) -> Z {
        i.distinct()
    }

    fn eval_owned(&mut self, i: Z) -> Z {
        i.distinct_owned()
    }
}

/// Incremental version of the distinct operator.
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `distinct(A) - distinct(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
struct DistinctIncremental<Z> {
    _type: PhantomData<Z>,
}

impl<Z> DistinctIncremental<Z> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<Z> Default for DistinctIncremental<Z> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Z> Operator for DistinctIncremental<Z>
where
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("DistinctIncremental")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<Z> BinaryOperator<Z, Z, Z> for DistinctIncremental<Z>
where
    Z: ZSet,
{
    fn eval(&mut self, delta: &Z, delayed_integral: &Z) -> Z {
        let mut builder = Z::TupleBuilder::with_capacity(delta.keys());
        let mut delta_cursor = delta.cursor();
        let mut integral_cursor = delayed_integral.cursor();

        while delta_cursor.valid(delta) {
            let vw = delta_cursor.key(delta);
            let (v, w) = vw;
            integral_cursor.seek(delayed_integral, vw);
            let old_weight = if integral_cursor.valid(delayed_integral)
                && integral_cursor.key(delayed_integral).0 == *v
            {
                integral_cursor.key(delayed_integral).1.clone()
            } else {
                Z::Value::zero()
            };

            let new_weight = old_weight.clone() + w.clone();

            if old_weight.le0() {
                // Weight changes from non-positive to positive.
                if new_weight.ge0() && !new_weight.is_zero() {
                    builder.push_tuple((v.clone(), Z::Value::one()));
                }
            } else if new_weight.le0() {
                // Weight changes from positive to non-positive.
                builder.push_tuple((v.clone(), Z::Value::one().neg()));
            }
            delta_cursor.step(delta);
        }

        builder.done()
    }

    // TODO: owned implementation.
    fn eval_owned_and_ref(&mut self, delta: Z, delayed_integral: &Z) -> Z {
        self.eval(&delta, delayed_integral)
    }

    fn eval_owned(&mut self, delta: Z, delayed_integral: Z) -> Z {
        self.eval_owned_and_ref(delta, &delayed_integral)
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, rc::Rc};

    use crate::{
        algebra::OrdFiniteMap,
        circuit::Root,
        finite_map,
        operator::{Apply2, GeneratorNested},
    };

    #[test]
    fn distinct_incremental_nested_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    finite_map! { 1 => 1, 2 => 1 },
                    finite_map! { 2 => -1, 3 => 2, 4 => 2 },
                ],
                vec![
                    finite_map! { 2 => 1, 3 => 1 },
                    finite_map! { 3 => -2, 4 => -1 },
                ],
                vec![
                    finite_map! { 5 => 1, 6 => 1 },
                    finite_map! { 2 => -1, 7 => 1 },
                    finite_map! { 2 => 1, 7 => -1, 8 => 2, 9 => 1 },
                ],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input = child.add_source(GeneratorNested::new(Box::new(move || {
                        *counter_clone.borrow_mut() = 0;
                        let mut deltas = inputs.next().unwrap_or_else(Vec::new).into_iter();
                        Box::new(move || deltas.next().unwrap_or_else(|| finite_map! {}))
                    })));

                    let distinct_inc = input.distinct_incremental_nested();
                    let distinct_noninc = input
                        // Non-incremental implementation of distinct_nested_incremental.
                        .integrate()
                        .integrate_nested()
                        .distinct()
                        .differentiate()
                        .differentiate_nested();

                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &OrdFiniteMap<usize, isize>,
                                 d2: &OrdFiniteMap<usize, isize>| {
                                    (d1.clone(), d2.clone())
                                },
                            ),
                            &distinct_inc,
                            &distinct_noninc,
                        )
                        .inspect(|(d1, d2)| assert_eq!(d1, d2));

                    Ok((
                        move || {
                            *counter.borrow_mut() += 1;
                            *counter.borrow() == 4
                        },
                        (),
                    ))
                })
                .unwrap();
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}
