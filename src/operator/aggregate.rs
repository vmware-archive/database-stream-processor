//! Aggregation operators.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    algebra::{GroupValue, IndexedZSet, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    layers::{Builder, Cursor, MergeBuilder, Trie, TupleBuilder},
    operator::{BinaryOperatorAdapter, UnaryOperatorAdapter},
    NumEntries, SharedRef,
};

impl<P, SR> Stream<Circuit<P>, SR>
where
    P: Clone + 'static,
    SR: SharedRef + 'static,
{
    /// Apply [`Aggregate`] operator to `self`.
    pub fn aggregate<VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        <SR as SharedRef>::Target: Trie,
        F: Fn(&<<SR as SharedRef>::Target as Trie>::Key,
              &<<<SR as SharedRef>::Target as Trie>::Cursor as Cursor>::ValueStorage,
              <<<<SR as SharedRef>::Target as Trie>::Cursor as Cursor>::ValueStorage as Trie>::Cursor) -> VO + 'static,
        VO: 'static,
        W: ZRingValue,
        O: Clone + Trie<Item = (VO, W)> + 'static,
    {
        self.circuit()
            .add_unary_operator(<UnaryOperatorAdapter<O, _>>::new(Aggregate::new(f)), self)
    }

    /// Incremental version of the [`Aggregate`] operator.
    ///
    /// This is equivalent to `self.integrate().aggregate(f).differentiate()`,
    /// but is more efficient.
    pub fn aggregate_incremental<VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        <SR as SharedRef>::Target: IndexedZSet,
        F: Fn(&<<SR as SharedRef>::Target as Trie>::Key,
              &<<<SR as SharedRef>::Target as Trie>::Cursor as Cursor>::ValueStorage,
              <<<<SR as SharedRef>::Target as Trie>::Cursor as Cursor>::ValueStorage as Trie>::Cursor) -> VO + 'static,
        VO: 'static,
        W: ZRingValue,
        O: Clone + Trie<Item = (VO, W)> + 'static,
    {
        self.circuit().add_binary_operator(
            BinaryOperatorAdapter::new(AggregateIncremental::new(f)),
            self,
            &self.integrate().delay(),
        )
    }

    /*
    /// A version of [`Self::aggregate_incremental`] optimized for linear
    /// aggregation functions.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`.  It will produce
    /// incorrect results if `f` is not linear.
    ///
    /// Note that this method adds the value of the key from the input indexed
    /// Z-set to the output Z-set, i.e., given an input key-value pair `(k,
    /// v)`, the output Z-set contains value `(k, f(k, v))`.  In contrast,
    /// [`Self::aggregate_incremental`] does not automatically include key
    /// in the output, since a user-defined aggregation function can be
    /// designed to return the key if necessar.  However,
    /// such an aggregation function can be non-linear (in fact, the plus
    /// operation may not even be defined for its output type).
    pub fn aggregate_linear_incremental<K, VI, VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        K: KeyProperties,
        VI: GroupValue,
        SR: SharedRef + 'static,
        <SR as SharedRef>::Target: FiniteMap<K, VI>,
        <SR as SharedRef>::Target: NumEntries + SharedRef<Target = SR::Target>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: GroupValue + NumEntries,
        W: ZRingValue,
        O: Clone + MapBuilder<(K, VO), W> + 'static,
    {
        let agg_delta: Stream<_, OrdFiniteMap<K, VO>> = self.map_values(f);
        agg_delta.aggregate_incremental(|key, agg_val| (key.clone(), agg_val.clone()))
    }
    */

    /// Incremental nested version of the [`Aggregate`] operator.
    ///
    /// This is equivalent to
    /// `self.integrate().integrate_nested().aggregate(f).differentiate_nested.
    /// differentiate()`, but is more efficient.
    pub fn aggregate_incremental_nested<VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        <SR as SharedRef>::Target: IndexedZSet,
        F: Fn(&<<SR as SharedRef>::Target as Trie>::Key,
              &<<<SR as SharedRef>::Target as Trie>::Cursor as Cursor>::ValueStorage,
              <<<<SR as SharedRef>::Target as Trie>::Cursor as Cursor>::ValueStorage as Trie>::Cursor) -> VO + 'static,
        VO: 'static,
        W: ZRingValue,
        O: Clone + Trie<Item = (VO, W)> + NumEntries + GroupValue + 'static,
    {
        self.integrate_nested()
            .aggregate_incremental(f)
            .differentiate_nested()
    }

    /*
    /// A version of [`Self::aggregate_incremental_nested`] optimized for linear
    /// aggregation functions.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`.  It will produce
    /// incorrect results if `f` is not linear.
    pub fn aggregate_linear_incremental_nested<K, VI, VO, W, F, O>(
        &self,
        f: F,
    ) -> Stream<Circuit<P>, O>
    where
        K: KeyProperties,
        VI: GroupValue,
        SR: SharedRef + 'static,
        <SR as SharedRef>::Target: FiniteMap<K, VI>,
        <SR as SharedRef>::Target: NumEntries + SharedRef<Target = SR::Target>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: NumEntries + GroupValue,
        W: ZRingValue,
        O: Clone + MapBuilder<(K, VO), W> + NumEntries + GroupValue,
    {
        self.integrate_nested()
            .aggregate_linear_incremental(f)
            .differentiate_nested()
    }
    */
}

/// Aggregate each indexed Z-set in the input stream.
///
/// Values in the input stream are finite maps that map keys of type
/// `K` to values of type `VI`.  The aggregation function `agg_func`
/// maps each key-value pair into an output value of type `VO`.  The
/// output of the operator is a Z-set of type `O` computed as:
/// `Aggregate(i) = Sum_{(k,v) in i}(+1 x agg_func(k,v))`
///
/// # Type arguments
///
/// * `I` - input map type.
/// * `VO` - output type of the aggregation function; value type in the output
///   Z-set.
/// * `W` - weight type in the output Z-set.
/// * `O` - output Z-set type.
pub struct Aggregate<I, VO, W, F, O> {
    agg_func: F,
    _type: PhantomData<(I, VO, W, O)>,
}

impl<I, VO, W, F, O> Aggregate<I, VO, W, F, O> {
    pub fn new(agg_func: F) -> Self {
        Self {
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<I, VO, W, F, O> Operator for Aggregate<I, VO, W, F, O>
where
    I: 'static,
    VO: 'static,
    W: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Aggregate")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<I, VO, W, F, O> UnaryOperator<I, O> for Aggregate<I, VO, W, F, O>
where
    I: Trie + 'static,
    F: Fn(
            &I::Key,
            &<I::Cursor as Cursor>::ValueStorage,
            <<I::Cursor as Cursor>::ValueStorage as Trie>::Cursor,
        ) -> VO
        + 'static,
    VO: 'static,
    W: ZRingValue,
    O: Clone + Trie<Item = (VO, W)> + 'static,
{
    fn eval(&mut self, i: &I) -> O {
        let mut builder = O::TupleBuilder::with_capacity(i.keys());
        let mut cursor = i.cursor();

        while cursor.valid(i) {
            let key = cursor.key(i);
            let (val_storage, val_cursor) = cursor.values(i);
            builder.push_tuple(((self.agg_func)(key, val_storage, val_cursor), W::one()));
            cursor.step(i);
        }

        builder.done()
    }
}

/// Incremental version of the `Aggregate` operator.
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `integrate(A) - integrate(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
pub struct AggregateIncremental<I, VO, W, F, O> {
    agg_func: F,
    _type: PhantomData<(I, VO, W, O)>,
}

impl<I, VO, W, F, O> AggregateIncremental<I, VO, W, F, O> {
    pub fn new(agg_func: F) -> Self {
        Self {
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<I, VO, W, F, O> Operator for AggregateIncremental<I, VO, W, F, O>
where
    I: 'static,
    VO: 'static,
    W: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AggregateIncremental")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<I, VO, W, F, O> BinaryOperator<I, I, O> for AggregateIncremental<I, VO, W, F, O>
where
    I: Trie + 'static,
    F: Fn(
            &I::Key,
            &<I::Cursor as Cursor>::ValueStorage,
            <<I::Cursor as Cursor>::ValueStorage as Trie>::Cursor,
        ) -> VO
        + 'static,
    VO: 'static,
    W: ZRingValue,
    O: Clone + Trie<Item = (VO, W)> + 'static,
{
    fn eval(&mut self, delta: &I, delayed_integral: &I) -> O {
        let mut result_builder = O::TupleBuilder::with_capacity(delta.keys());

        let mut delta_cursor = delta.cursor();
        let mut delayed_integral_cursor = delayed_integral.cursor();

        while delta_cursor.valid(delta) {
            let key = delta_cursor.key(delta);
            let (val_storage, val_cursor) = delta_cursor.values(delta);

            delayed_integral_cursor.seek(delayed_integral, key);

            if delayed_integral_cursor.valid(delayed_integral)
                && delayed_integral_cursor.key(delayed_integral) == key
            {
                // Retract the old value of the aggregate.
                let (old_storage, old_cursor) = delayed_integral_cursor.values(delayed_integral);
                result_builder.push_tuple((
                    (self.agg_func)(key, old_storage, old_cursor),
                    W::one().neg(),
                ));

                let (old_storage, old_cursor) = delayed_integral_cursor.values(delayed_integral);
                let mut builder =
                    <<I::Cursor as Cursor>::ValueStorage as Trie>::MergeBuilder::with_capacity(
                        val_cursor.keys() + old_cursor.keys(),
                        0,
                    );
                builder.push_merge((val_storage, val_cursor), (old_storage, old_cursor));
                let new_storage = builder.done();
                // Insert updated aggregate.
                if new_storage.keys() > 0 {
                    result_builder.push_tuple((
                        (self.agg_func)(key, &new_storage, new_storage.cursor()),
                        W::one(),
                    ))
                }
            } else {
                result_builder
                    .push_tuple(((self.agg_func)(key, val_storage, val_cursor), W::one()));
            }

            delta_cursor.step(delta);
        }

        result_builder.done()
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, rc::Rc};

    use crate::{
        algebra::{OrdFiniteMap, OrdIndexedZSet},
        circuit::{Root, Stream},
        finite_map,
        layers::{
            ordered_leaf::{OrderedLeaf, OrderedLeafCursor},
            Cursor,
        },
        operator::{Apply2, GeneratorNested},
    };

    #[test]
    fn aggregate_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    finite_map! { (1, 10) => 1, (1, 20) => 1 },
                    finite_map! { (2, 10) => 1, (1, 10) => -1, (1, 20) => 1, (3, 10) => 1 },
                ],
                vec![
                    finite_map! { (4, 20) => 1, (2, 10) => -1 },
                    finite_map! { (5, 10) => 1, (6, 10) => 1 },
                ],
                vec![],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input: Stream<_, OrdIndexedZSet<usize, usize, isize>> = child
                        .add_source(GeneratorNested::new(Box::new(move || {
                            *counter_clone.borrow_mut() = 0;
                            let mut deltas = inputs.next().unwrap_or_else(Vec::new).into_iter();
                            Box::new(move || deltas.next().unwrap_or_else(|| finite_map! {}))
                        })))
                        .index();

                    /*
                    // Weighted sum aggregate.  Returns `(key, weighted_sum)`.
                    let sum = |&key: &usize, storage: &OrderedLeaf<usize, isize>, mut cursor: OrderedLeafCursor<usize, isize>| -> (usize, isize) {
                        let mut result: isize = 0;

                        while cursor.valid(storage) {
                            let &(v, w) = cursor.key(storage);
                            result += (v as isize) * w;
                            cursor.step(storage);
                        }

                        (key, result)
                    };

                    // Weighted sum aggregate that returns only the weighted sum
                    // value and is therefore linear.
                    /*let sum_linear = |_key: &usize, zset: &OrdZSet<usize, isize>| -> isize {
                        let mut result: isize = 0;
                        for (v, w) in zset.into_iter() {
                            result += (*v as isize) * w;
                        }

                        result
                    };*/

                    let sum_inc = input.aggregate_incremental_nested(sum);
                    //let sum_inc_linear = input.aggregate_linear_incremental_nested(sum_linear);
                    let sum_noninc = input
                        .integrate_nested()
                        .integrate()
                        .aggregate(sum)
                        .differentiate()
                        .differentiate_nested();

                    // Compare outputs of all three implementations.
                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &OrdFiniteMap<(usize, isize), isize>,
                                 d2: &OrdFiniteMap<(usize, isize), isize>| {
                                    (d1.clone(), d2.clone())
                                },
                            ),
                            &sum_inc,
                            &sum_noninc,
                        )
                        .inspect(|(d1, d2)| {
                            //println!("incremental: {:?}", d1);
                            //println!("non-incremental: {:?}", d2);
                            assert_eq!(d1, d2);
                        });
                    */

                    /*child
                    .add_binary_operator(
                        Apply2::new(
                            |d1: &OrdFiniteMap<(usize, isize), isize>,
                             d2: &OrdFiniteMap<(usize, isize), isize>| {
                                (d1.clone(), d2.clone())
                            },
                        ),
                        &sum_inc,
                        &sum_inc_linear,
                    )
                    .inspect(|(d1, d2)| {
                        assert_eq!(d1, d2);
                    });*/

                    // Min aggregate (non-linear).
                    let min = |&key: &usize,
                               storage: &OrderedLeaf<usize, isize>,
                               mut cursor: OrderedLeafCursor<usize, isize>|
                     -> (usize, usize) {
                        let mut result = usize::MAX;

                        while cursor.valid(storage) {
                            let &(v, _) = cursor.key(storage);
                            if v < result {
                                result = v;
                            }
                            cursor.step(storage);
                        }

                        (key, result)
                    };

                    let min_inc = input.aggregate_incremental_nested(min);
                    let min_noninc = input
                        .integrate_nested()
                        .integrate()
                        .aggregate(min)
                        .differentiate()
                        .differentiate_nested();

                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &OrdFiniteMap<(usize, usize), isize>,
                                 d2: &OrdFiniteMap<(usize, usize), isize>| {
                                    (d1.clone(), d2.clone())
                                },
                            ),
                            &min_inc,
                            &min_noninc,
                        )
                        .inspect(|(d1, d2)| {
                            assert_eq!(d1, d2);
                        });

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
