//! Relational join operator.

use crate::{
    algebra::{IndexedZSet, MulByRef, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
    layers::{Builder, Cursor, Trie, TupleBuilder},
    operator::BinaryOperatorAdapter,
    SharedRef,
};
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    marker::PhantomData,
};

impl<P, SR1> Stream<Circuit<P>, SR1>
where
    P: Clone + 'static,
{
    /// Apply [`Join`] operator to `self` and `other`.
    ///
    /// See [`Join`] operator for more info.
    pub fn join<V, F, SR2, Z>(&self, other: &Stream<Circuit<P>, SR2>, f: F) -> Stream<Circuit<P>, Z>
    where
        SR1: SharedRef + 'static,
        SR2: SharedRef + 'static,
        SR1::Target: IndexedZSet,
        SR2::Target: IndexedZSet<
            IndexKey = <SR1::Target as IndexedZSet>::IndexKey,
            Weight = <SR1::Target as IndexedZSet>::Weight,
        >,
        F: Fn(
                &<SR1::Target as IndexedZSet>::IndexKey,
                &<SR1::Target as IndexedZSet>::Value,
                &<SR2::Target as IndexedZSet>::Value,
            ) -> V
            + 'static,
        V: 'static,
        Z: Clone + Trie<Item = (V, <SR1::Target as IndexedZSet>::Weight)> + 'static,
    {
        self.circuit().add_binary_operator(
            <BinaryOperatorAdapter<Z, _>>::new(Join::new(f)),
            self,
            other,
        )
    }
}

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
{
    /// Incremental join of two streams.
    ///
    /// Given streams `a` and `b` of changes to relations `A` and `B`
    /// respectively, computes a stream of changes to `A <> B` (where `<>`
    /// is the join operator):
    ///
    /// ```text
    /// delta(A <> B) = A <> B - z^-1(A) <> z^-1(B) = a <> z^-1(B) + z^-1(A) <> b + a <> b
    /// ```
    pub fn join_incremental<F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1: IndexedZSet,
        I2: IndexedZSet<IndexKey = I1::IndexKey, Weight = I1::Weight>,
        F: Clone + Fn(&I1::IndexKey, &I1::Value, &I2::Value) -> Z::Data + 'static,
        Z: ZSet<Weight = I1::Weight>,
    {
        self.integrate()
            .delay()
            .join(other, join_func.clone())
            .plus(&self.join(&other.integrate(), join_func))
    }

    /// Incremental join of two nested streams.
    ///
    /// Given nested streams `a` and `b` of changes to relations `A` and `B`,
    /// computes `(↑((↑(a <> b))∆))∆` using the following formula:
    ///
    /// ```text
    /// (↑((↑(a <> b))∆))∆ =
    ///     I(↑I(a)) <> b            +
    ///     ↑I(a) <> I(z^-1(b))      +
    ///     a <> I(↑I(↑z^-1(b)))     +
    ///     I(z^-1(a)) <> ↑I(↑z^-1(b)).
    /// ```
    pub fn join_incremental_nested<F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1: IndexedZSet,
        I2: IndexedZSet<IndexKey = I1::IndexKey, Weight = I1::Weight>,
        F: Clone + Fn(&I1::IndexKey, &I1::Value, &I2::Value) -> Z::Data + 'static,
        Z: ZSet<Weight = I1::Weight>,
    {
        let join1: Stream<_, Z> = self
            .integrate_nested()
            .integrate()
            .join(other, join_func.clone());
        let join2 = self
            .integrate()
            .join(&other.integrate_nested().delay_nested(), join_func.clone());
        let join3 = self.join(
            &other.integrate_nested().integrate().delay(),
            join_func.clone(),
        );
        let join4 = self
            .integrate_nested()
            .delay_nested()
            .join(&other.integrate().delay(), join_func);

        // Note: I would use `join.sum(...)` but for some reason
        //       Rust Analyzer tries to resolve it to `Iterator::sum()`
        Stream::sum(&join1, &[join2, join3, join4])
    }
}

/// Join two indexed Z-sets.
///
/// The operator takes two streams of indexed Z-sets and outputs
/// a stream obtained by joining each pair of inputs.
///
/// An indexed Z-set is a map from keys to a Z-set of values associated
/// with each key.  Both input streams must use the same key type `K`.
/// Indexed Z-sets are produced for example by the
/// [`Index`](`crate::operator::Index`) operator.
///
/// # Type arguments
///
/// * `V` - value type in the output Z-set.
/// * `F` - join function type: maps key and a pair of values from input Z-sets
///   to an output value.
/// * `I1` - indexed Z-set type in the first input stream.
/// * `I2` - indexed Z-set type in the second input stream.
/// * `Z` - output Z-set type.
pub struct Join<V, F, I1, I2, Z> {
    join_func: F,
    _types: PhantomData<(I1, I2, V, Z)>,
}

impl<V, F, I1, I2, Z> Join<V, F, I1, I2, Z> {
    pub fn new(join_func: F) -> Self {
        Self {
            join_func,
            _types: PhantomData,
        }
    }
}

impl<V, F, I1, I2, Z> Operator for Join<V, F, I1, I2, Z>
where
    I1: 'static,
    I2: 'static,
    F: 'static,
    V: 'static,
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Join")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<V, F, I1, I2, Z> BinaryOperator<I1, I2, Z> for Join<V, F, I1, I2, Z>
where
    I1: IndexedZSet,
    I2: IndexedZSet<IndexKey = I1::IndexKey, Weight = I1::Weight>,
    F: Fn(&I1::IndexKey, &I1::Value, &I2::Value) -> V + 'static,
    V: 'static,
    Z: Trie<Item = (V, I1::Weight)> + 'static,
{
    fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        let mut cursor1 = i1.cursor();
        let mut cursor2 = i2.cursor();

        // Choose capacity heuristically.
        let mut builder = Z::TupleBuilder::with_capacity(min(i1.tuples(), i2.tuples()));

        while cursor1.valid(i1) && cursor2.valid(i2) {
            match cursor1.key(i1).cmp(cursor2.key(i2)) {
                Ordering::Less => cursor1.seek(i1, cursor2.key(i2)),
                Ordering::Greater => cursor2.seek(i2, cursor1.key(i1)),
                Ordering::Equal => {
                    let (storage1, mut values1) = cursor1.values(i1);

                    while values1.valid(storage1) {
                        let (storage2, mut values2) = cursor2.values(i2);
                        while values2.valid(storage2) {
                            let (v1, w1) = values1.key(storage1);
                            let (v2, w2) = values2.key(storage2);
                            builder.push_tuple((
                                (self.join_func)(cursor1.key(i1), v1, v2),
                                w1.mul_by_ref(w2),
                            ));
                            values2.step(storage2);
                        }
                        values1.step(storage1);
                    }

                    cursor1.step(i1);
                    cursor2.step(i2);
                }
            }
        }

        builder.done()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{HasZero, OrdFiniteMap, OrdIndexedZSet},
        circuit::{Root, Stream},
        finite_map,
        operator::{DelayedFeedback, Generator},
    };
    use std::vec;

    #[test]
    fn join_test() {
        let root = Root::build(move |circuit| {
            let mut input1 = vec![
                finite_map! {
                    (1, "a") => 1,
                    (1, "b") => 2,
                    (2, "c") => 3,
                    (2, "d") => 4,
                    (3, "e") => 5,
                    (3, "f") => -2,
                },
                finite_map! {(1, "a") => 1},
                finite_map! {(1, "a") => 1},
                finite_map! {(4, "n") => 2},
                finite_map! {(1, "a") => 0},
            ]
            .into_iter();
            let mut input2 = vec![
                finite_map! {
                    (2, "g") => 3,
                    (2, "h") => 4,
                    (3, "i") => 5,
                    (3, "j") => -2,
                    (4, "k") => 5,
                    (4, "l") => -2,
                },
                finite_map! {(1, "b") => 1},
                finite_map! {(4, "m") => 1},
                finite_map! {},
                finite_map! {},
            ]
            .into_iter();
            let mut outputs = vec![
                finite_map! {
                    (2, "c g".to_string()) => 9,
                    (2, "c h".to_string()) => 12,
                    (2, "d g".to_string()) => 12,
                    (2, "d h".to_string()) => 16,
                    (3, "e i".to_string()) => 25,
                    (3, "e j".to_string()) => -10,
                    (3, "f i".to_string()) => -10,
                    (3, "f j".to_string()) => 4
                },
                finite_map! {
                    (1, "a b".to_string()) => 1,
                },
                finite_map! {},
                finite_map! {},
                finite_map! {},
            ]
            .into_iter();
            let mut inc_outputs = vec![
                finite_map! {
                    (2, "c g".to_string()) => 9,
                    (2, "c h".to_string()) => 12,
                    (2, "d g".to_string()) => 12,
                    (2, "d h".to_string()) => 16,
                    (3, "e i".to_string()) => 25,
                    (3, "e j".to_string()) => -10,
                    (3, "f i".to_string()) => -10,
                    (3, "f j".to_string()) => 4
                },
                finite_map! {
                    (1, "a b".to_string()) => 2,
                    (1, "b b".to_string()) => 2,
                },
                finite_map! {
                    (1, "a b".to_string()) => 1,
                },
                finite_map! {
                    (4, "n k".to_string()) => 10,
                    (4, "n l".to_string()) => -4,
                    (4, "n m".to_string()) => 2,
                },
                finite_map! {},
            ]
            .into_iter();

            let index1: Stream<_, OrdIndexedZSet<usize, &'static str, isize>> = circuit
                .add_source(Generator::new(move || input1.next().unwrap()))
                .index();
            let index2: Stream<_, OrdIndexedZSet<usize, &'static str, isize>> = circuit
                .add_source(Generator::new(move || input2.next().unwrap()))
                .index();
            index1
                .join(&index2, |&k: &usize, s1, s2| (k, format!("{} {}", s1, s2)))
                .inspect(move |fm: &OrdFiniteMap<(usize, String), _>| {
                    assert_eq!(fm, &outputs.next().unwrap())
                });
            index1
                .join_incremental(&index2, |&k: &usize, s1, s2| (k, format!("{} {}", s1, s2)))
                .inspect(move |fm: &OrdFiniteMap<(usize, String), _>| {
                    assert_eq!(fm, &inc_outputs.next().unwrap())
                });
        })
        .unwrap();

        for _ in 0..5 {
            root.step().unwrap();
        }
    }

    // Nested incremental reachability algorithm.
    #[test]
    fn join_incremental_nested_test() {
        let root = Root::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges: vec::IntoIter<OrdFiniteMap<(usize, usize), isize>> = vec![
                finite_map! { (1, 2) => 1 },
                finite_map! { (2, 3) => 1},
                finite_map! { (1, 3) => 1},
                finite_map! { (3, 1) => 1},
                finite_map! { (3, 1) => -1},
                finite_map! { (1, 2) => -1},
                finite_map! { (2, 4) => 1, (4, 1) => 1 },
                finite_map! { (2, 3) => -1, (3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let mut outputs: vec::IntoIter<OrdFiniteMap<(usize, usize), isize>> = vec![
                finite_map! { (1, 2) => 1 },
                finite_map! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (1, 2) => 1, (1, 3) => 1, (2, 3) => 1, (2, 1) => 1, (3, 1) => 1, (3, 2) => 1},
                finite_map! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (1, 3) => 1, (2, 3) => 1, (2, 4) => 1, (2, 1) => 1, (4, 1) => 1, (4, 3) => 1 },
                finite_map! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (4, 4) => 1,
                              (1, 2) => 1, (1, 3) => 1, (1, 4) => 1,
                              (2, 1) => 1, (2, 3) => 1, (2, 4) => 1,
                              (3, 1) => 1, (3, 2) => 1, (3, 4) => 1,
                              (4, 1) => 1, (4, 2) => 1, (4, 3) => 1 },
            ]
            .into_iter();

            let edges: Stream<_, OrdFiniteMap<(usize, usize), isize>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let paths = circuit.iterate_with_conditions(|child| {
                // ```text
                //                      distinct_incremental_nested
                //               ┌───┐          ┌───┐
                // edges         │   │          │   │  paths
                // ────┬────────►│ + ├──────────┤   ├────────┬───►
                //     │         │   │          │   │        │
                //     │         └───┘          └───┘        │
                //     │           ▲                         │
                //     │           │                         │
                //     │         ┌─┴─┐                       │
                //     │         │   │                       │
                //     └────────►│ X │ ◄─────────────────────┘
                //               │   │
                //               └───┘
                //      join_incremental_nested
                // ```
                let edges = edges.delta0(child);
                let paths_delayed = <DelayedFeedback<_, OrdFiniteMap<_, _>>>::new(child);

                let paths_inverted: Stream<_, OrdFiniteMap<(usize, usize), isize>> = paths_delayed
                    .stream()
                    .map_keys(|&(x, y)| (y, x));

                let paths_inverted_indexed: Stream<_, OrdIndexedZSet<usize, usize, isize>> = paths_inverted.index();
                let edges_indexed: Stream<_, OrdIndexedZSet<usize, usize, isize>> = edges.index();

                let paths = edges.plus(&paths_inverted_indexed.join_incremental_nested(&edges_indexed, |_via, from, to| (*from, *to)))
                    .distinct_incremental_nested();
                paths_delayed.connect(&paths);
                let output = paths.integrate();
                Ok((
                    vec![
                        paths.condition(HasZero::is_zero),
                        paths.integrate_nested().condition(HasZero::is_zero)
                    ],
                    output.export(),
                ))
            })
            .unwrap();

            paths.integrate().distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            })
        })
        .unwrap();

        for _ in 0..8 {
            root.step().unwrap();
        }
    }
}
