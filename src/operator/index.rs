//! Index operator.

use crate::{
    algebra::{IndexedZSet, ZRingValue},
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, NodeId, Scope, Stream,
    },
    circuit_cache_key,
    layers::{Builder, Cursor, OrderedLeaf, Trie, TupleBuilder},
    SharedRef,
};
use std::{borrow::Cow, marker::PhantomData};

circuit_cache_key!(IndexId<C, D>(NodeId => Stream<C, D>));

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: Clone,
    P: Clone + 'static,
{
    /// Apply [`Index`] operator to `self`.
    pub fn index<CO>(&self) -> Stream<Circuit<P>, CO>
    where
        CO: IndexedZSet + Clone,
        CI: Trie<Key = ((CO::IndexKey, CO::Value), CO::Weight)> + 'static,
    {
        self.circuit()
            .cache_get_or_insert_with(IndexId::new(self.local_node_id()), || {
                self.circuit().add_unary_operator(Index::new(), self)
            })
            .clone()
    }

    pub fn index_with<V1, W, CO, F>(&self, f: F) -> Stream<Circuit<P>, CO>
    where
        V1: Clone + 'static,
        W: ZRingValue,
        CI: SharedRef + 'static,
        CI::Target: Trie<Key = (V1, W)> + 'static,
        CO: IndexedZSet<Weight = W> + Clone,
        F: Fn(&V1) -> (CO::IndexKey, CO::Value) + Clone + 'static,
    {
        // TODO: implement UnorderedLeaf trie backed by an unsorted vector.
        self.map_keys::<_, _, _, OrderedLeaf<_, _>, _>(f).index()
    }
}

/// Operator that generates an indexed representation of a Z-set.
///
/// The input of the operator is a Z-set where the value type is
/// a key/value pair.  The output is an indexed representation of
/// the Z-set, i.e., a finite map from keys to Z-sets of
/// values associated with each key.
///
/// The input Z-set can be represented by any type that can be
/// converted into a sequence of `((key, value), weight)` pairs.
///
/// # Type arguments
///
/// * `CI` - input collection type.
/// * `CO` - output collection type, a finite map from keys to a Z-set of
///   values.
pub struct Index<CI, CO> {
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO> Index<CI, CO> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<CI, CO> Default for Index<CI, CO> {
    fn default() -> Self {
        Self::new()
    }
}

impl<CI, CO> Operator for Index<CI, CO>
where
    CI: 'static,
    CO: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Index")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<CI, CO> UnaryOperator<CI, CO> for Index<CI, CO>
where
    CO: IndexedZSet,
    CI: Trie<Key = ((CO::IndexKey, CO::Value), CO::Weight)> + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut cursor = i.cursor();
        let mut builder = <CO as Trie>::TupleBuilder::with_capacity(i.keys());
        while cursor.valid(i) {
            let ((k, v), w) = cursor.key(i);
            // TODO: pass key (and value?) by reference
            builder.push_tuple((k.clone(), (v.clone(), w.clone())));
            cursor.step(i);
        }
        builder.done()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation.
        self.eval(&i)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::OrdIndexedZSet, circuit::Root, finite_map, indexed_zset, operator::Generator,
    };

    #[test]
    fn index_sequence() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                finite_map!{ (1, "a") => 1
                           , (1, "a") => 2
                           , (1, "b") => 1
                           , (1, "b") => -1
                           , (2, "a") => 1
                           , (2, "c") => 1
                },
                finite_map!{ (1, "d") => 1
                           , (1, "e") => 1
                           , (2, "a") => -1
                           , (3, "a") => 2
                },
            ].into_iter();
            let mut outputs = vec![
                indexed_zset!{ 1 => {"a" => 3}, 2 => {"a" => 1, "c" => 1}},
                indexed_zset!{ 1 => {"a" => 3, "d" => 1, "e" => 1}, 2 => {"c" => 1}, 3 => {"a" => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index::<OrdIndexedZSet<_, _, _>>()
                   .integrate()
                   .inspect(move |fm: &OrdIndexedZSet<_, _, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }

    #[test]
    fn index_zset() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                finite_map!{
                      (1, "a") => 1
                    , (1, "b") => 1
                    , (2, "a") => 1
                    , (2, "c") => 1
                    , (1, "a") => 2
                    , (1, "b") => -1
                },
                finite_map!{
                      (1, "d") => 1
                    , (1, "e") => 1
                    , (2, "a") => -1
                    , (3, "a") => 2
                },
            ].into_iter();
            let mut outputs = vec![
                indexed_zset!{ 1 => {"a" => 3}, 2 => {"a" => 1, "c" => 1}},
                indexed_zset!{ 1 => {"a" => 3, "d" => 1, "e" => 1}, 2 => {"c" => 1}, 3 => {"a" => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index::<OrdIndexedZSet<_, _, _>>()
                   .integrate()
                   .inspect(move |fm: &OrdIndexedZSet<_, _, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }
}
