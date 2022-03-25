#[macro_use]
mod zset_macro;

use crate::{
    algebra::{
        finite_map::{FiniteMap, OrdFiniteMap},
        AddAssignByRef, AddByRef, ZRingValue,
    },
    layers::{Builder, Cursor, OrderedLayer, OrderedLeaf, Trie, TupleBuilder},
    NumEntries, SharedRef,
};
use std::ops::Add;

/// The Z-set trait.
///
/// A Z-set is a set where each element has a weight.
/// Weights belong to some ring.
///
/// `Data` - Type of values stored in Z-set.
/// `Weight` - Type of weights.  Must be a value from a Ring.
pub trait ZSet: FiniteMap<MapKey = Self::Data, Value = Self::Weight> {
    type Data: Eq + Ord + Clone;
    type Weight: ZRingValue;

    /// Returns a Z-set that contains all elements with positive weights from
    /// `self` with weights set to 1.
    fn distinct(&self) -> Self;

    /// Like `distinct` but optimized to operate on an owned value.
    fn distinct_owned(self) -> Self;
}

/// An implementation of Z-sets backed by [`OrdFiniteMap`].
pub type OrdZSet<Data, Weight> = OrdFiniteMap<Data, Weight>;

impl<Data, Weight> ZSet for OrdZSet<Data, Weight>
where
    Data: Ord + Clone + 'static,
    Weight: ZRingValue,
{
    type Data = Data;
    type Weight = Weight;

    fn distinct(&self) -> Self {
        let mut builder = Self::TupleBuilder::with_capacity(self.keys());
        let mut cursor = self.cursor();

        while cursor.valid(self) {
            let (key, value) = cursor.key(self);
            if value.ge0() {
                builder.push_tuple((key.clone(), Weight::one()));
            }
            cursor.step(self);
        }

        builder.done()
    }

    // TODO: optimized implementation for owned values
    fn distinct_owned(self) -> Self {
        self.distinct()
    }
}

/// An indexed Z-set maps arbitrary keys to Z-set values.
pub trait IndexedZSet:
    Trie<
        Key = Self::IndexKey,
        ChildKey = (Self::Value, Self::Weight),
        Item = (Self::IndexKey, (Self::Value, Self::Weight)),
    > + Add<Output = Self>
    + AddByRef
    + AddAssignByRef
    + Clone
    + NumEntries
    + SharedRef<Target = Self>
    + 'static
{
    type IndexKey: Clone + Ord;
    type Value: Clone + Ord;
    type Weight: ZRingValue;
}

impl<T, Key, Value, Weight> IndexedZSet for T
where
    T: Trie<Key = Key, ChildKey = (Value, Weight), Item = (Key, (Value, Weight))>
        + Add<Output = T>
        + AddByRef
        + AddAssignByRef
        + Clone
        + NumEntries
        + SharedRef<Target = Self>
        + 'static,
    Value: Clone + Ord,
    Weight: ZRingValue,
    Key: Clone + Ord,
{
    type IndexKey = Key;
    type Value = Value;
    type Weight = Weight;
}

/// An implementation of indexes Z-sets backed by [`OrderedLayer`].
pub type OrdIndexedZSet<Key, Value, Weight> = OrderedLayer<Key, OrderedLeaf<Value, Weight>>;
