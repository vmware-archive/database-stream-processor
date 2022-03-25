#[macro_use]
mod map_macro;

use crate::{
    algebra::GroupValue,
    layers::{OrderedLeaf, Trie},
    NumEntries, SharedRef,
};

/// Finite map trait.
///
/// A finite map maps arbitrary keys to values in a group.  It has
/// finite support: it is non-zero only for a finite number of keys.
/// Finite maps form a group whose plus operator computes point-wise
/// sum of values associated with each key.
pub trait FiniteMap:
    Trie<Key = (Self::MapKey, Self::Value), ChildKey = (), Item = (Self::MapKey, Self::Value)>
    + SharedRef<Target = Self>
    + GroupValue
    + NumEntries
{
    /// Type of values stored in finite map.
    type MapKey: Clone;
    /// Type of results.
    type Value: GroupValue;
}

impl<T, Key, Value> FiniteMap for T
where
    T: Trie<Key = (Key, Value), ChildKey = (), Item = (Key, Value)>
        + SharedRef<Target = Self>
        + GroupValue
        + NumEntries,
    Key: Clone,
    Value: GroupValue,
{
    type MapKey = Key;
    type Value = Value;
}

/// Finite map implementation backed by [`OrderedLeaf`].
///
/// Requires keys to form a total order.
pub type OrdFiniteMap<Key, Value> = OrderedLeaf<Key, Value>;
