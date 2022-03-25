//! Implementation using ordered keys and exponential search.

use super::{advance, Builder, Cursor, MergeBuilder, Trie, TupleBuilder};
use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, NegByRef},
    NumEntries, SharedRef,
};
use std::{
    marker::PhantomData,
    ops::{Add, AddAssign, Neg},
};

/// A layer of unordered values.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct OrderedLeaf<K, R> {
    /// Unordered values.
    pub vals: Vec<(K, R)>,
}

// TODO: by-value merge
impl<K, R> Add<Self> for OrderedLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self.merge(&rhs)
    }
}

impl<K, R> AddAssign<Self> for OrderedLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    fn add_assign(&mut self, rhs: Self) {
        *self = self.merge(&rhs);
    }
}

impl<K, R> AddAssignByRef for OrderedLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        *self = self.merge(other);
    }
}

impl<K, R> AddByRef for OrderedLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, R> NegByRef for OrderedLeaf<K, R>
where
    K: Ord + Clone,
    R: NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            vals: self
                .vals
                .iter()
                .map(|(k, v)| (k.clone(), v.neg_by_ref()))
                .collect(),
        }
    }
}

impl<K, R> Neg for OrderedLeaf<K, R>
where
    K: Ord + Clone,
    R: Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            vals: self.vals.into_iter().map(|(k, v)| (k, v.neg())).collect(),
        }
    }
}

impl<K, R> NumEntries for OrderedLeaf<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssignByRef + Clone,
{
    fn num_entries_shallow(&self) -> usize {
        self.keys()
    }

    fn num_entries_deep(&self) -> usize {
        self.keys()
    }

    fn const_num_entries() -> Option<usize> {
        None
    }
}

impl<K, R> SharedRef for OrderedLeaf<K, R>
where
    K: Clone,
    R: Clone,
{
    type Target = Self;

    fn try_into_owned(self) -> Result<Self::Target, Self> {
        Ok(self)
    }
}

impl<K: Eq + Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> Trie for OrderedLeaf<K, R> {
    type Key = (K, R);
    type ChildKey = ();
    type Item = (K, R);
    type Cursor = OrderedLeafCursor<K, R>;
    type MergeBuilder = OrderedLeafBuilder<K, R>;
    type TupleBuilder = UnorderedLeafBuilder<K, R>;
    fn keys(&self) -> usize {
        self.vals.len()
    }
    fn tuples(&self) -> usize {
        <OrderedLeaf<K, R> as Trie>::keys(self)
    }
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor {
        OrderedLeafCursor {
            bounds: (lower, upper),
            pos: lower,
            _types: PhantomData,
        }
    }
}

/// A builder for unordered values.
pub struct OrderedLeafBuilder<K, R> {
    /// Unordered values.
    pub vals: Vec<(K, R)>,
}

impl<K: Eq + Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> Builder
    for OrderedLeafBuilder<K, R>
{
    type Trie = OrderedLeaf<K, R>;
    fn boundary(&mut self) -> usize {
        self.vals.len()
    }
    fn done(self) -> Self::Trie {
        OrderedLeaf { vals: self.vals }
    }
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> MergeBuilder
    for OrderedLeafBuilder<K, R>
{
    fn with_capacity(keys: usize, _tuples: usize) -> Self {
        OrderedLeafBuilder {
            vals: Vec::with_capacity(keys),
        }
    }
    #[inline]
    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        self.vals.extend_from_slice(&other.vals[lower..upper]);
    }
    fn push_merge(
        &mut self,
        other1: (&Self::Trie, <Self::Trie as Trie>::Cursor),
        other2: (&Self::Trie, <Self::Trie as Trie>::Cursor),
    ) -> usize {
        let (trie1, cursor1) = other1;
        let (trie2, cursor2) = other2;
        let mut lower1 = cursor1.bounds.0;
        let upper1 = cursor1.bounds.1;
        let mut lower2 = cursor2.bounds.0;
        let upper2 = cursor2.bounds.1;

        self.vals.reserve((upper1 - lower1) + (upper2 - lower2));

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            match trie1.vals[lower1].0.cmp(&trie2.vals[lower2].0) {
                ::std::cmp::Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance(&trie1.vals[(1 + lower1)..upper1], |x| {
                        x.0 < trie2.vals[lower2].0
                    });
                    let step = std::cmp::min(step, 1000);
                    <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(
                        self,
                        trie1,
                        lower1,
                        lower1 + step,
                    );
                    lower1 += step;
                }
                ::std::cmp::Ordering::Equal => {
                    let mut sum = trie1.vals[lower1].1.clone();
                    sum.add_assign_by_ref(&trie2.vals[lower2].1);
                    if !sum.is_zero() {
                        self.vals.push((trie1.vals[lower1].0.clone(), sum));
                    }

                    lower1 += 1;
                    lower2 += 1;
                }
                ::std::cmp::Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let step = 1 + advance(&trie2.vals[(1 + lower2)..upper2], |x| {
                        x.0 < trie1.vals[lower1].0
                    });
                    let step = std::cmp::min(step, 1000);
                    <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(
                        self,
                        trie2,
                        lower2,
                        lower2 + step,
                    );
                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 {
            <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie1, lower1, upper1);
        }
        if lower2 < upper2 {
            <OrderedLeafBuilder<K, R> as MergeBuilder>::copy_range(self, trie2, lower2, upper2);
        }

        self.vals.len()
    }
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> TupleBuilder
    for OrderedLeafBuilder<K, R>
{
    type Item = (K, R);
    fn new() -> Self {
        OrderedLeafBuilder { vals: Vec::new() }
    }
    fn with_capacity(cap: usize) -> Self {
        OrderedLeafBuilder {
            vals: Vec::with_capacity(cap),
        }
    }
    #[inline]
    fn push_tuple(&mut self, tuple: (K, R)) {
        self.vals.push(tuple)
    }
}

pub struct UnorderedLeafBuilder<K, R> {
    pub vals: Vec<(K, R)>,
    boundary: usize,
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> Builder
    for UnorderedLeafBuilder<K, R>
{
    type Trie = OrderedLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        let consolidated_len = consolidate_slice(&mut self.vals[self.boundary..]);
        self.boundary += consolidated_len;
        self.vals.truncate(self.boundary);
        self.boundary
    }
    fn done(mut self) -> Self::Trie {
        self.boundary();
        OrderedLeaf { vals: self.vals }
    }
}

impl<K: Ord + Clone, R: Eq + HasZero + AddAssignByRef + Clone> TupleBuilder
    for UnorderedLeafBuilder<K, R>
{
    type Item = (K, R);
    fn new() -> Self {
        UnorderedLeafBuilder {
            vals: Vec::new(),
            boundary: 0,
        }
    }
    fn with_capacity(cap: usize) -> Self {
        UnorderedLeafBuilder {
            vals: Vec::with_capacity(cap),
            boundary: 0,
        }
    }
    #[inline]
    fn push_tuple(&mut self, tuple: (K, R)) {
        self.vals.push(tuple)
    }
}

/// A cursor for walking through an unordered sequence of values.
#[derive(Debug)]
pub struct OrderedLeafCursor<K, R> {
    pos: usize,
    bounds: (usize, usize),
    _types: PhantomData<(K, R)>,
}

impl<K: Eq + Ord + Clone, R: Eq + Clone> Cursor for OrderedLeafCursor<K, R> {
    type Key = (K, R);
    type ChildKey = ();
    type Storage = OrderedLeaf<K, R>;
    type ValueStorage = ();

    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }
    fn key<'a>(&self, storage: &'a OrderedLeaf<K, R>) -> &'a Self::Key {
        &storage.vals[self.pos]
    }
    fn values<'a>(&self, _storage: &'a OrderedLeaf<K, R>) -> (&'a (), ()) {
        (&(), ())
    }
    fn step(&mut self, storage: &OrderedLeaf<K, R>) {
        self.pos += 1;
        if !self.valid(storage) {
            self.pos = self.bounds.1;
        }
    }
    fn seek(&mut self, storage: &OrderedLeaf<K, R>, key: &Self::Key) {
        self.pos += advance(&storage.vals[self.pos..self.bounds.1], |(k, _)| {
            k.lt(&key.0)
        });
    }
    fn valid(&self, _storage: &OrderedLeaf<K, R>) -> bool {
        self.pos < self.bounds.1
    }
    fn rewind(&mut self, _storage: &OrderedLeaf<K, R>) {
        self.pos = self.bounds.0;
    }
    fn reposition(&mut self, _storage: &OrderedLeaf<K, R>, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
    }
}

/// Sorts and consolidates a slice, returning the valid prefix length.
pub fn consolidate_slice<T: Ord, R: HasZero + AddAssignByRef + Clone>(
    slice: &mut [(T, R)],
) -> usize {
    // We could do an insertion-sort like initial scan which builds up sorted,
    // consolidated runs. In a world where there are not many results, we may
    // never even need to call in to merge sort.
    slice.sort_unstable_by(|x, y| x.0.cmp(&y.0));

    // Counts the number of distinct known-non-zero accumulations. Indexes the write
    // location.
    let mut offset = 0;
    for index in 1..slice.len() {
        // The following unsafe block elides various bounds checks, using the reasoning
        // that `offset` is always strictly less than `index` at the beginning
        // of each iteration. This is initially true, and in each iteration
        // `offset` can increase by at most one (whereas `index` always
        // increases by one). As `index` is always in bounds, and `offset` starts at
        // zero, it too is always in bounds.
        //
        // LLVM appears to struggle to optimize out Rust's split_at_mut, which would
        // prove disjointness using run-time tests.
        unsafe {
            assert!(offset < index);

            // LOOP INVARIANT: offset < index
            let ptr1 = slice.as_mut_ptr().add(offset);
            let ptr2 = slice.as_mut_ptr().add(index);

            if (*ptr1).0 == (*ptr2).0 {
                (*ptr1).1.add_assign_by_ref(&(*ptr2).1);
            } else {
                if !(*ptr1).1.is_zero() {
                    offset += 1;
                }
                let ptr1 = slice.as_mut_ptr().add(offset);
                std::ptr::swap(ptr1, ptr2);
            }
        }
    }
    if offset < slice.len() && !slice[offset].1.is_zero() {
        offset += 1;
    }

    offset
}
