//! Based on the Network Analysis query from [GKG 2.0 Sample Queries](https://blog.gdeltproject.org/google-bigquery-gkg-2-0-sample-queries/)
//!
//! ```sql
//! SELECT a.name, b.name, COUNT(*) as count
//! FROM (
//!     FLATTEN(
//!         SELECT GKGRECORDID, UNIQUE(REGEXP_REPLACE(SPLIT(V2Persons, ';'), r',.*', ")) name
//!         FROM [gdelt-bq:gdeltv2.gkg]
//!         WHERE DATE > 20150302000000 and DATE < 20150304000000 and V2Persons like '%Tsipras%', name
//!     )
//! ) a
//! JOIN EACH (
//!     SELECT GKGRECORDID, UNIQUE(REGEXP_REPLACE(SPLIT(V2Persons, ';'), r',.*', ")) name
//!     FROM [gdelt-bq:gdeltv2.gkg]
//!     WHERE DATE > 20150302000000 and DATE < 20150304000000 and V2Persons like '%Tsipras%
//! ) b
//! ON a.GKGRECORDID = b.GKGRECORDID
//! WHERE a.name < b.name
//! GROUP EACH BY 1,2
//! ORDER BY 3 DESC
//! LIMIT 250
//! ```

use crate::data::PersonalNetworkGkgEntry;
use arcstr::ArcStr;
use bitvec::vec::BitVec;
use dbsp::{
    algebra::{IndexedZSet, MulByRef, ZRingValue},
    circuit::{
        metadata::{OperatorLocation, OperatorMeta},
        operator_traits::{BinaryOperator, Operator},
        Scope,
    },
    operator::FilterMap,
    time::AntichainRef,
    trace::{
        consolidation,
        layers::{Builder as LayerBuilder, TupleBuilder},
        spine_fueled::{MergeState, MergeVariant, Spine},
        unordered::{UnorderedLeaf, UnorderedLeafBuilder},
        Batch, BatchReader, Batcher, Builder, Consumer, Cursor, Merger, ValueConsumer,
    },
    Circuit, DBData, DBWeight, NumEntries, OrdIndexedZSet, OrdZSet, Stream,
};
use hashbrown::{
    hash_map::{Entry, RawEntryMut},
    HashMap,
};
use size_of::SizeOf;
use std::{
    borrow::Cow,
    cmp::max,
    fmt::{self, Debug},
    hash::Hash,
    marker::PhantomData,
    ops::Range,
    panic::Location,
};
use xxhash_rust::xxh3::Xxh3Builder;

pub fn personal_network(
    target: ArcStr,
    date_range: Option<Range<u64>>,
    events: &Stream<Circuit<()>, OrdZSet<PersonalNetworkGkgEntry, i32>>,
) -> Stream<Circuit<()>, OrdZSet<(ArcStr, ArcStr), i32>> {
    // Filter out events outside of our date range and that don't mention our target
    let relevant_events = if let Some(date_range) = date_range {
        events
            .filter(move |entry| date_range.contains(&entry.date) && entry.people.contains(&target))
    } else {
        events.filter(move |entry| entry.people.contains(&target))
    };

    let forward_events =
        relevant_events.index_with(|entry| (entry.id.clone(), entry.people.clone()));
    let flattened = relevant_events
        .flat_map(|event| {
            event
                .people
                .iter()
                .map(|person| (event.id.clone(), person.clone()))
                .collect::<Vec<_>>()
        })
        .index();

    // TODO: Hashjoin is much more optimal for joining strings
    let expected =
        flattened.join_generic::<(), _, _, OrdZSet<_, _>, _>(&forward_events, |_, a, people| {
            people
                .iter()
                .filter_map(|b| (a < b).then(|| ((a.clone(), b.clone()), ())))
                .collect::<Vec<_>>()
        });

    let hashjoined = hashjoin(&flattened, &forward_events, |_, a, people| {
        people
            .iter()
            .filter_map(|b| (a < b).then(|| ((a.clone(), b.clone()), ())))
            .collect::<Vec<_>>()
    });

    expected.minus(&hashjoined).gather(0).inspect(|errors| {
        let mut cursor = errors.cursor();
        while cursor.key_valid() {
            let mentions = cursor.weight();
            let (source, target) = cursor.key();
            println!(
                "error, {}: {source}, {target}, {mentions}",
                if mentions.is_positive() {
                    "missing"
                } else {
                    "added"
                },
            );
            cursor.step_key();
        }
    });

    // TODO: topk 250
    // TODO: Is there a better thing to do other than integration?
    hashjoined.integrate()
}

// TODO: Hash collections/traces
fn hashjoin<C, F, Iter, K, V1, V2, R, Z>(
    left: &Stream<Circuit<C>, OrdIndexedZSet<K, V1, R>>,
    right: &Stream<Circuit<C>, OrdIndexedZSet<K, V2, R>>,
    join: F,
) -> Stream<Circuit<C>, Z>
where
    C: Clone + 'static,
    F: Fn(&K, &V1, &V2) -> Iter + Clone + 'static,
    Iter: IntoIterator<Item = (Z::Key, Z::Val)> + 'static,
    K: DBData,
    V1: DBData,
    V2: DBData,
    R: DBWeight + ZRingValue,
    Z: IndexedZSet<R = R>,
    Z::R: ZRingValue,
{
    let left = left.shard();
    let right = right.shard();

    let left_trace = left.trace::<Spine<HashedKVBatch<K, V1, R>>>();
    let right_trace = right.trace::<Spine<HashedKVBatch<K, V2, R>>>();

    let left = left.circuit().add_binary_operator(
        HashJoin::new(join.clone(), Location::caller()),
        &left,
        &right_trace,
    );

    let right = left.circuit().add_binary_operator(
        HashJoin::new(
            move |k: &K, v2: &V2, v1: &V1| join(k, v1, v2),
            Location::caller(),
        ),
        &right,
        &left_trace.delay_trace(),
    );

    left.plus(&right)
}

pub struct HashJoin<F, I, V, Z, Iter> {
    join_func: F,
    location: &'static Location<'static>,
    // True if empty input batch was received at the current clock cycle.
    empty_input: bool,
    // True if empty output was produced at the current clock cycle.
    empty_output: bool,
    __type: PhantomData<*const (I, V, Z, Iter)>,
}

impl<F, I, V, Z, Iter> HashJoin<F, I, V, Z, Iter> {
    pub fn new(join_func: F, location: &'static Location<'static>) -> Self {
        Self {
            join_func,
            location,
            empty_input: false,
            empty_output: false,
            __type: PhantomData,
        }
    }
}

impl<F, I, V, Z, Iter> Operator for HashJoin<F, I, V, Z, Iter>
where
    F: 'static,
    I: 'static,
    V: 'static,
    Z: 'static,
    Iter: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("HashJoin")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.empty_input = false;
            self.empty_output = false;
        }
    }

    fn clock_end(&mut self, _scope: Scope) {}

    fn metadata(&self, _meta: &mut OperatorMeta) {}

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // We're in a stable state if input and output at the current clock cycle are
        // both empty, and there are no precomputed outputs before the end of the
        // clock epoch.
        self.empty_input && self.empty_output
    }
}

impl<F, I, V, Z, Iter> BinaryOperator<I, Spine<HashedKVBatch<I::Key, V, I::R>>, Z>
    for HashJoin<F, I, V, Z, Iter>
where
    I: IndexedZSet,
    V: DBData,
    F: Fn(&I::Key, &I::Val, &V) -> Iter + Clone + 'static,
    Z: IndexedZSet<R = I::R>,
    Z::R: ZRingValue,
    Iter: IntoIterator<Item = (Z::Key, Z::Val)> + 'static,
{
    // TODO: We can use an unordered collection as our index since we don't actually
    // care about them being in any coherent order
    fn eval(&mut self, index: &I, trace: &Spine<HashedKVBatch<I::Key, V, I::R>>) -> Z {
        self.empty_input = index.is_empty();

        dbg!(&trace.merging);

        let mut index_cursor = index.cursor();
        let mut trace_probe = SpineProbes::new(trace);

        let mut batch = Vec::with_capacity(index.len());
        while index_cursor.key_valid() {
            if trace_probe.probe_key(index_cursor.key()) {
                while index_cursor.val_valid() {
                    let index_weight = index_cursor.weight();
                    let v1 = index_cursor.val();

                    while trace_probe.val_valid() {
                        let output = (self.join_func)(index_cursor.key(), v1, trace_probe.val());
                        let weight = index_weight.mul_by_ref(trace_probe.weight());

                        for (key, value) in output {
                            batch.push((Z::item_from(key, value), weight.clone()));
                        }

                        trace_probe.step_val();
                    }

                    trace_probe.rewind_vals();
                    index_cursor.step_val();
                }
            }

            index_cursor.step_key();
        }

        let mut batcher = Z::Batcher::new_batcher(());
        batcher.push_batch(&mut batch);

        let result = batcher.seal();
        self.empty_output = result.is_empty();

        result
    }
}

struct SpineProbes<'a, K, V, R> {
    probes: Vec<HashedKVBatchProbe<'a, K, V, R>>,
    contains_key: BitVec,
    current: usize,
}

impl<'a, K, V, R> SpineProbes<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn new(spine: &'a Spine<HashedKVBatch<K, V, R>>) -> Self {
        let mut probes = Vec::with_capacity(spine.merging.len());
        for merge_state in spine.merging.iter().rev() {
            match merge_state {
                MergeState::Double(MergeVariant::InProgress(batch1, batch2, _)) => {
                    if !batch1.is_empty() {
                        probes.push(batch1.probe());
                    }

                    if !batch2.is_empty() {
                        probes.push(batch2.probe());
                    }
                }

                MergeState::Double(MergeVariant::Complete(Some(batch)))
                | MergeState::Single(Some(batch)) => {
                    if !batch.is_empty() {
                        probes.push(batch.probe());
                    }
                }

                MergeState::Double(MergeVariant::Complete(None))
                | MergeState::Single(None)
                | MergeState::Vacant => {}
            }
        }

        let contains_key = BitVec::repeat(false, probes.len());

        Self {
            probes,
            contains_key,
            current: 0,
        }
    }

    fn probe_key(&mut self, key: &K) -> bool {
        for (idx, probe) in self.probes.iter_mut().enumerate() {
            self.contains_key.set(idx, probe.probe_key(key));
        }
        self.current = self.contains_key.first_one().unwrap_or(self.probes.len());

        self.contains_key.any()
    }

    fn val_valid(&self) -> bool {
        self.current < self.probes.len() && self.probes[self.current].val_valid()
    }

    fn val(&self) -> &V {
        self.probes[self.current].val()
    }

    fn weight(&self) -> &R {
        self.probes[self.current].weight()
    }

    fn step_val(&mut self) {
        if self.current < self.probes.len() {
            self.probes[self.current].step_val();

            if !self.probes[self.current].val_valid() {
                self.current = self
                    .contains_key
                    .iter()
                    .enumerate()
                    .skip(self.current)
                    .filter_map(|(idx, contains_key)| contains_key.then_some(idx))
                    .next()
                    .unwrap_or(self.probes.len());
            }
        }
    }

    fn rewind_vals(&mut self) {
        self.current = 0;
        for probe in &mut self.probes {
            probe.rewind_vals();
        }
    }
}

struct HashedKVBatchProbe<'a, K, V, R> {
    batch: &'a HashedKVBatch<K, V, R>,
    current: usize,
    start: usize,
    end: usize,
}

impl<'a, K, V, R> HashedKVBatchProbe<'a, K, V, R> {
    const fn new(batch: &'a HashedKVBatch<K, V, R>) -> Self {
        Self {
            batch,
            current: 0,
            start: 0,
            end: 0,
        }
    }

    fn val_valid(&self) -> bool {
        self.current < self.end
    }

    fn val(&self) -> &V {
        &self.batch.values.keys()[self.current]
    }

    fn weight(&self) -> &R {
        &self.batch.values.diffs()[self.current]
    }

    fn step_val(&mut self) {
        self.current += 1;
    }

    fn rewind_vals(&mut self) {
        self.current = self.start;
    }
}

impl<'a, K, V, R> HashedKVBatchProbe<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn probe_key(&mut self, key: &K) -> bool {
        if let Some(offset) = self.batch.keys.get(key).copied() {
            self.current = self.batch.offsets[offset];
            self.end = self.batch.offsets[offset + 1];
            true
        } else {
            false
        }
    }
}

// TODO: We can use an `O: OrdOffset` instead of the `usize` offsets we
// currently use
#[derive(Clone, SizeOf)]
struct HashedKVBatch<K, V, R> {
    // FIXME: `SizeOf for Xxh3Builder`
    // Invariant: Each offset within `keys` and each offset within keys +1 are valid indices into
    // `offsets`
    #[size_of(skip)]
    keys: HashMap<K, usize, Xxh3Builder>,
    // Invariant: Each offset within `offsets` is a valid index into `values`
    offsets: Vec<usize>,
    // The value+diff pairs associated with any given key can be fetched with
    // `values[offsets[keys[&key]]..offsets[keys[&key] + 1]]`
    values: UnorderedLeaf<V, R>,
}

impl<K, V, R> HashedKVBatch<K, V, R> {
    fn probe(&self) -> HashedKVBatchProbe<'_, K, V, R> {
        HashedKVBatchProbe::new(self)
    }
}

impl<K, V, R> NumEntries for HashedKVBatch<K, V, R> {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.keys.len()
    }

    fn num_entries_deep(&self) -> usize {
        self.values.len()
    }
}

impl<K, V, R> BatchReader for HashedKVBatch<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;

    type Cursor<'a> = HashedKVCursor<'a, K, V, R>;
    type Consumer = HashedKVConsumer<K, V, R>;

    fn cursor(&self) -> Self::Cursor<'_> {
        todo!()
    }

    fn consumer(self) -> Self::Consumer {
        todo!()
    }

    fn key_count(&self) -> usize {
        self.keys.len()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn lower(&self) -> AntichainRef<'_, Self::Time> {
        AntichainRef::empty()
    }

    fn upper(&self) -> AntichainRef<'_, Self::Time> {
        AntichainRef::new(&[()])
    }
}

impl<K, V, R> Batch for HashedKVBatch<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, V);
    type Batcher = HashedKVBuilder<K, V, R>;
    type Builder = HashedKVBuilder<K, V, R>;
    type Merger = HashedKVBuilder<K, V, R>;

    fn item_from(key: Self::Key, value: Self::Val) -> Self::Item {
        (key, value)
    }

    fn from_keys(_time: Self::Time, inputs: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        let mut keys = HashMap::with_capacity_and_hasher(inputs.len(), Xxh3Builder::new());
        let mut values = UnorderedLeafBuilder::with_capacity(inputs.len());
        let mut offsets = Vec::with_capacity(inputs.len() + 1);
        offsets.push(0);

        for (key, diff) in inputs {
            if !diff.is_zero() {
                // Push the value+diff pair
                values.push_tuple((Self::Val::from(()), diff));

                // Add the key and the offset of the start of its value range to the keys map
                debug_assert!(!keys.contains_key(&key));
                keys.insert_unique_unchecked(key, offsets.len() - 1);

                // Record the end of the current key's values in offsets
                offsets.push(values.boundary());
            }
        }

        Self {
            keys,
            offsets,
            values: values.done(),
        }
    }

    fn recede_to(&mut self, _frontier: &Self::Time) {}
}

impl<K, V, R> Debug for HashedKVBatch<K, V, R>
where
    K: Debug,
    V: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct KVBatch<'a, K, V, R>(&'a HashedKVBatch<K, V, R>);

        impl<K, V, R> Debug for KVBatch<'_, K, V, R>
        where
            K: Debug,
            V: Debug,
            R: Debug,
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let batch = self.0;

                let mut map = f.debug_map();
                for (key, &offset) in batch.keys.iter() {
                    let start = batch.offsets[offset];
                    let end = batch.offsets[offset + 1];

                    map.entry(
                        key,
                        &ValDiffPairs(
                            &batch.values.keys()[start..end],
                            &batch.values.diffs()[start..end],
                        ),
                    );
                }

                map.finish()
            }
        }

        struct ValDiffPairs<'a, V, R>(&'a [V], &'a [R]);

        impl<V, R> Debug for ValDiffPairs<'_, V, R>
        where
            V: Debug,
            R: Debug,
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_map().entries(self.0.iter().zip(self.1)).finish()
            }
        }

        f.debug_struct("HashedKVBatch")
            .field("batch", &KVBatch(self))
            .finish_non_exhaustive()
    }
}

struct HashedKVCursor<'a, K, V, R> {
    __type: PhantomData<&'a (K, V, R)>,
}

impl<'a, K, V, R> Cursor<'a, K, V, (), R> for HashedKVCursor<'a, K, V, R> {
    fn key_valid(&self) -> bool {
        todo!()
    }

    fn val_valid(&self) -> bool {
        todo!()
    }

    fn key(&self) -> &K {
        todo!()
    }

    fn val(&self) -> &V {
        todo!()
    }

    fn fold_times<F, U>(&mut self, _init: U, _fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        todo!()
    }

    fn fold_times_through<F, U>(&mut self, _upper: &(), init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.fold_times(init, fold)
    }

    fn weight(&mut self) -> R {
        todo!()
    }

    fn step_key(&mut self) {
        todo!()
    }

    fn seek_key(&mut self, _key: &K) {
        todo!()
    }

    fn last_key(&mut self) -> Option<&K> {
        todo!()
    }

    fn step_val(&mut self) {
        todo!()
    }

    fn seek_val(&mut self, _value: &V) {
        todo!()
    }

    fn seek_val_with<P>(&mut self, _predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        todo!()
    }

    fn rewind_keys(&mut self) {
        todo!()
    }

    fn rewind_vals(&mut self) {
        todo!()
    }
}

struct HashedKVConsumer<K, V, R> {
    __type: PhantomData<(K, V, R)>,
}

impl<K, V, R> Consumer<K, V, R, ()> for HashedKVConsumer<K, V, R> {
    type ValueConsumer<'a> = HashedValueConsumer<'a, V, R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &K {
        todo!()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &K)
    where
        K: Ord,
    {
        todo!()
    }
}

struct HashedValueConsumer<'a, V, R> {
    __type: PhantomData<&'a (V, R)>,
}

impl<'a, V, R> ValueConsumer<'a, V, R, ()> for HashedValueConsumer<'a, V, R> {
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (V, R, ()) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}

#[derive(Debug, SizeOf)]
struct HashedKVBuilder<K, V, R> {
    // FIXME: ???
    #[size_of(skip)]
    pairs: HashMap<K, Vec<(V, R)>, Xxh3Builder>,
}

impl<K, V, R> HashedKVBuilder<K, V, R> {
    // TODO: Once we're confident in this code we can remove pretty much all of the
    // bounds checks
    fn append_batch(&mut self, batch: &HashedKVBatch<K, V, R>)
    where
        K: Hash + Eq + Clone,
        V: Clone,
        R: Clone,
    {
        for (key, &offset) in &batch.keys {
            let value_start = batch.offsets[offset];
            let value_end = batch.offsets[offset + 1];
            let key_values = value_end - value_start;

            assert!(value_start <= value_end && value_end <= batch.values.len());
            match self.pairs.raw_entry_mut().from_key(key) {
                RawEntryMut::Occupied(mut occupied) => {
                    let values = occupied.get_mut();
                    values.reserve(key_values);

                    for idx in value_start..value_end {
                        values.push((
                            batch.values.keys()[idx].clone(),
                            batch.values.diffs()[idx].clone(),
                        ));
                    }
                }

                RawEntryMut::Vacant(vacant) => {
                    let (_, values) = vacant.insert(key.clone(), Vec::with_capacity(key_values));

                    for idx in value_start..value_end {
                        values.push((
                            batch.values.keys()[idx].clone(),
                            batch.values.diffs()[idx].clone(),
                        ));
                    }
                }
            }
        }
    }
}

impl<K, V, R> Builder<(K, V), (), R, HashedKVBatch<K, V, R>> for HashedKVBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn new_builder(_time: ()) -> Self {
        Self {
            pairs: HashMap::with_hasher(Xxh3Builder::new()),
        }
    }

    fn with_capacity(_time: (), capacity: usize) -> Self {
        Self {
            pairs: HashMap::with_capacity_and_hasher(capacity, Xxh3Builder::new()),
        }
    }

    fn push(&mut self, ((key, value), diff): ((K, V), R)) {
        match self.pairs.entry(key) {
            Entry::Occupied(mut entry) => entry.get_mut().push((value, diff)),
            Entry::Vacant(entry) => {
                entry.insert(vec![(value, diff)]);
            }
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.pairs.reserve(additional);
    }

    fn done(self) -> HashedKVBatch<K, V, R> {
        let mut keys = HashMap::with_capacity_and_hasher(self.pairs.len(), Xxh3Builder::new());
        let mut values = UnorderedLeafBuilder::with_capacity(self.pairs.len());
        let mut offsets = Vec::with_capacity(keys.len() + 1);
        offsets.push(0);

        for (key, mut key_values) in self.pairs {
            // Consolidate the values of each key
            consolidation::consolidate(&mut key_values);

            // If any values are actually produced for the given key, create the entry
            if !key_values.is_empty() {
                // Add the key's values to the values leaf
                values.extend_tuples(key_values);

                // Add the key and the offset of the start of its value range to the keys map
                debug_assert!(!keys.contains_key(&key));
                keys.insert_unique_unchecked(key, offsets.len() - 1);

                // Record the end of the current key's values in offsets
                offsets.push(values.boundary());
            }
        }

        HashedKVBatch {
            keys,
            offsets,
            values: values.done(),
        }
    }
}

impl<K, V, R> Batcher<(K, V), (), R, HashedKVBatch<K, V, R>> for HashedKVBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn new_batcher(time: ()) -> Self {
        Self::new_builder(time)
    }

    fn push_batch(&mut self, batch: &mut Vec<((K, V), R)>) {
        self.extend(batch.drain(..));
    }

    fn push_consolidated_batch(&mut self, batch: &mut Vec<((K, V), R)>) {
        self.extend(batch.drain(..));
    }

    fn tuples(&self) -> usize {
        self.pairs.values().map(Vec::len).sum()
    }

    fn seal(self) -> HashedKVBatch<K, V, R> {
        Builder::done(self)
    }
}

impl<K, V, R> Merger<K, V, (), R, HashedKVBatch<K, V, R>> for HashedKVBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn new_merger(left: &HashedKVBatch<K, V, R>, right: &HashedKVBatch<K, V, R>) -> Self {
        Self::with_capacity((), left.key_count() + right.key_count())
    }

    fn work(
        &mut self,
        left: &HashedKVBatch<K, V, R>,
        right: &HashedKVBatch<K, V, R>,
        fuel: &mut isize,
    ) {
        self.reserve(left.key_count() + right.key_count());
        self.append_batch(left);
        self.append_batch(right);

        // FIXME: Not really sure what I'm doing here tbh, I currently just kinda
        // instantly finish all merges which is somewhat sub-optimal
        *fuel = max(*fuel - self.pairs.len() as isize, 1);
    }

    fn done(self) -> HashedKVBatch<K, V, R> {
        Builder::done(self)
    }
}
