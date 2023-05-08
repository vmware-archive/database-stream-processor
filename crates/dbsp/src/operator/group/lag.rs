use super::{Monotonicity, NonIncrementalGroupTransformer};
use crate::{
    algebra::ZRingValue, trace::Cursor, DBData, DBWeight, IndexedZSet, OrdIndexedZSet, RootCircuit,
    Stream,
};
use std::marker::PhantomData;

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet + Send,
{
    pub fn lag<OV, PF, DF>(&self, lag: usize, project: PF, default: DF) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, (B::Val, OV), B::R>>
    where
        B::R: ZRingValue,
        PF: Fn(&Option<B::Val>) -> OV,
        DF: Fn() -> OV,
    {
        self.group_transform(Lag::new(lag, true, project, default))
    }

    pub fn lead<OV, PF, DF>(&self, lead: usize, project: F) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, (B::Val, OV), B::R>>
    where
        B::R: ZRingValue,
        PF: Fn(&B::Val) -> OV,
        DF: Fn() -> OV,
    {
        self.group_transform(Lag::new(lead, false, project, default))
    }
}

pub struct Lag<I, O, R, PF, DF> {
    name: String,
    lag: usize,
    asc: bool,
    project: PF,
    default: DF,
    _phantom: PhantomData<(I, R)>,
}

impl Lag<I, O, R, PF, DF> {
    fn new(lag: usize, asc: bool, project: PF, default: DF) -> Self {
        Self {
            name: format!("{}({lag})", if asc { "lag" } else { "lead" }),
            lag,
            asc,
            project,
            default
        }
    }
}

impl<I, O, R, PF, DF> GroupTransformer<I, (I, O), R> for Lag<I, O, PF, DF>
where
    I: DBData,
    O: DBData,
    R: DBWeight,
    PF: Fn(&I) -> O,
    DF: Fn() -> O,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn monotonicity(&self) -> Monotonicity {
        // Since outputs are produced during the second (backward) pass,
        // `lag` produces outputs on descending order, while `lead` -- in
        // ascending.
        if self.asc {
            Monotonicity::Descending
        } else {
            Monotonicity::Ascending
        }
    }

    fn transform<C1, C2, C3, CB>(
        &self,
        input_delta: &mut C1,
        input_trace: &mut C2,
        output_trace: &mut C3,
        output_cb: CB,
    ) where
        C1: Cursor<I, (), (), R>,
        C2: Cursor<I, (), (), R>,
        C3: Cursor<(I, O), (), (), R>,
        CB: FnMut(O, R)
    {
        // TODO: implement the other direction.
        assert!(self.asc);

        let mut next_key = input_delta.get_key();

        // Forward pass: compute contiguous key ranges that require updates.
        while next_key.is_some() && output_trace.key_valid() {
            // Seek key in `input_trace` and `output_trace`.
            input_trace.seek_key(next_key.unwrap());
            while input_trace.weight().is_zero() { input_trace.step_key() };

            output_trace.seek_key(next_key.unwrap());
            while output_trace.weight().is_zero() { output_trace.step_key() };

            // `input_trace` and `output_trace` must contain the exact same set
            // of keys with identical weights.
            debug_assert_eq!(input_trace.get_key(), output_trace.get_key());

            let mut lag = 0;

            while lag <= self.lag {
                // Reset the counter if we've hit the next key.
                if let Some(key) = next_key && output_trace.key_valid() {
                    if output_trace.key() > key {
                        retractions.push((key, None));
                        input_delta.step_key();
                        next_key = input_delta.get_key();
                        lag = 1;
                    } else if output_trace.key() == key {
                        input_delta.step_key();
                        next_key = input_delta.get_key();
                        lag = 0;
                    }
                };

                if !output_trace.key_valid() {
                    break;
                }
                
                retractions.push((output_trace.key(), output_trace.weight().neg()));

                input_trace.step_key();
                while input_trace.weight().is_zero() { input_trace.step_key() };

                output_trace.step_key();
                while output_trace.weight().is_zero() { output_trace.step_key() };

                debug_assert_eq!(input_trace.get_key(), output_trace.get_key());

                lag += 1;
            }

            retractions.push(None);
        }

        // Push remaining keys from `input_delta` as a single range.
        while input_delta.key_valid() {
            retractions.push((input_delta.key(), None));
            input_delta.step_key();
        }

        // Backward pass: compute updated values.
        let mut input_cursor = CursorPair::new(input_delta, input_trace);
        input_cursor.fast_forward_keys();

        let mut lag_cursor = input_cursor.clone();

        let retractions = retractions.drain(..).rev();

        while let Some(retraction) = retractions.next() {
            if retraction.is_none() {
                retraction = retractions.next();

                // seek to key or step to key on overlap.
                if lag_cursor.is_valid() && lag_cursor.key() <= retraction.0 {
                    while input_cursor.key() > retraction.0 {
                        input_cursor.step_key_reverse();
                        skip_zeros();
                        lag_cursor.step_reverse_n(1);
                    }
                } else {
                    input_cursor.seek_reverse(retraction.0);
                    debug_assert_eq!(input_cursor.key(), retraction.key());

                    // fn skip_zeros();
                    while input_cursor.weight().is_zero() {
                        // retraction.
                        output_cb();
                        let retraction = retractions.next();
                        input_cursor.step_key_reverse();
                        debug_assert_eq!(input_cursor.key(), retraction.key());
                    }

                    lag_cursor.seek_reverse();
                    debug_assert_eq!(log_cursor.get_key(), input_cursor.get_key());
                    lag_cursor.step_reverse_n(self.lag);
                }
            } else {
                // step both cursors
                input_cursor.step_key_reverse();
                skip_zeros();
                lag_cursor.step_reverse_n(1);

                // generate insertion
                if let Some(retraction) = retraction {
                    output_cb();
                    output_cb();
                } else {
                    output_cb();
                }
            }
        }
    }
}
