use crate::{
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        Scope,
    },
    operator::trace::{TraceBounds, TraceFeedback},
    trace::{
        cursor::{CursorEmpty, CursorGroup},
        Builder, Cursor, Spine, Trace,
    },
    Circuit, IndexedZSet, RootCircuit, Stream,
};
use std::{borrow::Cow, marker::PhantomData};

pub trait GroupTransformer<I, O, R>: 'static {
    fn name(&self) -> &str;

    fn transform_incremental<C1, C2, C3, CB>(
        &self,
        input_delta: &mut C1,
        input_trace: &mut C2,
        output_trace: &mut C3,
        output_cb: CB,
    ) where
        C1: Cursor<I, (), (), R>,
        C2: Cursor<I, (), (), R>,
        C3: Cursor<O, (), (), R>,
        CB: FnMut(O, R);

    fn transform_non_incremental<C, CB>(
        &self,
        cursor: &mut C,
        output_cb: CB,
    ) where
        C1: Cursor<I, (), (), R>,
        CB: FnMut(O, R);
}




impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet + Send,
{
    fn group_transform_generic<GT, OB>(&self, transform: GT) -> Stream<RootCircuit, OB>
    where
        OB: IndexedZSet<Key = B::Key, R = B::R>,
        GT: GroupTransformer<B::Val, OB::Val, B::R>,
    {
        let circuit = self.circuit();
        let stream = self.shard();

        let bounds = TraceBounds::unbounded();
        let feedback = circuit.add_integrate_trace_feedback::<Spine<OB>>(bounds);

        let output = circuit
            .add_ternary_operator(
                GroupTransform::new(transform),
                &stream,
                &stream.integrate_trace(),
                &feedback.delayed_trace,
            )
            .mark_sharded();

        feedback.connect(&output);

        output
    }
}

struct GroupTransform<B, OB, T, OT, GT> {
    transformer: GT,
    _phantom: PhantomData<(B, OB, T, OT)>,
}

impl<B, OB, T, OT, GT> GroupTransform<B, OB, T, OT, GT> {
    fn new(transformer: GT) -> Self {
        Self {
            transformer,
            _phantom: PhantomData,
        }
    }
}

impl<B, OB, T, OT, GT> Operator for GroupTransform<B, OB, T, OT, GT>
where
    B: IndexedZSet + 'static,
    OB: IndexedZSet + 'static,
    T: 'static,
    OT: 'static,
    GT: GroupTransformer<B::Val, OB::Val, B::R>,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from(format!("GroupTransform({})", self.transformer.name()))
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<B, OB, T, OT, GT> TernaryOperator<B, T, OT, OB> for GroupTransform<B, OB, T, OT, GT>
where
    B: IndexedZSet,
    T: Trace<Key = B::Key, Val = B::Val, Time = (), R = B::R> + Clone,
    OB: IndexedZSet<Key = B::Key, R = B::R>,
    OT: Trace<Key = B::Key, Val = OB::Val, Time = (), R = B::R> + Clone,
    GT: GroupTransformer<B::Val, OB::Val, B::R>,
{
    fn eval<'a>(
        &mut self,
        delta: Cow<'a, B>,
        input_trace: Cow<'a, T>,
        output_trace: Cow<'a, OT>,
    ) -> OB {
        let mut delta_cursor = delta.cursor();
        let mut input_trace_cursor = input_trace.cursor();
        let mut output_trace_cursor = output_trace.cursor();

        let mut builder = OB::Builder::with_capacity((), delta.len());

        while delta_cursor.key_valid() {
            let key = delta_cursor.key().clone();

            input_trace_cursor.seek_key(&key);

            // I am not able to avoid 4-way code duplication below.  Depending on
            // whether `key` is found in the input and output trace, we must invoke
            // `transformer.transform` with four different combinations of
            // empty/non-empty cursors.  Since the cursors have different types
            // (`CursorEmpty` and `CursorGroup`), we kind bind them to the same
            // variable.
            if input_trace_cursor.key_valid() && input_trace_cursor.key() == &key {
                let mut input_group_cursor = CursorGroup::new(&mut input_trace_cursor, ());

                output_trace_cursor.seek_key(&key);

                if output_trace_cursor.key_valid() && output_trace_cursor.key() == &key
                {
                    let mut output_group_cursor = CursorGroup::new(&mut output_trace_cursor, ());

                    self.transformer.transform(
                        &mut CursorGroup::new(&mut delta_cursor, ()),
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        |val, w| builder.push((OB::item_from(key.clone(), val), w)),
                    );
                } else {
                    let mut output_group_cursor = CursorEmpty::new();

                    self.transformer.transform(
                        &mut CursorGroup::new(&mut delta_cursor, ()),
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        |val, w| builder.push((OB::item_from(key.clone(), val), w)),
                    );
                };
            } else {
                let mut input_group_cursor = CursorEmpty::new();

                output_trace_cursor.seek_key(&key);

                if output_trace_cursor.key_valid() && output_trace_cursor.key() == &key
                {
                    let mut output_group_cursor = CursorGroup::new(&mut output_trace_cursor, ());

                    self.transformer.transform(
                        &mut CursorGroup::new(&mut delta_cursor, ()),
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        |val, w| builder.push((OB::item_from(key.clone(), val), w)),
                    );
                } else {
                    let mut output_group_cursor = CursorEmpty::new();

                    self.transformer.transform(
                        &mut CursorGroup::new(&mut delta_cursor, ()),
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        |val, w| builder.push((OB::item_from(key.clone(), val), w)),
                    );
                };
            };

            delta_cursor.step_key();
        }

        builder.done()
    }
}
