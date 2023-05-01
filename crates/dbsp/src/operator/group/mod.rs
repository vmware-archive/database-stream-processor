
pub trait GroupTransformer<I, O, R> {
    fn transform(
        input_delta: Cursor<I, (), R, ()>,
        input_trace: Cursor<I, (), R, ()>,
        output_trace: Cursor<O, (), R, ()>,
        output_cb: CB
    )
    where
        CB: Fn(O, R);
}

impl Stream<RootCircuit, B>
where
    B: BatchReader,
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

        let output = circuit.add_ternary_operator(
            GroupTransform::new(transform),
            &stream,
            &stream.integrate_trace(),
            &feedback.delayed_trace,
        ).mark_sharded();

        feedback.connect(&output);

        output
    }
}

struct GroupTransform<B, OB, T, OT, GT> {
    name: Cow<'static, str>,
    transform: GT,
    _phantom: PhantomData<(B, OB, T, OT)>,
}

impl<B, OB, T, OT, GT> Operator for GroupTransform<B, OB, T, OT, GT>
where
    B: 'static,
    OB: 'static,
    T: 'static,
    OT: 'static,
    GT: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from(format!("GroupTransform({})", self.name))
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<B, OB, T, OT, GT> TernaryOperator<B, T, OT, OB> for GroupTransform<B, OB, T, OT, GT>
where
    B: BatchReader,
    T: Trace<Batch = B>,
    OB: IndexedZSet<Key = B::Key, R = B::R>,
    OT: Trace<Batch = OB>,
    GT: GroupTransformer<B::Val, OB::Val, B::R>,
{
    fn eval<'a>(
        &mut self,
        delta: Cow<'a, B>,
        input_trace: Cow<'a, T>,
        output_trace: Cow<'a, OT>) -> OB
    {
        let mut delta_cursor = delta.cursor();
        let mut input_trace_cursor = input_trace.cursor();
        let mut output_trace_cursor = output_trace.cursor();

        let mut builder = OB::Builder::with_capacity(delta_cursor.len());

        while delta_cursor.key_valid() {
            input_trace_cursor.seek_key(delta_cursor.key());

            let mut input_group_cursor = if input_trace_cursor.key_valid() && input_trace_cursor.key() == delta_cursor.key() {
                input_trace_cursor.group_cursor()
            } else {
                empty_cursor
            };

            output_trace_cursor.seek_key(delta_cursor.key());

            let mut output_group_cursor = if output_trace_cursor.key_valid() && output_trace_cursor.key() == delta_cursor.key() {
                output_trace_cursor.group_cursor()
            } else {
                empty_cursor
            };

            self.transform.transform()
                delta_cursor.group_cursor(),
                input_group_cursor,
                output_group_cursor,
                |val, w| builder.push(OB::item_from(delta_cursor.key(), val), w),
            );
            
            delta_cursor.step_key();
        }

        builder.done()
    }
}
