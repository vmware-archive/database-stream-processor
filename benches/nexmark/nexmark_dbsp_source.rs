//! DBSP Source operator that reads from a Nexmark Generator.

use crate::generator::{wallclock_time, NexmarkGenerator, NextEvent};
use crate::model::Event;
use dbsp::{
    algebra::{ZRingValue, ZSet},
    circuit::{
        operator_traits::{Data, Operator, SourceOperator},
        Scope,
    },
};
use rand::Rng;
use std::thread::sleep;
use std::time::Duration;
use std::{borrow::Cow, marker::PhantomData};

pub struct NexmarkDBSPSource<R: Rng, W, C> {
    /// The generator for events emitted by this source.
    // TODO(absoludity): Longer-term, it'd be great to use a client (such as a gRPC client) here to
    // completely separate the generator and allow the generator to be used with other languages
    // etc.
    generator: NexmarkGenerator<R>,
    // next_event stores the next event during `eval` when `next_event()` is called but returns an
    // event in the future, so that we can include it in the next call to eval.
    next_event: Option<NextEvent>,

    _t: PhantomData<(C, W)>,
}

impl<R, W, C> NexmarkDBSPSource<R, W, C>
where
    R: Rng,
{
    pub fn from_generator(generator: NexmarkGenerator<R>) -> Self {
        NexmarkDBSPSource {
            generator,
            next_event: None,
            _t: PhantomData,
        }
    }
}

impl<R, W, C> Operator for NexmarkDBSPSource<R, W, C>
where
    C: Data,
    R: Rng + 'static,
    W: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("NexmarkDBSPSource")
    }

    // For the ability to reset the circuit and run it from clean state without
    // rebuilding it, then clock_start resets the generator to the beginning of the
    // sequence.
    fn clock_start(&mut self, _scope: Scope) {
        self.generator.reset();
    }

    // Returns true if the generator has no more data (and so this source will
    // return empty zsets from now on).
    fn fixedpoint(&self, _scope: Scope) -> bool {
        !self.generator.has_next()
    }
}

impl<R, W, C> SourceOperator<C> for NexmarkDBSPSource<R, W, C>
where
    R: Rng + 'static,
    W: ZRingValue + 'static,
    C: Data + ZSet<Key = Event, R = W>,
{
    fn eval(&mut self) -> C {
        // Grab a next event, either the last event from the previous call that
        // was saved because it couldn't yet be emitted, or the next generated
        // event.
        let next_event = match self.next_event.clone() {
            Some(e) => Some(e),
            None => self.generator.next_event().unwrap(),
        };

        // If there are no more events, we return an empty set.
        if next_event.is_none() {
            return C::empty(());
        }

        // Otherwise we want to emit at least one event, so if the next event
        // is still in the future, we sleep until we can emit it.
        let next_event = next_event.unwrap();
        let wallclock_time_now = wallclock_time().unwrap();
        if next_event.wallclock_timestamp > wallclock_time_now {
            sleep(Duration::from_millis(
                next_event.wallclock_timestamp - wallclock_time_now,
            ));
        }

        // Collect as many next events as are ready.
        let mut next_events = vec![next_event];
        let mut next_event = self.generator.next_event().unwrap();
        let wallclock_time_now = wallclock_time().unwrap();
        while next_event
            .is_some_and(|next_event| next_event.wallclock_timestamp.clone() < wallclock_time_now)
        {
            next_events.push(next_event.unwrap());
            next_event = self.generator.next_event().unwrap();
        }

        C::from_tuples(
            (),
            next_events
                .into_iter()
                .map(|next_event| ((next_event.event, ()), W::one()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::generator::{config::Config, tests::generate_expected_next_events};
    use dbsp::{circuit::Root, trace::ord::OrdZSet, trace::Batch};
    use rand::rngs::mock::StepRng;

    fn make_test_source(
        wallclock_base_time: u64,
        max_events: u64,
    ) -> NexmarkDBSPSource<StepRng, isize, OrdZSet<Event, isize>> {
        let mut source = NexmarkDBSPSource::from_generator(NexmarkGenerator::new(
            Config {
                max_events,
                ..Config::default()
            },
            StepRng::new(0, 1),
        ));
        source
            .generator
            .set_wallclock_base_time(wallclock_base_time);
        source
    }

    // Generates a zset manually using the default test NexmarkGenerator
    fn generate_expected_zset(
        wallclock_base_time: u64,
        num_events: usize,
    ) -> OrdZSet<Event, isize> {
        let expected_events = generate_expected_next_events(wallclock_base_time, num_events);
        let expected_zset_tuples = expected_events
            .into_iter()
            .filter(|event| event.is_some())
            .map(|event| ((event.unwrap().event, ()), 1))
            .collect();

        OrdZSet::<Event, isize>::from_tuples((), expected_zset_tuples)
    }

    #[test]
    fn test_start_clock() {
        let expected_zset = generate_expected_zset(1_000_000, 2);

        let mut source = make_test_source(1_000_000, 2);

        assert_eq!(source.eval(), expected_zset);

        // Calling start_clock begins the events again (manually setting the
        // wallclock base time again for the test).
        source.clock_start(1);

        assert_eq!(source.eval(), expected_zset);
    }

    // After exhausting events, the source indicates a fixed point.
    #[test]
    fn test_fixed_point() {
        let mut source = make_test_source(1_000_000, 1);

        source.eval();

        assert!(source.fixedpoint(1));
    }

    // After exhausting events, the source returns empty ZSets.
    #[test]
    fn test_eval_empty_zset() {
        let mut source = make_test_source(1_000_000, 1);

        source.eval();

        assert_eq!(source.eval(), OrdZSet::empty(()));
    }

    #[test]
    fn test_nexmark_dbsp_source_full_batch() {
        let root = Root::build(move |circuit| {
            let source = make_test_source(1_000_000, 10);

            let expected_zset = generate_expected_zset(1_000_000, 10);

            circuit
                .add_source(source)
                .inspect(move |data: &OrdZSet<Event, isize>| {
                    assert_eq!(data, &expected_zset);
                });
        })
        .unwrap();

        root.step().unwrap();
    }

    // TODO: Figure out best way to test when not all events are in the past,
    // given that the code uses `now()` - perhaps pass in implementation so
    // tests can use a canned `now()`?
}
