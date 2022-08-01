//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]
use std::{cell::Cell, rc::Rc, time::Instant};

use anyhow::Result;
use clap::Parser;
use dbsp::{
    circuit::{Circuit, Root},
    nexmark::{
        config::Config as NexmarkConfig,
        generator::config::Config as GeneratorConfig,
        model::Event,
        queries::{q0, q1, q2, q3, q4},
        NexmarkSource,
    },
    trace::{ord::OrdZSet, BatchReader},
};
use rand::prelude::ThreadRng;

// TODO: Ideally these macros would be in a separate `lib.rs` in this benchmark
// crate, but benchmark binaries don't appear to work like that (in that, I
// haven't yet found a way to import from a `lib.rs` in the same directory as
// the benchmark's `main.rs`)

/// Returns a closure for a circuit with the nexmark source that sets
/// `max_events_reached` once no more data is available.
macro_rules! nexmark_circuit {
    ( $q:expr, $generator_config:expr, $max_events_reached:expr ) => {
        |circuit: &mut Circuit<()>| {
            let source =
                NexmarkSource::<ThreadRng, isize, OrdZSet<Event, isize>>::new($generator_config);
            let input = circuit.add_source(source);

            let output = $q(input);

            output.inspect(move |zs: &OrdZSet<_, _>| {
                // Turns out we can't count events by accumulating the lengths of
                // the `OrdZSet` since duplicate bids are not that difficult to produce in the
                // generator (0-3 per 1000), which get merged to a single Item in the `OrdZSet`
                // with an adjusted weight. Instead, stop when we get empty sets.
                // TODO(absoludity): Nope, can't do that either as some streams
                // return empty sets for initial data (eg, q2 doesn't emit data until the 123rd
                // auction, which is well beyond 1000 generated events).
                println!("zset: {:?}", zs);
                if zs.len() == 0 {
                    $max_events_reached.set(true);
                }
            });
        }
    };
}

macro_rules! run_query {
    ( $q:expr, $generator_config:expr ) => {{
        // Until we have the I/O API to control the running of circuits,
        // use an Rc<Cell<bool>> to communicate when the test is finished (ie. all
        // events processed).
        let max_events_reached = Rc::new(Cell::new(false));
        let max_events_reached_cloned = max_events_reached.clone();

        let circuit = nexmark_circuit!($q, $generator_config, max_events_reached);

        let root = Root::build(circuit).unwrap();

        let start = Instant::now();
        loop {
            if max_events_reached_cloned.get() {
                break;
            }
            root.step().unwrap();
        }
        start.elapsed().as_millis()
    }};
}

macro_rules! run_queries {
    ( $generator_config:expr, $max_events:expr, $queries_to_run:expr, $( ($q_name:expr, $q:expr) ),+ ) => {{
        $(
        if $queries_to_run.len() == 0 || $queries_to_run.contains(&$q_name) {
            println!("Starting {} bench of {} events...", $q_name, $max_events);

            let elapsed_ms = run_query!($q, $generator_config.clone());

            println!(
                "{} completed {} events in {}ms",
                $q_name, $max_events, elapsed_ms
            );
        }
        )+
    }};
}

// TODO(absoludity): Some tools mentioned at
// https://nnethercote.github.io/perf-book/benchmarking.html but as had been
// said earlier, most are more suited to micro-benchmarking.  I assume that our
// best option for comparable benchmarks will be to try to do exactly what the
// Java implementation does: core(s) * time [see Run
// Nexmark](https://github.com/nexmark/nexmark#run-nexmark).  Right now, just
// grab elapsed time for each query run.  See
// https://github.com/matklad/t-cmd/blob/master/src/main.rs Also CpuMonitor.java
// in nexmark (binary that uses procfs to get cpu usage ever 100ms?)

fn main() -> Result<()> {
    let nexmark_config = NexmarkConfig::parse();
    let max_events = nexmark_config.max_events;
    let queries_to_run = nexmark_config.query.clone();
    let generator_config = GeneratorConfig::new(nexmark_config, 0, 0, 0);

    run_queries!(
        generator_config,
        max_events,
        queries_to_run,
        (String::from("q0"), q0),
        (String::from("q1"), q1),
        (String::from("q2"), q2),
        (String::from("q3"), q3),
        (String::from("q4"), q4)
    );

    Ok(())
}
