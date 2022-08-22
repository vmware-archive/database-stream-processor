//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]

#[cfg(unix)]
use libc::{getrusage, rusage, timeval, RUSAGE_THREAD};
use std::{
    io::Error,
    mem::MaybeUninit,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use ascii_table::AsciiTable;
use clap::Parser;
use dbsp::{
    nexmark::{
        config::Config as NexmarkConfig,
        generator::config::Config as GeneratorConfig,
        model::Event,
        queries::{q0, q1, q2, q3, q4, q6},
        NexmarkSource,
    },
    trace::{ord::OrdZSet, BatchReader},
    Circuit, CollectionHandle, DBSPHandle, Runtime,
};
use num_format::{Locale, ToFormattedString};
use rand::prelude::ThreadRng;

// TODO: Ideally these macros would be in a separate `lib.rs` in this benchmark
// crate, but benchmark binaries don't appear to work like that (in that, I
// haven't yet found a way to import from a `lib.rs` in the same directory as
// the benchmark's `main.rs`)

/// Returns a closure for a circuit with the nexmark source that returns
/// the input handle.
macro_rules! nexmark_circuit {
    ( $q:expr ) => {
        |circuit: &mut Circuit<()>| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = $q(stream);

            output.inspect(move |zs: &OrdZSet<_, _>| {
                // Uncomment the println! below to see how the zsets are growing (or
                // not) over time.
                println!("zs.len() = {}", zs.len());
            });

            input_handle
        }
    };
}

/// Currently just the elapsed time, but later add CPU and Mem.
struct NexmarkResult {
    name: String,
    num_events: u64,
    elapsed: Duration,
    usr_cpu: Duration,
    sys_cpu: Duration,
    max_rss: u64,
}

enum StepCompleted {
    DBSP,
    Source,
}

fn spawn_dbsp_consumer(
    mut dbsp: DBSPHandle,
    step_do_rx: mpsc::Receiver<()>,
    step_done_tx: mpsc::SyncSender<StepCompleted>,
    resource_usage_tx: mpsc::SyncSender<NexmarkResult>,
) {
    thread::spawn(move || {
        let start = Instant::now();

        while let Ok(()) = step_do_rx.recv() {
            dbsp.step().unwrap();
            step_done_tx.send(StepCompleted::DBSP).unwrap();
        }

        let (usr_cpu, sys_cpu, max_rss) = unsafe { rusage_thread() };

        resource_usage_tx
            .send(NexmarkResult {
                name: String::from(""),
                num_events: 0,
                elapsed: start.elapsed(),
                sys_cpu,
                usr_cpu,
                max_rss,
            })
            .unwrap();
    });
}

fn spawn_source_producer(
    generator_config: GeneratorConfig,
    mut input_handle: CollectionHandle<Event, isize>,
    step_do_rx: mpsc::Receiver<isize>,
    step_done_tx: mpsc::SyncSender<StepCompleted>,
    source_exhausted_tx: mpsc::SyncSender<u64>,
) {
    thread::spawn(move || {
        // Create the source and load up the first batch of input ready for processing.
        let mut source =
            NexmarkSource::<ThreadRng, isize, OrdZSet<Event, isize>>::new(generator_config);
        let mut num_events_generated: u64 = 0;
        let mut batch = source.next().unwrap();
        num_events_generated += batch.len() as u64;
        input_handle.append(&mut batch);

        // Wait for further coordination before adding more input.
        while let Ok(num_batches) = step_do_rx.recv() {
            for _ in 0..num_batches {
                // When the source is exhausted, communicate the number of events generated
                // back.
                let mut batch = match source.next() {
                    Some(b) => b,
                    None => {
                        source_exhausted_tx.send(num_events_generated).unwrap();
                        step_done_tx.send(StepCompleted::Source).unwrap();
                        return;
                    }
                };

                num_events_generated += batch.len() as u64;
                // TODO: Try collecting the batches for a single call to `append` so hashing is
                // done once? Shouldn't make a difference.
                input_handle.append(&mut batch);
            }
            step_done_tx.send(StepCompleted::Source).unwrap();
        }
        source_exhausted_tx.send(num_events_generated).unwrap();
    });
}

fn coordinate_input_and_steps(
    dbsp_step_tx: mpsc::SyncSender<()>,
    source_step_tx: mpsc::SyncSender<isize>,
    step_done_rx: mpsc::Receiver<StepCompleted>,
    source_exhausted_rx: mpsc::Receiver<u64>,
) -> u64 {
    let mut num_input_batches = 1;
    // Continue until the source is exhausted.
    loop {
        match source_exhausted_rx.try_recv() {
            Ok(num_events) => return num_events,
            _ => (),
        }

        // Trigger the step and the input of the next batch.
        dbsp_step_tx.send(()).unwrap();
        source_step_tx.send(num_input_batches).unwrap();

        // If the consumer finished first, increase the input batches.
        match step_done_rx.recv() {
            Ok(StepCompleted::DBSP) => num_input_batches += 1,
            _ => (),
        }
        step_done_rx.recv().unwrap();
    }
}

macro_rules! run_query {
    ( $q_name:expr, $q:expr, $generator_config:expr) => {{
        let circuit_closure = nexmark_circuit!($q);

        let num_cores = $generator_config.nexmark_config.cpu_cores;
        let (dbsp, input_handle) = Runtime::init_circuit(num_cores, circuit_closure).unwrap();

        // Create a channel for the coordinating thread to determine whether the
        // producer or consumer step is completed first.
        let (step_done_tx, step_done_rx) = mpsc::sync_channel(2);

        // Start the DBSP runtime processing steps only when it receives a message to do
        // so. The DBSP processing happens in its own thread where the resource usage
        // calculation can also happen.
        let (dbsp_step_tx, dbsp_step_rx) = mpsc::sync_channel(1);
        let (resource_usage_tx, resource_usage_rx): (
            mpsc::SyncSender<NexmarkResult>,
            mpsc::Receiver<NexmarkResult>,
        ) = mpsc::sync_channel(0);
        spawn_dbsp_consumer(dbsp, dbsp_step_rx, step_done_tx.clone(), resource_usage_tx);

        // Start the generator inputting the specified number of batches to the circuit
        // whenever it receives a message.
        let (source_step_tx, source_step_rx) = mpsc::sync_channel(1);
        let (source_exhausted_tx, source_exhausted_rx) = mpsc::sync_channel(1);
        spawn_source_producer(
            $generator_config,
            input_handle,
            source_step_rx,
            step_done_tx,
            source_exhausted_tx,
        );

        let num_events_generated = coordinate_input_and_steps(
            dbsp_step_tx,
            source_step_tx,
            step_done_rx,
            source_exhausted_rx,
        );

        println!("{num_events_generated} events generated.");

        NexmarkResult {
            name: $q_name.to_string(),
            num_events: num_events_generated,
            ..resource_usage_rx.recv().unwrap()
        }
    }};
}

macro_rules! run_queries {
    ( $generator_config:expr, $max_events:expr, $queries_to_run:expr, $( ($q_name:expr, $q:expr) ),+ ) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();

        $(
        if $queries_to_run.len() == 0 || $queries_to_run.contains(&$q_name.to_string()) {
            println!("Starting {} bench of {} events...", $q_name, $max_events);
            let thread_generator_config = $generator_config.clone();
            results.push(run_query!($q_name, $q, thread_generator_config));
        }
        )+
        results
    }};
}

fn create_ascii_table() -> AsciiTable {
    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(160);
    ascii_table.column(0).set_header("Query");
    ascii_table.column(1).set_header("#Events");
    ascii_table.column(2).set_header("Cores");
    ascii_table.column(3).set_header("Time(s)");
    ascii_table.column(4).set_header("Cores * Time(s)");
    ascii_table.column(5).set_header("Throughput/Cores");
    ascii_table.column(6).set_header("User CPU(s)");
    ascii_table.column(7).set_header("System CPU(s)");
    ascii_table.column(8).set_header("Max RSS(Kb)");
    ascii_table
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

// TODO: Implement for non-unix platforms (mainly removing libc perf stuff)
#[cfg(not(unix))]
fn main() -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn main() -> Result<()> {
    let nexmark_config = NexmarkConfig::parse();
    let max_events = nexmark_config.max_events;
    let queries_to_run = nexmark_config.query.clone();
    let cpu_cores = nexmark_config.cpu_cores;
    let generator_config = GeneratorConfig::new(nexmark_config, 0, 0, 0);

    let results = run_queries!(
        generator_config,
        max_events,
        queries_to_run,
        ("q0", q0),
        ("q1", q1),
        ("q2", q2),
        ("q3", q3),
        ("q4", q4),
        ("q6", q6)
    );

    let ascii_table = create_ascii_table();
    ascii_table.print(results.into_iter().map(|r| {
        vec![
            r.name,
            format!("{}", r.num_events.to_formatted_string(&Locale::en)),
            format!("{}", cpu_cores),
            format!("{0:.3}", r.elapsed.as_secs_f32()),
            format!("{0:.3}", cpu_cores as f32 * r.elapsed.as_secs_f32()),
            format!(
                "{0:.3} K/s",
                r.num_events as f32 / r.elapsed.as_secs_f32() / cpu_cores as f32 / 1000.0
            ),
            format!("{0:.3}", r.usr_cpu.as_secs_f32()),
            format!("{0:.3}", r.sys_cpu.as_secs_f32()),
            format!("{}", r.max_rss.to_formatted_string(&Locale::en)),
        ]
    }));

    Ok(())
}

#[cfg(unix)]
fn duration_for_timeval(tv: timeval) -> Duration {
    Duration::new(tv.tv_sec as u64, tv.tv_usec as u32 * 1_000)
}

/// Returns the user CPU, system CPU and maxrss (in Kb) for the current thread.
#[cfg(unix)]
pub unsafe fn rusage_thread() -> (Duration, Duration, u64) {
    let mut ru: MaybeUninit<rusage> = MaybeUninit::uninit();
    let err_code = getrusage(RUSAGE_THREAD, ru.as_mut_ptr());
    if err_code != 0 {
        panic!("getrusage returned {}", Error::last_os_error());
    }
    let ru = ru.assume_init();
    (
        duration_for_timeval(ru.ru_utime),
        duration_for_timeval(ru.ru_stime),
        ru.ru_maxrss as u64,
    )
}
