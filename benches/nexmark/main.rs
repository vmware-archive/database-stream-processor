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
    Circuit, Runtime,
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
                // Currently using the print below not only to get an idea of the batch
                // size but also to see how much time is spent creating the zsets from
                // the input.
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

macro_rules! run_query {
    ( $q_name:expr, $q:expr, $generator_config:expr, $result_tx:expr ) => {{
        let circuit_closure = nexmark_circuit!($q);

        let num_cores = $generator_config.nexmark_config.cpu_cores;
        let (mut dbsp, mut input_handle) =
            Runtime::init_circuit(num_cores, circuit_closure).unwrap();

        let source =
            NexmarkSource::<ThreadRng, isize, OrdZSet<Event, isize>>::new($generator_config);

        let mut num_events_generated: u64 = 0;
        let start = Instant::now();

        for mut batch in source {
            num_events_generated += batch.len() as u64;
            input_handle.append(&mut batch);
            dbsp.step().unwrap();
        }

        let (usr_cpu, sys_cpu, max_rss) = unsafe { rusage_thread() };

        $result_tx
            .send(NexmarkResult {
                name: $q_name.to_string(),
                num_events: num_events_generated,
                elapsed: start.elapsed(),
                sys_cpu,
                usr_cpu,
                max_rss,
            })
            .unwrap();
    }};
}

macro_rules! run_queries {
    ( $generator_config:expr, $max_events:expr, $queries_to_run:expr, $( ($q_name:expr, $q:expr) ),+ ) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();

        // Run each query in a separate thread so we can measure the resource
        // usage of the thread in isolation. We'll communicate the resource usage
        // for collection via a channel to accumulate here.
        let (result_tx, result_rx): (mpsc::Sender<NexmarkResult>, mpsc::Receiver<NexmarkResult>) =
            mpsc::channel();

        $(
        if $queries_to_run.len() == 0 || $queries_to_run.contains(&$q_name.to_string()) {
            println!("Starting {} bench of {} events...", $q_name, $max_events);
            let thread_result_tx = result_tx.clone();
            let thread_generator_config = $generator_config.clone();
            thread::spawn(move || {
                run_query!($q_name, $q, thread_generator_config, thread_result_tx);
            });
            // Wait for the thread to finish then collect the result.
            results.push(result_rx.recv().unwrap());
        }
        )+
        results
    }};
}

fn create_ascii_table() -> AsciiTable {
    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(120);
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
