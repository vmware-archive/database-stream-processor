//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]

#[cfg(unix)]
use libc::{getrusage, rusage, timeval};
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
    trace::ord::OrdZSet,
    Circuit, CollectionHandle, DBSPHandle, Runtime,
};
use num_format::{Locale, ToFormattedString};
use pbr::ProgressBar;
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

            output.inspect(move |_zs: &OrdZSet<_, _>| ());

            input_handle
        }
    };
}

/// Currently just the elapsed time, but later add CPU and Mem.
#[derive(Default)]
struct NexmarkResult {
    name: String,
    num_events: u64,
    elapsed: Duration,
    total_usr_cpu: Duration,
    total_sys_cpu: Duration,
    input_usr_cpu: Duration,
    input_sys_cpu: Duration,
    max_rss: Option<u64>,
}

struct InputStats {
    num_events: u64,
    usr_cpu: Duration,
    sys_cpu: Duration,
}

fn spawn_dbsp_consumer(
    mut dbsp: DBSPHandle,
    input_complete_rx: mpsc::Receiver<()>,
    processing_complete_tx: mpsc::SyncSender<()>,
) {
    thread::spawn(move || {
        let mut count = 0;
        loop {
            dbsp.step().unwrap();
            count += 1;
            println!("Step called {count} times");

            // When the input is complete, we do one final step and return.
            if let Ok(()) = input_complete_rx.try_recv() {
                dbsp.step().unwrap();
                count += 1;
                println!("Step called {count} times");
                processing_complete_tx.send(()).unwrap();
                return;
            }
        }
    });
}

fn spawn_source_producer(
    generator_config: GeneratorConfig,
    mut input_handle: CollectionHandle<Event, isize>,
    input_ready_tx: mpsc::SyncSender<()>,
    source_exhausted_tx: mpsc::SyncSender<InputStats>,
) {
    thread::spawn(move || {
        let mut progress_bar = ProgressBar::new(generator_config.nexmark_config.max_events);
        let source =
            NexmarkSource::<ThreadRng, isize, OrdZSet<Event, isize>>::new(generator_config);
        let mut num_events: u64 = 0;

        for mut batch in source {
            let batch_len = batch.len() as u64;
            input_handle.append(&mut batch);
            if num_events == 0 {
                input_ready_tx.send(()).unwrap();
            }
            num_events += batch_len;
            progress_bar.add(batch_len);
        }
        let (input_usr_cpu, input_sys_cpu, _) = unsafe { rusage(libc::RUSAGE_THREAD) };
        source_exhausted_tx
            .send(InputStats {
                num_events,
                usr_cpu: input_usr_cpu,
                sys_cpu: input_sys_cpu,
            })
            .unwrap();
        progress_bar.finish_print("Done");
    });
}

macro_rules! run_query {
    ( $q:expr, $generator_config:expr) => {{
        let circuit_closure = nexmark_circuit!($q);

        let num_cores = $generator_config.nexmark_config.cpu_cores;
        let (dbsp, input_handle) = Runtime::init_circuit(num_cores, circuit_closure).unwrap();

        // Start the generator inputting the specified number of batches to the circuit
        // whenever it receives a message.
        let (source_exhausted_tx, source_exhausted_rx) = mpsc::sync_channel(1);
        let (input_ready_tx, input_ready_rx) = mpsc::sync_channel(1);
        spawn_source_producer(
            $generator_config,
            input_handle,
            input_ready_tx,
            source_exhausted_tx,
        );

        // Wait until some input has been added before starting the consumer.
        input_ready_rx.recv().unwrap();
        let (input_complete_tx, input_complete_rx) = mpsc::sync_channel(1);
        let (processing_complete_tx, processing_complete_rx) = mpsc::sync_channel(1);
        spawn_dbsp_consumer(dbsp, input_complete_rx, processing_complete_tx);

        // Wait for the source to be exhausted, then let the consumer know it can
        // finishe up too and wait for it to complete.
        let input_stats = source_exhausted_rx.recv().unwrap();
        input_complete_tx.send(()).unwrap();
        processing_complete_rx.recv().unwrap();

        // Return the user/system CPU overhead from the generator/input thread.
        NexmarkResult {
            num_events: input_stats.num_events,
            input_usr_cpu: input_stats.usr_cpu,
            input_sys_cpu: input_stats.sys_cpu,
            ..NexmarkResult::default()
        }
    }};
}

macro_rules! run_queries {
    ( $generator_config:expr, $max_events:expr, $queries_to_run:expr, $( ($q_name:expr, $q:expr) ),+ ) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();
        // We have no way (currently) of finding the max memory usage for each
        // subsequent query as the value is for the process. So only the first
        // query will have a value.
        let mut query_count = 0;
        $(
        if $queries_to_run.len() == 0 || $queries_to_run.contains(&$q_name.to_string()) {
            query_count += 1;
            println!("Starting {} bench of {} events...", $q_name, $max_events);

            let start = Instant::now();
            let (before_usr_cpu, before_sys_cpu, before_max_rss) = unsafe { rusage(libc::RUSAGE_SELF) };

            let thread_generator_config = $generator_config.clone();
            let result = run_query!($q, thread_generator_config);
            let (after_usr_cpu, after_sys_cpu, after_max_rss) = unsafe { rusage(libc::RUSAGE_SELF) };
            results.push(NexmarkResult {
                name: $q_name.to_string(),
                total_usr_cpu: after_usr_cpu - before_usr_cpu,
                total_sys_cpu: after_sys_cpu - before_sys_cpu,
                max_rss: match query_count { 1 => Some(after_max_rss - before_max_rss), _ => None},
                elapsed: start.elapsed(),
                ..result
            });
        }
        )+
        results
    }};
}

fn create_ascii_table() -> AsciiTable {
    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(200);
    ascii_table.column(0).set_header("Query");
    ascii_table.column(1).set_header("#Events");
    ascii_table.column(2).set_header("Cores");
    ascii_table.column(3).set_header("Elapsed");
    ascii_table.column(4).set_header("Cores * Elapsed");
    ascii_table.column(5).set_header("Throughput/Cores");
    ascii_table.column(6).set_header("Input Usr CPU");
    ascii_table.column(7).set_header("Input Sys CPU");
    ascii_table.column(8).set_header("DBSP Usr CPU");
    ascii_table.column(9).set_header("DBSP Sys CPU");
    ascii_table.column(10).set_header("Max RSS(Kb)");
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
            format!("{cpu_cores}"),
            format!("{0:.3}s", r.elapsed.as_secs_f32()),
            format!("{0:.3}s", cpu_cores as f32 * r.elapsed.as_secs_f32()),
            format!(
                "{0:.3} K/s",
                r.num_events as f32 / r.elapsed.as_secs_f32() / cpu_cores as f32 / 1000.0
            ),
            format!("{0:.3}s", r.input_usr_cpu.as_secs_f32()),
            format!("{0:.3}s", r.input_sys_cpu.as_secs_f32()),
            format!("{0:.3}s", (r.total_usr_cpu - r.input_usr_cpu).as_secs_f32()),
            format!("{0:.3}s", (r.total_sys_cpu - r.input_sys_cpu).as_secs_f32()),
            format!(
                "{}",
                if let Some(max_rss) = r.max_rss {
                    max_rss.to_formatted_string(&Locale::en)
                } else {
                    "N/A".to_string()
                }
            ),
        ]
    }));

    Ok(())
}

#[cfg(unix)]
fn duration_for_timeval(tv: timeval) -> Duration {
    Duration::new(tv.tv_sec as u64, tv.tv_usec as u32 * 1_000)
}

/// Returns the user CPU, system CPU and maxrss (in Kb) for the current process.
#[cfg(unix)]
pub unsafe fn rusage(target: i32) -> (Duration, Duration, u64) {
    let mut ru: MaybeUninit<rusage> = MaybeUninit::uninit();
    let err_code = getrusage(target, ru.as_mut_ptr());
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
