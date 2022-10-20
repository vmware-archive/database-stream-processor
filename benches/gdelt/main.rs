mod data;
mod personal_network;

use crate::data::{get_gkg_file, get_master_file, parse_personal_network_gkg, GKG_SUFFIX};
use arcstr::literal;
use clap::Parser;
use dbsp::{
    trace::{BatchReader, Cursor},
    Circuit, Runtime,
};
use hashbrown::{HashMap, HashSet};
use std::{
    cmp::Reverse,
    io::{BufRead, BufReader, Write},
    num::NonZeroUsize,
    panic::{self, AssertUnwindSafe},
    sync::atomic::{AtomicBool, Ordering},
    thread,
};
use xxhash_rust::xxh3::Xxh3Builder;

static FINISHED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Parser)]
struct Args {
    /// The number of threads to use for the dataflow, defaults to the
    /// number of cores the current machine has
    #[clap(long)]
    threads: Option<NonZeroUsize>,

    /// The number of 15-minute batches to ingest
    #[clap(long, default_value = "20")]
    batches: NonZeroUsize,

    // When running with `cargo bench` the binary gets the `--bench` flag, so we
    // have to parse and ignore it so clap doesn't get angry
    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}

fn main() {
    let args = Args::parse();
    let threads = args
        .threads
        .or_else(|| thread::available_parallelism().ok())
        .map(NonZeroUsize::get)
        .unwrap_or(1);
    let batches = args.batches.get();

    Runtime::run(threads, move || {
        let (root, mut handle) = Circuit::build(|circuit| {
            let (events, handle) = circuit.add_input_zset();

            let person = literal!("joe biden");
            let mut network_buf = Vec::with_capacity(4096);

            personal_network::personal_network(person, None, &events)
                .gather(0)
                .inspect(move |network| {
                    if !network.is_empty() {
                        let mut cursor = network.cursor();
                        while cursor.key_valid() {
                            if cursor.val_valid() {
                                let count = cursor.weight();
                                let (source, target) = cursor.key().clone();
                                network_buf.push((source, target, count));
                            }
                            cursor.step_key();
                        }

                        if !network_buf.is_empty() {
                            network_buf.sort_unstable_by_key(|&(.., count)| Reverse(count));

                            let mut stdout = std::io::stdout().lock();

                            writeln!(stdout, "Network:").unwrap();
                            for (source, target, count) in network_buf.drain(..) {
                                writeln!(stdout, "- {source}, {target}, {count}").unwrap();
                            }
                            writeln!(stdout).unwrap();

                            stdout.flush().unwrap();
                        }
                    }
                });

            handle
        })
        .unwrap();

        if Runtime::worker_index() == 0 {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let file_urls = BufReader::new(get_master_file())
                    .lines()
                    .filter_map(|line| {
                        let line = line.unwrap();
                        line.ends_with(GKG_SUFFIX)
                            .then(|| line.split(' ').last().unwrap().to_owned())
                    });

                let mut interner = HashSet::with_capacity_and_hasher(4096, Xxh3Builder::new());
                let normalizations = {
                    // I have no idea why gdelt does this sometimes, but it does
                    let normals = [
                        ("a harry truman", literal!("harry truman")),
                        ("a ronald reagan", literal!("ronald reagan")),
                        ("a lyndon johnson", literal!("lyndon johnson")),
                        ("a sanatan dharam", literal!("sanatan dharam")),
                        ("b richard nixon", literal!("richard nixon")),
                        ("b dwight eisenhower", literal!("dwight eisenhower")),
                        ("c george w bush", literal!("george w bush")),
                        ("c gerald ford", literal!("gerald ford")),
                        ("c john f kennedy", literal!("john f kennedy")),
                        // I can't even begin to explain this one
                        ("obama jeb bush", literal!("jeb bush")),
                    ];

                    let mut map =
                        HashMap::with_capacity_and_hasher(normals.len(), Xxh3Builder::new());
                    map.extend(normals);
                    map
                };

                let mut ingested = 0;
                for url in file_urls {
                    if ingested >= batches {
                        break;
                    }

                    if let Some(file) = get_gkg_file(&url) {
                        parse_personal_network_gkg(
                            &mut handle,
                            &mut interner,
                            &normalizations,
                            file,
                        );

                        println!("ingesting batch {}/{batches}", ingested + 1);
                        root.step().unwrap();

                        ingested += 1;
                    }
                }
            }));

            FINISHED.store(true, Ordering::Release);
            if let Err(panic) = result {
                panic::resume_unwind(panic);
            }
        } else {
            let mut current_batch = 0;
            while !FINISHED.load(Ordering::Acquire) && current_batch < batches {
                root.step().unwrap();
                current_batch += 1;
            }
        }
    })
    .join()
    .unwrap();
}
