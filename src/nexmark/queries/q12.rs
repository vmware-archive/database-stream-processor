use super::{process_time, NexmarkStream};
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

///
/// Query 12: Processing Time Windows (Not in original suite)
///
/// How many bids does a user make within a fixed processing time limit?
/// Illustrates working in processing time window.
///
/// Group bids by the same user into processing time windows of 10 seconds.
/// Emit the count of bids per window.
///
/// ```sql
/// CREATE TABLE discard_sink (
///   bidder BIGINT,
///   bid_count BIGINT,
///   starttime TIMESTAMP(3),
///   endtime TIMESTAMP(3)
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     B.bidder,
///     count(*) as bid_count,
///     TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,
///     TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime
/// FROM (SELECT *, PROCTIME() as p_time FROM bid) B
/// GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND);
/// ```

type Q12Stream = Stream<Circuit<()>, OrdZSet<(u64, u64, u64, u64), isize>>;
const TUMBLE_SECONDS: u64 = 10;

fn window_for_process_time(ptime: u64) -> (u64, u64) {
    let window_lower = ptime - (ptime % (TUMBLE_SECONDS * 1000));
    (window_lower, window_lower + TUMBLE_SECONDS * 1000)
}

// This function enables us to test the q12 functionality without using the
// actual process time, while the actual q12 function below uses the real
// process time.
// TODO: I originally planned to pass a FnMut closure for process_time that
// just emits a new u64 each time it is called, but can't do this as the
// closure of `flat_map_index` is Fn not FnMut, and would need to capture the
// process_time closure. So right now, it's quite ugly: to avoid an `FnMut`,
// I'm instead passing an optional vector of times with the assumption that
// those times are indexed by the bid.auction.
// There must be a better way without resorting to interior mutability? Anyway,
// it works for the tests and is only used in the tests.
fn q12_for_process_time(input: NexmarkStream, process_times: Option<Vec<u64>>) -> Q12Stream {
    let bids_by_bidder_window = input.flat_map_index(move |event| match event {
        // TODO: Can I call process_time() just once per batch, rather than for every Bid? How?
        Event::Bid(b) => {
            let t = match &process_times {
                Some(v) => v[b.auction as usize],
                None => process_time(),
            };
            let (starttime, endtime) = window_for_process_time(t);
            Some(((b.bidder, starttime, endtime), ()))
        }
        _ => None,
    });

    bids_by_bidder_window
        .aggregate_linear::<(), _, _>(|&_key, &()| -> isize { 1 })
        .map(|(&(bidder, starttime, endtime), &count)| (bidder, count as u64, starttime, endtime))
}

pub fn q12(input: NexmarkStream) -> Q12Stream {
    q12_for_process_time(input, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nexmark::{
            generator::tests::make_bid,
            model::{Bid, Event},
        },
        zset, Circuit,
    };
    use rstest::rstest;

    #[rstest]
    #[case::one_bidder_single_window(
        vec![vec![(1, 0), (1, 1), (1, 2), (1, 3)], vec![(1, 4), (1, 5)]],
        vec![3_000, 4_000, 5_000, 6_000, 7_000, 8_000],
        vec![
            zset! {(1, 4, 0, 10_000) => 1},
            zset! { (1, 4, 0, 10_000) => -1, (1, 6, 0, 10_000) => 1},
        ],
    )]
    #[case::one_bidder_multiple_windows(
        vec![vec![(1, 0), (1, 1), (1, 2), (1, 3)], vec![(1, 4), (1, 5)]],
        vec![3_000, 4_000, 5_000, 6_000, 11_000, 12_000],
        vec![
            zset! {(1, 4, 0, 10_000) => 1},
            zset! {(1, 2, 10_000, 20_000) => 1},
        ],
    )]
    #[case::multiple_bidders_multiple_windows(
        vec![vec![(1, 0), (1, 1), (1, 2), (1, 3), (2, 5), (2, 6)], vec![(1, 7), (1, 8)]],
        vec![3_000, 4_000, 5_000, 6_000, 7_000, 8_000, 9_000, 11_000, 12_000],
        vec![
            zset! {(1, 4, 0, 10_000) => 1, (2, 2, 0, 10_000) => 1},
            zset! {(1, 2, 10_000, 20_000) => 1},
        ],
    )]
    fn test_q12(
        #[case] bidder_bid_batches: Vec<Vec<(u64, u64)>>,
        #[case] proc_times: Vec<u64>,
        #[case] expected_zsets: Vec<OrdZSet<(u64, u64, u64, u64), isize>>,
    ) {
        let input_vecs = bidder_bid_batches.into_iter().map(|batch| {
            batch
                .into_iter()
                .map(|(bidder, auction)| {
                    (
                        Event::Bid(Bid {
                            bidder,
                            auction,
                            ..make_bid()
                        }),
                        1,
                    )
                })
                .collect()
        });

        let (circuit, mut input_handle) = Circuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = q12_for_process_time(stream, Some(proc_times));

            let mut expected_output = expected_zsets.into_iter();
            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            input_handle
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }
}
