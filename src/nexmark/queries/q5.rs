use super::NexmarkStream;
use crate::{
    nexmark::model::Event,
    operator::{FilterMap, Max},
    Circuit, OrdIndexedZSet, OrdZSet, Stream,
};

/// Query 5: Hot Items
///
/// Which auctions have seen the most bids in the last period?
/// Illustrates sliding windows and combiners.
///
/// The original Nexmark Query5 calculate the hot items in the last hour
/// (updated every minute). To make things a bit more dynamic and easier to test
/// we use much shorter windows, i.e. in the last 10 seconds and update every 2
/// seconds.
///
/// From [Nexmark q5.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q5.sql):
///
/// ```
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   num  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT AuctionBids.auction, AuctionBids.num
///  FROM (
///    SELECT
///      B1.auction,
///      count(*) AS num,
///      HOP_START(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
///      HOP_END(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
///    FROM bid B1
///    GROUP BY
///      B1.auction,
///      HOP(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
///  ) AS AuctionBids
///  JOIN (
///    SELECT
///      max(CountBids.num) AS maxn,
///      CountBids.starttime,
///      CountBids.endtime
///    FROM (
///      SELECT
///        count(*) AS num,
///        HOP_START(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
///        HOP_END(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
///      FROM bid B2
///      GROUP BY
///        B2.auction,
///        HOP(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
///      ) AS CountBids
///    GROUP BY CountBids.starttime, CountBids.endtime
///  ) AS MaxBids
///  ON AuctionBids.starttime = MaxBids.starttime AND
///     AuctionBids.endtime = MaxBids.endtime AND
///     AuctionBids.num >= MaxBids.maxn;
/// ```

/// If I am reading Flink docs
/// (https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/)
/// correctly, its default behavior is to trigger computation on
/// a window once the watermark passes the end of the window. Furthermore, since
/// the default "lateness" attribute of a stream is 0 the aggregate won't get
/// updated once the watermark passes the end of the window.  In other words, it
/// will aggregate within each window exactly once, which is what we implement
/// here.

type Q5Stream = Stream<Circuit<()>, OrdZSet<(u64, usize), isize>>;

const WINDOW_WIDTH_SECONDS: u64 = 10;
const TUMBLE_SECONDS: u64 = 2;

pub fn q5(input: NexmarkStream) -> Q5Stream {
    // All bids indexed by date time to be able to window the result.
    let bids_by_time: Stream<_, OrdIndexedZSet<u64, u64, _>> =
        input.flat_map_index(|event| match event {
            Event::Bid(b) => Some((b.date_time, b.auction)),
            _ => None,
        });

    // Extract the largest timestamp from the input stream. We will use it as
    // current time. Set watermark to `TUMBLE_SECONDS` in the past.
    let watermark = bids_by_time.watermark_monotonic(|date_time| date_time - TUMBLE_SECONDS * 1000);

    // 10-second window with 2-second step.
    let window_bounds = watermark.apply(|watermark| {
        let watermark_rounded = *watermark - (*watermark % (TUMBLE_SECONDS * 1000));
        (
            watermark_rounded.saturating_sub(WINDOW_WIDTH_SECONDS * 1000),
            watermark_rounded,
        )
    });

    // Only consider bids within the current window.
    let windowed_bids: Stream<_, OrdZSet<u64, _>> = bids_by_time.window(&window_bounds);

    // Count the number of bids per auction.
    let auction_counts = windowed_bids.aggregate_linear::<(), _, _>(|&_, &()| -> isize {
        //println!("key: {:?}, vals: {:?}", key, vals);
        1
    });

    // Find the largest number of bids across all auctions.
    let max_auction_count = auction_counts
        .map_index(|(_auction, count)| ((), *count))
        .aggregate::<(), _>(Max)
        .map(|((), max_count)| *max_count);

    // Filter out auctions with the largest number of bids.
    // TODO: once the query works, this can be done more efficiently
    // using `apply2`.
    let auction_by_count = auction_counts.map_index(|(auction, count)| (*count, auction.clone()));

    max_auction_count.join::<(), _, _, _>(&auction_by_count, |max_count, &(), &auction| {
        (auction, *max_count as usize)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nexmark::{
            generator::tests::{make_auction, make_bid},
            model::{Auction, Bid, Event},
        },
        operator::Generator,
        zset, Circuit, Stream,
    };

    #[test]
    fn test_q5_windows_from_latest_bid() {
        let root = Circuit::build(move |circuit| {
            type Time = usize;

            let mut source = vec![zset! {
                Event::Auction(Auction {
                    id: 1,
                    ..make_auction()
                }) => 1,
                // This bid should not be included in the aggregate for the
                // first batch, as it's earlier than 12000 - 10000.
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 1000,
                    ..make_bid()
                }) => 1,
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 2000,
                    ..make_bid()
                }) => 1,
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 12000,
                    ..make_bid()
                }) => 1,
            }]
            .into_iter();
            let input: Stream<_, OrdZSet<Event, isize>> =
                circuit.add_source(Generator::new(move || source.next().unwrap()));

            let mut expected_output = vec![zset! { (1, 2) => 1 }].into_iter();

            let output = q5(input);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));
        })
        .unwrap()
        .0;

        for _ in 0..1 {
            root.step().unwrap();
        }
    }

    #[test]
    fn test_q5_contains_hottest_auctions() {
        let root = Circuit::build(move |circuit| {
            type Time = usize;

            let mut source = vec![zset! {
                Event::Auction(Auction {
                    id: 1,
                    ..make_auction()
                }) => 1,
                Event::Auction(Auction {
                    id: 2,
                    ..make_auction()
                }) => 1,
                Event::Auction(Auction {
                    id: 3,
                    ..make_auction()
                }) => 1,
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 1000,
                    price: 80,
                    ..make_bid()
                }) => 1,
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 2000,
                    price: 80,
                    ..make_bid()
                }) => 1,
                Event::Bid(Bid {
                    auction: 1,
                    date_time: 3000,
                    ..make_bid()
                }) => 1,
                Event::Bid(Bid {
                    auction: 2,
                    date_time: 2000,
                    price: 100,
                    ..make_bid()
                }) => 1,
            }]
            .into_iter();

            let mut expected_output = vec![zset! { (1, 3) => 1, (2, 1) => 1 }].into_iter();

            let input: Stream<_, OrdZSet<Event, isize>> =
                circuit.add_source(Generator::new(move || source.next().unwrap()));

            let output = q5(input);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));
        })
        .unwrap()
        .0;

        for _ in 0..1 {
            root.step().unwrap();
        }
    }
}
