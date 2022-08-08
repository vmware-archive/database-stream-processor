use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

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

type Q5Stream = Stream<Circuit<()>, OrdZSet<(u64, usize), isize>>;

const WINDOW_WIDTH_SECONDS: u64 = 10;

pub fn q5(input: NexmarkStream) -> Q5Stream {
    // I don't think the current API allows me to have a sliding 10s window that is
    // only updated every 2 seconds... the closest we can do is a sliding 10s
    // window that is updated incrementally as new data arrives.

    // Question: How do I get the latest bid's date_time (later, rounded to 2000s).
    // What I'm doing here is crazy - indexing on a constant to get the max
    // timestamp, but not sure what else is available.
    let all_bids_grouped_by_const = input
        .flat_map(|event| match event {
            Event::Bid(b) => Some((1, b.date_time)),
            _ => None,
        })
        .index();
    let window_bounds_zset: Stream<_, OrdZSet<(u64, u64), isize>> = all_bids_grouped_by_const
        .aggregate_incremental(|&key, vals| -> (u64, u64) {
            let (&latest, _) = vals.last().unwrap();
            (latest.saturating_sub(WINDOW_WIDTH_SECONDS * 1000), latest)
        });

    // Question: How do I convert a Stream<_, OrdZSet<(u64, u64), isize>> to a
    // Stream<_, (u64, u64)> as required by `window`? If I try, I get `Batch` trait
    // not implemented for (u64, u64).
    let window_bound: Stream<_, (u64, u64)> =
        window_bounds_zset.map_generic(|(start, end)| (start, end));

    // All bids indexed by date time to be able to window the result.
    let all_bids = input
        .flat_map(|event| match event {
            Event::Bid(b) => Some((b.date_time, b.auction)),
            _ => None,
        })
        .index();
    let windowed_bids = all_bids.window(&window_bound);

    // Switch to index on the auction rather than the timestamp.
    // Simpler way to switch the tuple index?
    let windowed_bids_by_auction = windowed_bids
        .map(|(date_time, auction)| (auction, date_time))
        .index();

    // Should then be able to just return the aggregation for each auction.
    let auction_counts =
        windowed_bids_by_auction.aggregate_incremental(|&key, vals| -> (u64, usize) {
            println!("key: {:?}, vals: {:?}", key, vals);
            (key, vals.len())
        });

    auction_counts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        circuit::{Root, Stream},
        nexmark::{
            generator::tests::{make_auction, make_bid},
            model::{Auction, Bid, Event},
        },
        operator::Generator,
        trace::ord::OrdIndexedZSet,
        zset,
    };

    #[test]
    fn test_q5_windows_from_latest_bid() {
        let root = Root::build(move |circuit| {
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
        .unwrap();

        for _ in 0..1 {
            root.step().unwrap();
        }
    }

    #[test]
    fn test_q5_contains_hottest_auctions() {
        let root = Root::build(move |circuit| {
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
        .unwrap();

        for _ in 0..1 {
            root.step().unwrap();
        }
    }
}
