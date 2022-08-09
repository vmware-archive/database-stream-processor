use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

/// Query 6:
/// Query 6: Average Selling Price by Seller
///
/// What is the average selling price per seller for their last 10 closed
/// auctions. Shares the same ‘winning bids’ core as for Query4, and illustrates
/// a specialized combiner.
///
/// From [Nexmark q6.sql](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q6.sql)
///
/// ```
/// CREATE TABLE discard_sink (
///   seller VARCHAR,
///   avg_price  BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// -- TODO: this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't
/// --  support to consume retractions.
/// INSERT INTO discard_sink
/// SELECT
///     Q.seller,
///     AVG(Q.final) OVER
///         (PARTITION BY Q.seller ORDER BY Q.dateTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
/// FROM (
///     SELECT MAX(B.price) AS final, A.seller, B.dateTime
///     FROM auction AS A, bid AS B
///     WHERE A.id = B.auction and B.dateTime between A.dateTime and A.expires
///     GROUP BY A.id, A.seller
/// ) AS Q;
/// ```

type Q6Stream = Stream<Circuit<()>, OrdZSet<(u64, usize), isize>>;

pub fn q6(input: NexmarkStream) -> Q6Stream {
    // Select auctions sellers and index by auction id.
    let auctions_by_id = input
        .flat_map_index(|event| match event {
            Event::Auction(a) => Some((a.id, (a.seller, a.date_time, a.expires))),
            _ => None,
        })
        .integrate();

    // Select bids and index by auction id.
    let bids_by_auction = input
        .flat_map_index(|event| match event {
            Event::Bid(b) => Some((b.auction, (b.price, b.date_time))),
            _ => None,
        })
        .integrate();

    type BidsAuctionsJoin =
        Stream<Circuit<()>, OrdZSet<((u64, u64, u64, u64), (usize, u64)), isize>>;

    // Join to get bids for each auction.
    let bids_for_auctions: BidsAuctionsJoin = auctions_by_id.join::<(), _, _, _>(
        &bids_by_auction,
        |&auction_id, &(seller, a_date_time, a_expires), &(bid_price, bid_date_time)| {
            (
                (auction_id, seller, a_date_time, a_expires),
                (bid_price, bid_date_time),
            )
        },
    );

    // Filter out the invalid bids while indexing.
    // TODO: update to use incremental version of `join_range` once implemented
    // (#137).
    let bids_for_auctions_indexed = bids_for_auctions.flat_map_index(
        |&((auction_id, seller, a_date_time, a_expires), (bid_price, bid_date_time))| {
            if bid_date_time >= a_date_time && bid_date_time <= a_expires {
                Some(((auction_id, seller), bid_price))
            } else {
                None
            }
        },
    );

    // winning_bids_by_seller: once we have the winning bids, we don't
    // need the auction ids anymore.
    // TODO: We can optimize this given that there are no deletions, as DBSP
    // doesn't need to keep records of the bids for future max calculations.
    let winning_bids_by_seller: Stream<Circuit<()>, OrdZSet<(u64, usize), isize>> =
        bids_for_auctions_indexed.aggregate(|&key, vals| -> (u64, usize) {
            // `vals` is sorted in ascending order for each key, so we can
            // just grab the last one.
            let (&max, _) = vals.last().unwrap();
            (key.1, max)
        });
    let winning_bids_by_seller_indexed = winning_bids_by_seller.index();

    // Finally, calculate the average winning bid per seller, using the last
    // 10 closed auctions.
    // TODO: use linear aggregation when ready (#138).
    winning_bids_by_seller_indexed.aggregate(|&key, vals| -> (u64, usize) {
        // We need to take into account the weight of each zset for the number of items
        // and average calculation.
        println!("key: {key:?}, vals: {vals:?}");
        let num_items = vals.iter().map(|(_, count)| count).sum::<isize>();
        // TODO: Need to update this so the zset is ordered by auction id so that we can
        // take the last 10 (which will also mean we *don't* need to aggregate using the
        // weights as I've done here)
        let sum = vals
            .drain(..)
            .map(|(bid, count)| (*bid) * count as usize) // No deletions
            .sum::<usize>();
        (key, sum / num_items as usize)
    })
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
        zset,
    };

    #[test]
    fn test_q6_average_bids_per_seller_single_seller_single_auction() {
        let root = Root::build(move |circuit| {
            let mut source = vec![
                // The first batch has a single auction for seller 99 with a highest bid of 100
                // (currently).
                zset! {
                    Event::Auction(Auction {
                        id: 1,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 1_000,
                        price: 80,
                        ..make_bid()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                },
                // The second batch has a new highest bid for the (currently) only auction.
                zset! {
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 9_000,
                        price: 200,
                        ..make_bid()
                    }) => 1,
                },
                // The third batch has a new bid but it's not higher, so no effect.
                zset! {
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 9_500,
                        price: 150,
                        ..make_bid()
                    }) => 1,
                },
            ]
            .into_iter();

            let mut expected_output = vec![
                // First batch has a single auction seller with best bid of 100.
                zset! { (99, 100) => 1 },
                // The second batch just updates the best bid for the single auction to 200 (ie. no
                // averaging).
                zset! {(99, 200) => 1 },
                // The third batch has a bid that isn't higher, so no change.
                zset! {(99, 200) => 1 },
            ]
            .into_iter();

            let input: Stream<_, OrdZSet<Event, isize>> =
                circuit.add_source(Generator::new(move || source.next().unwrap()));

            let output = q6(input);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }

    #[test]
    fn test_q6_average_bids_per_seller_single_seller_multiple_auctions() {
        let root = Root::build(move |circuit| {
            let mut source = vec![
                // The first batch has a single auction for seller 99 with a highest bid of 100.
                zset! {
                    Event::Auction(Auction {
                        id: 1,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                },
                // The second batch adds a new auction for the same seller, with
                // a final bid of 200, so the average should be 150 for this seller.
                zset! {
                    Event::Auction(Auction {
                        id: 2,
                        seller: 99,
                        expires: 20_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 2,
                        date_time: 15_000,
                        price: 200,
                        ..make_bid()
                    }) => 1,
                },
            ]
            .into_iter();

            let mut expected_output = vec![
                // First batch has a single auction seller with best bid of 100.
                zset! { (99, 100) => 1 },
                // The second batch adds another auction for the same seller with a final bid of
                // 200, so average is 150.
                zset! {(99, 150) => 1 },
            ]
            .into_iter();

            let input: Stream<_, OrdZSet<Event, isize>> =
                circuit.add_source(Generator::new(move || source.next().unwrap()));

            let output = q6(input);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }

    #[test]
    fn test_q6_average_bids_per_seller_single_seller_more_than_10_auctions() {
        let root = Root::build(move |circuit| {
            let mut source = vec![
                // The first batch has 5 auctions all with single bids of 100, except
                // the first which is at 200.
                zset! {
                    Event::Auction(Auction {
                        id: 1,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 2_000,
                        price: 200,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 2,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 2,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 3,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 3,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 4,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 4,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 5,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 5,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                },
                // The second batch has another 5 auctions all with single bids of 100.
                zset! {
                    Event::Auction(Auction {
                        id: 6,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 6,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 7,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 7,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 8,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 8,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 9,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 9,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                    Event::Auction(Auction {
                        id: 10,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 10,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                },
                // The third batch has a single auction and bid of 100. The last
                // 10 auctions all have 100 now, so average is 100.
                zset! {
                    Event::Auction(Auction {
                        id: 11,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }) => 1,
                    Event::Bid(Bid {
                        auction: 11,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }) => 1,
                },
            ]
            .into_iter();

            let mut expected_output = vec![
                // First has 5 auction for person 99, but average is (200 + 100 * 4) / 5.
                zset! { (99, 120) => 1 },
                // Second batch adds another 5 auctions for person 99, but average is still 100.
                zset! {(99, 110) => 1 },
                // Third batch adds a single auction with bid of 100, pushing
                // out the first bid so average is now 100.
                zset! {(99, 100) => 1 },
            ]
            .into_iter();

            let input: Stream<_, OrdZSet<Event, isize>> =
                circuit.add_source(Generator::new(move || source.next().unwrap()));

            let output = q6(input);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}
