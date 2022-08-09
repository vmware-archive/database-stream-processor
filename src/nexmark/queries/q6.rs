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
    let auctions_by_id = input.flat_map_index(|event| match event {
        Event::Auction(a) => Some((a.id, (a.seller, a.date_time, a.expires))),
        _ => None,
    });

    // Select bids and index by auction id.
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, (b.price, b.date_time))),
        _ => None,
    });

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
        bids_for_auctions_indexed.aggregate_incremental(|&key, vals| -> (u64, usize) {
            // `vals` is sorted in ascending order for each key, so we can
            // just grab the last one.
            let (&max, _) = vals.last().unwrap();
            (key.1, max)
        });
    let winning_bids_by_seller_indexed = winning_bids_by_seller.index();

    // Finally, calculate the average winning bid per seller, using the last
    // 10 closed auctions.
    // TODO: use linear aggregation when ready (#138).
    winning_bids_by_seller_indexed.aggregate_incremental(|&key, vals| -> (u64, usize) {
        let num_items = vals.len();
        // TODO: Once initial test issue solved, add test to ensure only last
        // 10 auctions are considered then update here to get it passing.
        let sum = vals.drain(..).map(|(bid, _)| bid).sum::<usize>();
        (key, sum / num_items)
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
    fn test_q6_average_bids_per_seller_one_auction() {
        let root = Root::build(move |circuit| {
            let mut source = vec![
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
                zset! {
                    // Should not be repeating the auction here - but the
                    // current query requires it apparently - because the join
                    // is on the zset diff representations, and so there is
                    // nothing to join on if there's not a (new) auction here...
                    // find out how to join when we have new bids but no change
                    // on the auction.
                    // Event::Auction(Auction {
                    //     id: 1,
                    //     seller: 99,
                    //     expires: 10_000,
                    //     ..make_auction()
                    // }) => 1,
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 9_000,
                        price: 200,
                        ..make_bid()
                    }) => 1,
                },
            ]
            .into_iter();

            let mut expected_output =
                vec![zset! { (99, 100) => 1 }, zset! {(99, 150) => 1 }].into_iter();

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
}
