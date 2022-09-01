use super::NexmarkStream;
use crate::{
    nexmark::model::Event,
    operator::{FilterMap, Max},
    Circuit, OrdIndexedZSet, OrdZSet, Stream,
};

/// Query 9: Winning Bids (Not in original suite)
///
/// Find the winning bid for each auction.
///
/// From https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/resources/queries/q9.sql
///
/// TODO: streaming join doesn't support rowtime attribute in input, this should
/// be fixed by FLINK-18651. As a workaround, we re-create a new view without
/// rowtime attribute for now.
///
/// ```sql
/// DROP VIEW IF EXISTS auction;
/// DROP VIEW IF EXISTS bid;
/// CREATE VIEW auction AS SELECT auction.* FROM ${NEXMARK_TABLE} WHERE event_type = 1;
/// CREATE VIEW bid AS SELECT bid.* FROM ${NEXMARK_TABLE} WHERE event_type = 2;
///
/// CREATE TABLE discard_sink (
///   id  BIGINT,
///   itemName  VARCHAR,
///   description  VARCHAR,
///   initialBid  BIGINT,
///   reserve  BIGINT,
///   dateTime  TIMESTAMP(3),
///   expires  TIMESTAMP(3),
///   seller  BIGINT,
///   category  BIGINT,
///   extra  VARCHAR,
///   auction  BIGINT,
///   bidder  BIGINT,
///   price  BIGINT,
///   bid_dateTime  TIMESTAMP(3),
///   bid_extra  VARCHAR
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra,
///     auction, bidder, price, bid_dateTime, bid_extra
/// FROM (
///    SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,
///      ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
///    FROM auction A, bid B
///    WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
/// )
/// WHERE rownum <= 1;
/// ```

// TODO: Why does the above Flink version of this query simply output all the
// fields from the two tables, including duplicating the auction id from both
// records?
// Rust's Ord trait is currently implemented for [tuples up to a length of 12 elements only](https://doc.rust-lang.org/std/primitive.tuple.html#trait-implementations-1)
// so I've reduced it to something sensible, but that changes the output. (Could
// try a tuple-struct, as [mentioned in a comment to this question](https://stackoverflow.com/questions/59446476/can-ord-be-defined-for-a-tuple-in-rust)).
// Is it even worth us including these extra queries that weren't in the
// original Nexmark bench? (Queries 1-8 are from the paper, with only
// modifications in the flink implementation being the windowing times easier
// for testing, for example, with Query 8 changing the window from 12 hours to
// 10 seconds).
// This query 9, for example, appears to be very similar to query 4, just more
// memory intensive due to all the fields. Perhaps we should select certain
// interesting queries - or just do them all for comparison?
type Q9Stream = Stream<
    Circuit<()>,
    OrdZSet<
        (
            u64,
            String,
            String,
            usize,
            // usize, Pull out reserve to limit tuple to 12 elements.
            u64,
            u64,
            u64,
            // usize, Pull out category to limit tuple to 12 elements.
            String,
            // u64, Pull out the duplication of the auction id to limet tuple to 12 elements.
            u64,
            usize,
            u64,
            String,
        ),
        isize,
    >,
>;

pub fn q9(input: NexmarkStream) -> Q9Stream {
    // Select auctions and index by auction id.
    let auctions_by_id = input.flat_map_index(|event| match event {
        Event::Auction(a) => Some((
            a.id,
            (
                a.item_name.clone(),
                a.description.clone(),
                a.initial_bid,
                a.date_time,
                a.expires,
                a.seller,
                a.extra.clone(),
            ),
        )),
        _ => None,
    });

    // Select bids and index by auction id.
    let bids_by_auction = input.flat_map_index(|event| match event {
        Event::Bid(b) => Some((b.auction, (b.bidder, b.price, b.date_time, b.extra.clone()))),
        _ => None,
    });

    type BidsAuctionsJoin = Stream<
        Circuit<()>,
        OrdZSet<
            (
                (
                    u64,
                    String,
                    String,
                    usize,
                    // usize, Pull out reserve to limit tuple to 12 elements.
                    u64,
                    u64,
                    u64,
                    // usize, Pull out category to limit tuple to 12 elements.
                    String,
                ),
                (u64, usize, u64, String),
            ),
            isize,
        >,
    >;

    // Join to get bids for each auction.
    let bids_for_auctions: BidsAuctionsJoin = auctions_by_id.join::<(), _, _, _>(
        &bids_by_auction,
        |&auction_id,
         (a_item_name, a_description, a_initial_bid, a_date_time, a_expires, a_seller, a_extra),
         (b_bidder, b_price, b_date_time, b_extra)| {
            (
                (
                    auction_id,
                    a_item_name.clone(),
                    a_description.clone(),
                    *a_initial_bid,
                    *a_date_time,
                    *a_expires,
                    *a_seller,
                    a_extra.clone(),
                ),
                (*b_bidder, *b_price, *b_date_time, b_extra.clone()),
            )
        },
    );

    // Filter out the invalid bids while indexing.
    // TODO: update to use incremental version of `join_range` once implemented
    // (#137).
    let bids_for_auctions_indexed = bids_for_auctions.flat_map_index(
        |(
            (
                auction_id,
                a_item_name,
                a_description,
                a_initial_bid,
                a_date_time,
                a_expires,
                a_seller,
                a_extra,
            ),
            (b_bidder, b_price, b_date_time, b_extra),
        )| {
            if b_date_time >= a_date_time && b_date_time <= a_expires {
                Some((
                    (
                        *auction_id,
                        a_item_name.clone(),
                        a_description.clone(),
                        *a_initial_bid,
                        *a_date_time,
                        *a_expires,
                        *a_seller,
                        a_extra.clone(),
                    ),
                    // Note that the price of the bid is first in the tuple here to ensure that the
                    // default lexicographic Ord of tuples does what we want below.
                    (*b_price, *b_bidder, *b_date_time, b_extra.clone()),
                ))
            } else {
                None
            }
        },
    );

    // TODO: We can optimize this given that there are no deletions, as DBSP
    // doesn't need to keep records of the bids for future max calculations.
    type AuctionsWithWinningBids = Stream<
        Circuit<()>,
        OrdIndexedZSet<
            (u64, String, String, usize, u64, u64, u64, String),
            (usize, u64, u64, String),
            isize,
        >,
    >;
    let auctions_with_winning_bids: AuctionsWithWinningBids =
        bids_for_auctions_indexed.aggregate::<(), _>(Max);

    // Finally, put the output together as expected and flip the price/bidder
    // into the output order.
    auctions_with_winning_bids.map(
        |(
            (
                auction_id,
                a_item_name,
                a_description,
                a_initial_bid,
                a_date_time,
                a_expires,
                a_seller,
                a_extra,
            ),
            (b_price, b_bidder, b_date_time, b_extra),
        )| {
            (
                *auction_id,
                a_item_name.clone(),
                a_description.clone(),
                *a_initial_bid,
                *a_date_time,
                *a_expires,
                *a_seller,
                a_extra.clone(),
                *b_bidder,
                *b_price,
                *b_date_time,
                b_extra.clone(),
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nexmark::{
            generator::tests::{make_auction, make_bid},
            model::{Auction, Bid, Event},
        },
        zset,
    };

    #[test]
    fn test_q9() {
        let input_vecs = vec![
            // The first batch has a single auction for seller 99 with a highest bid of 100
            // (currently).
            vec![
                (
                    Event::Auction(Auction {
                        id: 1,
                        seller: 99,
                        expires: 10_000,
                        ..make_auction()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 1_000,
                        price: 80,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 2_000,
                        price: 100,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The second batch has a new highest bid for the (currently) only auction.
            // And adds a new auction without any bids (empty join).
            vec![
                (
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 9_000,
                        price: 200,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Auction(Auction {
                        id: 2,
                        seller: 101,
                        expires: 20_000,
                        ..make_auction()
                    }),
                    1,
                ),
            ],
            // The third batch has a new bid but it's not higher, so no effect to the first
            // auction. A bid added for the second auction, so it is added.
            vec![
                (
                    Event::Bid(Bid {
                        auction: 1,
                        date_time: 9_500,
                        price: 150,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        auction: 2,
                        date_time: 19_000,
                        price: 400,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
            // The fourth and final batch has a new bid for auction 2, but it's
            // come in (one millisecond) too late to be valid, so no change.
            vec![(
                Event::Bid(Bid {
                    auction: 2,
                    date_time: 20_001,
                    price: 999,
                    ..make_bid()
                }),
                1,
            )],
        ]
        .into_iter();

        let (circuit, mut input_handle) = Circuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let mut expected_output = vec![
                // First batch has a single auction seller with best bid of 100.
                zset! { (1, "item-name".into(), "description".into(), 5, 0, 10000, 99, "".into(), 1, 100, 2000, "".into()) => 1 },
                // The second batch just updates the best bid for the single auction to 200.
                zset! { (1, "item-name".into(), "description".into(), 5, 0, 10000, 99, "".into(), 1, 100, 2000, "".into()) => -1, (1, "item-name".into(), "description".into(), 5, 0, 10000, 99, "".into(), 1, 200, 9000, "".into()) => 1 },
                // The third batch has a bid for the first auction that isn't
                // higher than the existing bid, so no change there. A (first)
                // bid for the second auction creates a new addition:
                zset! { (2, "item-name".into(), "description".into(), 5, 0, 20_000, 101, "".into(), 1, 400, 19_000, "".into()) => 1 },
                // The last batch just has an invalid (too late) winning bid for
                // auction 2, so no change.
                zset! {},
            ]
            .into_iter();

            let output = q9(stream);
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
