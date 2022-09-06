use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;

/// Query 15: Bidding Statistics Report (Not in original suite)
///
/// How many distinct users join the bidding for different level of price?
/// Illustrates multiple distinct aggregations with filters.
///
/// ```sql
/// CREATE TABLE discard_sink (
///   `day` VARCHAR,
///   total_bids BIGINT,
///   rank1_bids BIGINT,
///   rank2_bids BIGINT,
///   rank3_bids BIGINT,
///   total_bidders BIGINT,
///   rank1_bidders BIGINT,
///   rank2_bidders BIGINT,
///   rank3_bidders BIGINT,
///   total_auctions BIGINT,
///   rank1_auctions BIGINT,
///   rank2_auctions BIGINT,
///   rank3_auctions BIGINT
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///      DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
///      count(*) AS total_bids,
///      count(*) filter (where price < 10000) AS rank1_bids,
///      count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
///      count(*) filter (where price >= 1000000) AS rank3_bids,
///      count(distinct bidder) AS total_bidders,
///      count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
///      count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
///      count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
///      count(distinct auction) AS total_auctions,
///      count(distinct auction) filter (where price < 10000) AS rank1_auctions,
///      count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
///      count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
/// FROM bid
/// GROUP BY DATE_FORMAT(dateTime, 'yyyy-MM-dd');
/// ```

#[derive(Eq, Clone, Debug, Default, PartialEq, PartialOrd, Ord)]
pub struct Q15Output {
    day: String,
    total_bids: usize,
    rank1_bids: usize,
    rank2_bids: usize,
    rank3_bids: usize,
    total_bidders: usize,
    rank1_bidders: usize,
    rank2_bidders: usize,
    rank3_bidders: usize,
    total_auctions: usize,
    rank1_auctions: usize,
    rank2_auctions: usize,
    rank3_auctions: usize,
}

type Q15Stream = Stream<Circuit<()>, OrdZSet<Q15Output, isize>>;

pub fn q15(input: NexmarkStream) -> Q15Stream {
    // Group/index and aggregate by day - keeping only the price, bidder, auction
    input.flat_map_index(|event| match event {
        Event::Bid(b) => {
            let date_time = SystemTime::UNIX_EPOCH + SystemTime::Duration::from_millis(b.date_time);
            let day = date_time.into().format("%Y-%m-%d");
            Some((day, Q15Output::default()))
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nexmark::{generator::tests::make_bid, model::Bid},
        zset,
    };

    #[test]
    fn test_q15_bids() {
        let input_vecs = vec![
            vec![(
                Event::Bid(Bid {
                    auction: 1,
                    ..make_bid()
                }),
                1,
            )],
            vec![
                (
                    Event::Bid(Bid {
                        auction: 2,
                        ..make_bid()
                    }),
                    1,
                ),
                (
                    Event::Bid(Bid {
                        auction: 3,
                        ..make_bid()
                    }),
                    1,
                ),
            ],
        ]
        .into_iter();

        let (circuit, mut input_handle) = Circuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let mut expected_output = vec![
                zset![
                    Q15Output {
                        day: String::from("1970-01-01"),
                        total_bids: 1,
                        ..Q15Output::default()
                    } => 1,
                ],
                zset![
                    Q15Output {
                        day: String::from("1970-01-01"),
                        total_bids: 1,
                        ..Q15Output::default()
                    } => -1,
                    Q15Output {
                        day: String::from("1970-01-01"),
                        total_bids: 3,
                        ..Q15Output::default()
                    } => 1,
                ],
            ]
            .into_iter();

            let output = q15(stream);

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
