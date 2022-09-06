use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read, Result},
};

use csv;

/// Query 13: Bounded Side Input Join (Not in original suite)
///
/// Joins a stream to a bounded side input, modeling basic stream enrichment.
///
/// TODO: use the new "filesystem" connector once FLINK-17397 is done
/// ```
/// CREATE TABLE side_input (
///   key BIGINT,
///   `value` VARCHAR
/// ) WITH (
///   'connector.type' = 'filesystem',
///   'connector.path' = 'file://${FLINK_HOME}/data/side_input.txt',
///   'format.type' = 'csv'
/// );
///
/// CREATE TABLE discard_sink (
///   auction  BIGINT,
///   bidder  BIGINT,
///   price  BIGINT,
///   dateTime  TIMESTAMP(3),
///   `value`  VARCHAR
/// ) WITH (
///   'connector' = 'blackhole'
/// );
///
/// INSERT INTO discard_sink
/// SELECT
///     B.auction,
///     B.bidder,
///     B.price,
///     B.dateTime,
///     S.`value`
/// FROM (SELECT *, PROCTIME() as p_time FROM bid) B
/// JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
/// ON mod(B.auction, 10000) = S.key;
/// ```
///
/// NOTE: although the Flink test uses a static file as the side input, the
/// query itself allows joining the temporal table from the filesystem file that
/// is updated while the query runs, joining the temporal table using process
/// time. Flink itself ensures that the file is monitored for changes. The
/// [current documentation for this connector](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/filesystem/)
/// is not that used above, since the new connector (in 1.17-SNAPSHOT) does not
/// allow specifying a file name, but only a directory to monitor. Rather, the
/// Nexmark test appears to have used the previous legacy filesystem connector, [stable filesystem connector](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/filesystem/)
/// which does allow specifying a file path.
///
/// Also see [Flink's Join with a Temporal Table](https://nightlies.apache.org/flink/flink-docs-release-1.11/dev/table/streaming/joins.html#join-with-a-temporal-table).
///
/// So, although Flink supports monitoring the side-loaded file for updates, a
/// simple static file is used for this bounded side-input for the Nexmark tests
/// and that is also what is tested here.

const SIDE_INPUT_CSV: &str = "benches/nexmark/data/side_input.txt";

type Q13Stream = Stream<Circuit<()>, OrdZSet<(u64, u64, usize, u64, String), isize>>;

fn read_side_input<R: Read>(reader: R) -> Result<HashMap<usize, String>> {
    let reader = BufReader::new(reader);
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(reader);
    let mut hm: HashMap<usize, String> = HashMap::new();
    for result in csv_reader.deserialize() {
        let (key, val): (usize, String) = result?;
        hm.insert(key, val);
    }
    Ok(hm)
}

pub fn q13(input: NexmarkStream) -> Q13Stream {
    let side_input = read_side_input(File::open(SIDE_INPUT_CSV).unwrap()).unwrap();

    input.flat_map(move |event| match event {
        Event::Bid(b) => Some((
            b.auction,
            b.bidder,
            b.price,
            b.date_time,
            side_input[&((b.auction % 10_000) as usize)].clone(),
        )),
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
    fn test_q13() {
        let input_vecs = vec![vec![
            (
                Event::Bid(Bid {
                    auction: 1005,
                    ..make_bid()
                }),
                1,
            ),
            (
                Event::Bid(Bid {
                    auction: 10005,
                    ..make_bid()
                }),
                1,
            ),
        ]]
        .into_iter();

        let (circuit, mut input_handle) = Circuit::build(move |circuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let mut expected_output = vec![zset![
                (1_005, 1, 99, 0, String::from("1005")) => 1,
                (10_005, 1, 99, 0, String::from("5")) => 1,
            ]]
            .into_iter();

            let output = q13(stream);

            output.inspect(move |batch| assert_eq!(batch, &expected_output.next().unwrap()));

            input_handle
        })
        .unwrap();

        for mut vec in input_vecs {
            input_handle.append(&mut vec);
            circuit.step().unwrap();
        }
    }

    #[test]
    fn test_read_side_input() {
        let reader = "1,five\n2,four\n3,three".as_bytes();

        let got = read_side_input(reader).unwrap();

        for (key, val) in HashMap::<usize, String>::from([
            (1, String::from("five")),
            (2, String::from("four")),
            (3, String::from("three")),
        ]) {
            assert_eq!(got[&key], val);
        }
    }
}
