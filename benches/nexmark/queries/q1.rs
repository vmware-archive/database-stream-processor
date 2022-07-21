use super::NexmarkStream;
use crate::model::{Bid, Event};
use dbsp::operator::FilterMap;

/// Currency Conversion
///
/// Convert each bid value from dollars to euros.
pub fn q1(input: NexmarkStream) -> NexmarkStream {
    input.map(|event| match event {
        Event::NewBid(b) => Event::NewBid(Bid {
            price: b.price * 89 / 100,
            ..b.clone()
        }),
        _ => event.clone(),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::generator::wallclock_time;
    use crate::model::{Bid, Event};
    use crate::nexmark_dbsp_source::tests::{generate_expected_zset_tuples, make_test_source};
    use dbsp::{circuit::Root, trace::ord::OrdZSet, trace::Batch};

    #[test]
    fn test_q1() {
        let wallclock_time = wallclock_time().unwrap();
        let source = make_test_source(wallclock_time, 5);
        // Manually update the generated test result with the expected prices.
        let mut expected_zset_tuples = generate_expected_zset_tuples(wallclock_time, 10)
            .into_iter()
            .map(|((event, _), w)| {
                let event = match event {
                    Event::NewBid(b) => Event::NewBid(Bid { price: 89, ..b }),
                    _ => event,
                };
                ((event, ()), w)
            });

        let root = Root::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q1(input);

            output.inspect(move |e| {
                assert_eq!(
                    e,
                    &OrdZSet::from_tuples((), vec![expected_zset_tuples.next().unwrap()])
                )
            });
        })
        .unwrap();

        for _ in 0..5 {
            root.step().unwrap();
        }
    }
}
