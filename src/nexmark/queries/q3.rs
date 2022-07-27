use super::NexmarkStream;
use crate::{nexmark::model::Event, operator::FilterMap, Circuit, OrdZSet, Stream};

/// Local Item Suggestion
///
/// Who is selling in OR, ID or CA in category 10, and for what auction ids?
/// Illustrates an incremental join (using per-key state and timer) and filter.
///
/// See https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/resources/queries/q3.sql

const STATES_OF_INTEREST: &[&str] = &["OR", "ID", "CA"];
const CATEGORY_OF_INTEREST: usize = 10;

pub fn q3(
    input: NexmarkStream,
) -> Stream<Circuit<()>, OrdZSet<(String, String, String, u64), isize>> {
    // TODO: It's unclear to me how I'd using the DBSP join here (which seems
    // more like a zip). In particular, how is state maintained for the people
    // to look up the person when a related auction is found? Looks like it
    // may be related to an indexed zset - but how would it be indexed on the person
    // id?
    //
    // let auctions = input.filter(|event| match event {
    //     Event::Auction(a) => a.category == 10,
    //     _ => false,
    // });

    // For now, just return the people matching the states regardless of
    // the join on auction.seller.
    input.flat_map(|event| match event {
        Event::Person(p) => match STATES_OF_INTEREST.contains(&p.state.as_str()) {
            true => Some((p.name.clone(), p.city.clone(), p.state.clone(), 0)),
            false => None,
        },
        _ => None,
    })
    // let people_indexed = people.index();

    // Look at join_trace_test for an example that uses same input (edges).
    // auctions.join(&people, |_via, not, sure| {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexmark::{
        generator::{
            tests::{make_auction, make_next_event, make_person, CannedEventGenerator},
            NextEvent,
        },
        model::{Auction, Person},
        NexmarkSource,
    };
    use crate::{circuit::Root, trace::ord::OrdZSet, trace::Batch};
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_q3_people() {
        let canned_events: Vec<NextEvent> = vec![
            NextEvent {
                event: Event::Person(Person {
                    id: 1,
                    name: String::from("NL Seller"),
                    state: String::from("NL"),
                    ..make_person()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Person(Person {
                    id: 2,
                    name: String::from("CA Seller"),
                    state: String::from("CA"),
                    ..make_person()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Person(Person {
                    id: 3,
                    name: String::from("ID Seller"),
                    state: String::from("ID"),
                    ..make_person()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Auction(Auction {
                    id: 999,
                    seller: 2,
                    category: CATEGORY_OF_INTEREST,
                    ..make_auction()
                }),
                ..make_next_event()
            },
            NextEvent {
                event: Event::Auction(Auction {
                    id: 452,
                    seller: 3,
                    category: CATEGORY_OF_INTEREST,
                    ..make_auction()
                }),
                ..make_next_event()
            },
        ];

        let source: NexmarkSource<StepRng, isize, OrdZSet<Event, isize>> =
            NexmarkSource::from_generator(CannedEventGenerator::new(canned_events));

        let root = Root::build(move |circuit| {
            let input = circuit.add_source(source);

            let output = q3(input);

            output.inspect(move |e| {
                // This is failing currently because it's just returning the sellers and not
                // joining to get the correct auction ids, until I go back and learn more about
                // DBSP joins.
                assert_eq!(
                    e,
                    &OrdZSet::from_tuples(
                        (),
                        vec![
                            (
                                (
                                    (
                                        String::from("CA Seller"),
                                        String::from("Phoenix"),
                                        String::from("CA"),
                                        999,
                                    ),
                                    ()
                                ),
                                1
                            ),
                            (
                                (
                                    (
                                        String::from("ID Seller"),
                                        String::from("Phoenix"),
                                        String::from("ID"),
                                        452,
                                    ),
                                    ()
                                ),
                                1
                            ),
                        ]
                    )
                )
            });
        })
        .unwrap();

        root.step().unwrap();
    }
}
