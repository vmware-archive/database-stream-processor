//! Generates people for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PersonGenerator.java).

use super::strings::next_string;
use crate::config;
use crate::model::{DateTime, Id, Person};
use rand::{seq::SliceRandom, Rng};

// Keep the number of states small so that the example queries will find
// results even with a small batch of events.
const US_STATES: &[&str] = &["AZ", "CA", "ID", "OR", "WA", "WY"];

const US_CITIES: &[&str] = &[
    "Phoenix",
    "Los Angeles",
    "San Francisco",
    "Boise",
    "Portland",
    "Bend",
    "Redmond",
    "Seattle",
    "Kent",
    "Cheyenne",
];

const FIRST_NAMES: &[&str] = &[
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
];

const LAST_NAMES: &[&str] = &[
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
];

// Generate and return a random person with next available id.
pub fn next_person<R: Rng + ?Sized>(
    conf: &config::Config,
    next_event_id: Id,
    rng: &mut R,
    timestamp: u64,
) -> Person {
    // TODO(absoludity): Figure out the purpose of the extra field - appears to be
    // aiming to adjust the number of bytes for the record to be an average, which will
    // need slightly different handling in Rust.
    // int currentSize =
    //     8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
    // String extra = nextExtra(random, currentSize, config.getAvgPersonByteSize());

    Person {
        id: last_base0_person_id(conf, next_event_id) + config::FIRST_PERSON_ID,
        name: next_person_name(rng),
        email_address: next_email(rng),
        credit_card: next_credit_card(rng),
        city: next_us_city(rng),
        state: next_us_state(rng),
        date_time: DateTime::UNIX_EPOCH + std::time::Duration::from_millis(timestamp),
        extra: String::new(),
    }
}

/// Return a random person id (base 0).
///
/// Choose a random person from any of the 'active' people, plus a few 'leads'.
/// By limiting to 'active' we ensure the density of bids or auctions per person
/// does not decrease over time for long running jobs.  By choosing a person id
/// ahead of the last valid person id we will make newPerson and newAuction
/// events appear to have been swapped in time.
///
/// NOTE: The above is the original comment from the Java implementation. The
/// "base 0" is referring to the fact that the returned Id is not including the
/// FIRST_PERSON_ID offset, and should really be "offset 0".

pub fn next_base0_person_id<R: Rng + ?Sized>(
    conf: &config::Config,
    event_id: Id,
    rng: &mut R,
) -> Id {
    let num_people = last_base0_person_id(conf, event_id) + 1;
    let active_people = std::cmp::min(num_people, config::NUM_ACTIVE_PEOPLE);
    let n = rng.gen_range(0..(active_people + config::PERSON_ID_LEAD));
    num_people - active_people + n
}

/// Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the
/// current person id if due to generate a person.
pub fn last_base0_person_id(conf: &config::Config, event_id: Id) -> Id {
    let epoch = event_id / conf.total_proportion();
    let mut offset = event_id % conf.total_proportion();

    if offset >= conf.person_proportion {
        // About to generate an auction or bid.
        // Go back to the last person generated in this epoch.
        offset = conf.person_proportion - 1;
    }
    // About to generate a person.
    epoch * conf.person_proportion + offset
}

// Return a random US state.
fn next_us_state<R: Rng + ?Sized>(rng: &mut R) -> String {
    US_STATES.choose(rng).unwrap().to_string()
}

// Return a random US city.
fn next_us_city<R: Rng + ?Sized>(rng: &mut R) -> String {
    US_CITIES.choose(rng).unwrap().to_string()
}

// Return a random person name.
fn next_person_name<R: Rng + ?Sized>(rng: &mut R) -> String {
    format!(
        "{} {}",
        FIRST_NAMES.choose(rng).unwrap(),
        LAST_NAMES.choose(rng).unwrap()
    )
}

// Return a random email address.
fn next_email<R: Rng + ?Sized>(rng: &mut R) -> String {
    format!("{}@{}.com", next_string(rng, 7), next_string(rng, 5))
}

// Return a random credit card number.
fn next_credit_card<R: Rng + ?Sized>(rng: &mut R) -> String {
    format!(
        "{:04} {:04} {:04} {:04}",
        rng.gen_range(0..10_000),
        rng.gen_range(0..10_000),
        rng.gen_range(0..10_000),
        rng.gen_range(0..10_000)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use clap::Parser;
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_next_person() {
        let conf = Config::parse();

        let mut rng = StepRng::new(0, 5);

        let p = next_person(&conf, 105, &mut rng, 1_000_000_000_000);

        assert_eq!(
            p,
            Person {
                id: 1002,
                name: "Peter Shultz".into(),
                email_address: "AAA@AAA.com".into(),
                credit_card: "0000 0000 0000 0000".into(),
                city: "Phoenix".into(),
                state: "AZ".into(),
                date_time: DateTime::UNIX_EPOCH
                    + std::time::Duration::from_millis(1_000_000_000_000),
                extra: String::new(),
            }
        );
    }

    #[test]
    fn test_next_base0_person_id() {
        let conf = Config::parse();
        let mut rng = StepRng::new(0, 5);

        // When one more than the last person id is less than the configured
        // active people (1000), the id returned is a random id from one of
        // the currently active people plus the 'lead' people.
        // Note: the mock rng is always returning zero for the random addition
        // in the range (0..active_people).
        assert_eq!(next_base0_person_id(&conf, 50 * 998, &mut rng), 0);

        // Even when one more than the last person id is equal to the configured
        // active people, the id returned is a random id from one of the
        // active people plus the 'lead' people.
        assert_eq!(next_base0_person_id(&conf, 50 * 999, &mut rng), 0);

        // When one more than the last person id is one greater than the
        // configured active people, we consider the most recent
        // NUM_ACTIVE_PEOPLE to be the active ones, and return a random id from
        // those plus the 'lead'people.
        assert_eq!(next_base0_person_id(&conf, 50 * 1000, &mut rng), 1);

        // When one more than the last person id is 501 greater than the
        // configured active people, we consider the most recent
        // NUM_ACTIVE_PEOPLE to be the active ones, and return a random id from
        // those plus the 'lead' people.
        assert_eq!(next_base0_person_id(&conf, 50 * 1500, &mut rng), 501);
    }

    #[test]
    fn test_last_base0_person_id_default() {
        let conf = Config::parse();
        // With the default config, the first 50 events will only include one
        // person
        assert_eq!(last_base0_person_id(&conf, 25), 0);

        // The 50th event will correspond to the next...
        assert_eq!(last_base0_person_id(&conf, 50), 1);
        assert_eq!(last_base0_person_id(&conf, 75), 1);

        // And so on...
        assert_eq!(last_base0_person_id(&conf, 100), 2);
    }

    #[test]
    fn test_last_base0_person_id_custom() {
        // Set the configured bid proportion to 21,
        // which together with the other defaults for person and auction
        // proportion, makes the total 25.
        let mut conf = Config::parse();
        conf.bid_proportion = 21;

        // With the total proportion at 25, there will be a new person
        // at every 25th event.
        assert_eq!(last_base0_person_id(&conf, 25), 1);
        assert_eq!(last_base0_person_id(&conf, 50), 2);
        assert_eq!(last_base0_person_id(&conf, 75), 3);
        assert_eq!(last_base0_person_id(&conf, 100), 4);
    }

    #[test]
    fn test_next_us_state() {
        let mut rng = StepRng::new(0, 5);

        let s = next_us_state(&mut rng);

        assert_eq!(s, "AZ");
    }

    #[test]
    fn test_next_us_city() {
        let mut rng = StepRng::new(0, 5);

        let c = next_us_city(&mut rng);

        assert_eq!(c, "Phoenix");
    }

    #[test]
    fn test_next_person_name() {
        let mut rng = StepRng::new(0, 5);

        let n = next_person_name(&mut rng);

        assert_eq!(n, "Peter Shultz");
    }

    #[test]
    fn test_next_email() {
        let mut rng = StepRng::new(0, 5);

        let e = next_email(&mut rng);

        assert_eq!(e, "AAA@AAA.com");
    }

    #[test]
    fn test_next_credit_card() {
        let mut rng = StepRng::new(0, 5);

        let e = next_credit_card(&mut rng);

        assert_eq!(e, "0000 0000 0000 0000");
    }
}
