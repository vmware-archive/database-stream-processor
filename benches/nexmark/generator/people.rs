//! Generates people for the Nexmark streaming data.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PersonGenerator.java).

use rand::{seq::SliceRandom, Rng};
use super::strings::next_string;
use crate::model::{DateTime, Id, Person};

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

// TODO(absoludity): add GeneratorConfig rather than hard-coding.
const PERSON_PROPORTION: usize = 3;
const TOTAL_PROPORTION: usize = 10;
const NUM_ACTIVE_PEOPLE: usize = 2;
const FIRST_PERSON_ID: usize = 0;

// Generate and return a random person with next available id.
// TODO(absoludity): Update to take GeneratorConfig.
pub fn next_person<R: Rng + ?Sized>(next_event_id: Id, rng: &mut R, timestamp: u64) -> Person {
    // TODO(absoludity): Figure out the purpose of the extra field - appears to be
    // aiming to adjust the number of bytes for the record to be an average, which will
    // need slightly different handling in Rust.
    // int currentSize =
    //     8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
    // String extra = nextExtra(random, currentSize, config.getAvgPersonByteSize());

    Person {
        id: last_base0_person_id(next_event_id) + FIRST_PERSON_ID,
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
// TODO(absoludity): Update to take GeneratorConfig.
pub fn next_base0_person_id<R: Rng + ?Sized>(event_id: Id, rng: &mut R) -> Id {
    // Choose a random person from any of the 'active' people, plus a few 'leads'.
    // By limiting to 'active' we ensure the density of bids or auctions per person
    // does not decrease over time for long running jobs.
    // TODO(absoludity): Understand why this code appears to shift the active
    // people ids to always be the most recent people, rather than what the
    // comment above claims.

    // By choosing a person id ahead of the last valid person id we will make
    // newPerson and newAuction events appear to have been swapped in time.
    let num_people = last_base0_person_id(event_id) + 1;
    let active_people = std::cmp::min(num_people, NUM_ACTIVE_PEOPLE);
    let n = rng.gen_range(0..active_people);
    num_people - active_people + n
}

/// Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the
/// current person id if due to generate a person.
// TODO(absoludity): Update to take GeneratorConfig.
pub fn last_base0_person_id(event_id: Id) -> Id {
    let epoch = event_id / TOTAL_PROPORTION;
    let mut offset = event_id % TOTAL_PROPORTION;

    if offset >= PERSON_PROPORTION {
        // About to generate an auction or bid.
        // Go back to the last person generated in this epoch.
        offset = PERSON_PROPORTION - 1;
    }
    // About to generate a person.
    epoch * PERSON_PROPORTION + offset
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
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_next_person() {
        let mut rng = StepRng::new(0, 5);

        let p = next_person(5, &mut rng, 1_000_000_000_000);

        assert_eq!(
            p,
            Person {
                id: 2,
                name: "Peter Shultz".into(),
                email_address: "AAA@AAA.com".into(),
                credit_card: "0000 0000 0000 0000".into(),
                city: "Phoenix".into(),
                state: "AZ".into(),
                date_time: DateTime::UNIX_EPOCH + std::time::Duration::from_millis(1_000_000_000_000),
                extra: String::new(),
            }
        );
    }

    #[test]
    fn test_next_base0_person_id() {
        let mut rng = StepRng::new(0, 5);

        // When one more than the last person id is less than the configured
        // active people, the id returned is one of the active people.
        // Note: the mock rng is always returning zero for n.
        assert_eq!(next_base0_person_id(0, &mut rng), 0);

        // When one more than the last person id is equal to the configured
        // active people, the id returned is one of the active people.
        assert_eq!(next_base0_person_id(1, &mut rng), 0);

        // When one more than the last person id is one greater than the
        // configured active people, we return a number from a range one
        // greater than the active people.
        assert_eq!(next_base0_person_id(2, &mut rng), 1);
        assert_eq!(next_base0_person_id(5, &mut rng), 1);

        // When one more than the last person id is four greater than the
        // configured active people, we return a number from a the range four
        // greater than the active people.
        // TODO(absoludity): Understand why this code appears to shift the active
        // people ids to always be the most recent people, rather than what the
        // comment in the code claims.
        assert_eq!(next_base0_person_id(12, &mut rng), 4);
    }

    #[test]
    fn test_last_base0_person_id() {
        assert_eq!(last_base0_person_id(2), 2);

        assert_eq!(last_base0_person_id(5), 2);

        assert_eq!(last_base0_person_id(12), 1 * PERSON_PROPORTION + 2);
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
