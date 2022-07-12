//! Configuration options for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink Configuration API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/NexmarkConfiguration.java)
//! and the specific [Nexmark Flink Generator config](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java).
use clap::Parser;

// Number of yet-to-be-created people and auction ids allowed.
pub const PERSON_ID_LEAD: usize = 10;

/// A Nexmark streaming data source generator
///
/// Based on the Java/Flink generator found in the [Nexmark repository](https://github.com/nexmark/nexmark).
#[derive(Parser, Debug)]
#[clap(author, version, about)]
pub struct Config {
    #[clap(
        long = "auction-proportion",
        default_value = "3",
        env = "NEXMARK_AUCTION_PROPORTION",
        help = "Specify the proportion of events that will be new auctions"
    )]
    pub auction_proportion: usize,

    #[clap(
        long = "bid-proportion",
        default_value = "46",
        env = "NEXMARK_BID_PROPORTION",
        help = "Specify the proportion of events that will be new bids"
    )]
    pub bid_proportion: usize,

    #[clap(
        long = "person-proportion",
        default_value = "1",
        env = "NEXMARK_PERSON_PROPORTION",
        help = "Specify the proportion of events that will be new people"
    )]
    pub person_proportion: usize,

    #[clap(
        long = "out-of-order-group-size",
        default_value = "1",
        env = "NEXMARK_OUT_OF_ORDER_GROUP_SIZE",
        help = "Number of events in out-of-order groups. 1 implies no out-of-order events. 1000 implies every 1000 events per generator are emitted in pseudo-random order."
    )]
    pub out_of_order_group_size: usize,

    #[clap(
        long = "num-in-flight-auctions",
        default_value = "100",
        env = "NEXMARK_NUM_IN_FLIGHT_AUCTIONS",
        help = "Average number of auctions which should be inflight at any time, per generator."
    )]
    pub num_in_flight_auctions: usize,

    #[clap(
        long = "num-active-people",
        default_value = "1000",
        env = "NEXMARK_NUM_ACTIVE_PEOPLE",
        help = "Maximum number of people to consider as active for placing auctions or bids."
    )]
    pub num_active_people: usize,

    #[clap(
        long = "first-event-rate",
        default_value = "10000",
        env = "NEXMARK_FIRST_EVENT_RATE",
        help = "Initial overall event rate (per second)."
    )]
    pub first_event_rate: usize,

    #[clap(
        long = "num-event-generators",
        default_value = "1",
        env = "NEXMARK_NUM_EVENT_GENERATORS",
        help = "Number of event generators to use. Each generates events in its own timeline."
    )]
    pub num_event_generators: usize,
}

/// Implementation of config methods based on the Java implementation at
/// [NexmarkConfig.java](https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/NexmarkConfiguration.java).
impl Config {
    pub fn total_proportion(&self) -> usize {
        self.person_proportion + self.auction_proportion + self.bid_proportion
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn make_default_nexmark_config() -> Config {
        Config {
            auction_proportion: 3,
            bid_proportion: 46,
            person_proportion: 1,
            num_in_flight_auctions: 100,
            num_active_people: 1000,
            out_of_order_group_size: 1,
            first_event_rate: 10_000,
            num_event_generators: 1,
        }
    }

    #[test]
    fn test_total_proportion_default() {
        assert_eq!(make_default_nexmark_config().total_proportion(), 50);
    }
}
