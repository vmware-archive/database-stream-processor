//! Nexmark Queries in DBSP.
use crate::model::Event;
use dbsp::{Circuit, OrdZSet, Stream};

type NexmarkStream = Stream<Circuit<()>, OrdZSet<Event, isize>>;

mod q1;
