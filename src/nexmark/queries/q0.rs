use super::NexmarkStream;
use crate::operator::FilterMap;
/// Passthrough
///
/// Measures the monitoring overhead including the source generator.
pub fn q0(input: NexmarkStream) -> NexmarkStream {
    input.map(|event| event.clone())
}
