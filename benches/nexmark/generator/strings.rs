//! Generates strings which are used for different field in other model objects.
//!
//! API based on the equivalent [Nexmark Flink StringsGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/StringsGenerator.java).

use super::NexmarkGenerator;
use rand::{distributions::Alphanumeric, Rng};

const MIN_STRING_LENGTH: usize = 3;

impl<R: Rng> NexmarkGenerator<R> {
    pub fn next_string(&mut self, max_length: usize) -> String {
        let len = self.rng.gen_range(MIN_STRING_LENGTH..=max_length);
        (&mut self.rng)
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    //TODO
    use super::super::config::tests::make_default_config;
    use super::*;
    use rand::rngs::mock::StepRng;

    #[test]
    fn next_string_length() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: make_default_config(),
        };

        let s = ng.next_string(5);

        assert_eq!(s, "AAA");
    }
}
