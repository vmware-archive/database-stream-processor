// Copyright 2022 the DBSP contributors.
// SPDX-License-Identifier: MIT

//! Generates strings which are used for different field in other model objects.
//!
//! API based on the equivalent [Nexmark Flink StringsGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/StringsGenerator.java).

use rand::{distributions::Alphanumeric, Rng};

const MIN_STRING_LENGTH: usize = 3;

/// Returns a string of random alphanumeric characters.
pub fn next_string<R: Rng + ?Sized>(rng: &mut R, max_length: usize) -> String {
    let len = rng.gen_range(MIN_STRING_LENGTH..=max_length);
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::mock::StepRng;

    #[test]
    fn next_string_length() {
        let mut rng = StepRng::new(0, 5);

        let s = next_string(&mut rng, 5);

        assert_eq!(s, "AAA");
    }
}
