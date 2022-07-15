//! Generates bids for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/BidGenerator.java).
use super::strings::next_string;
use super::NexmarkGenerator;
use cached::Cached;
use rand::Rng;

pub const CHANNELS_NUMBER: usize = 10_000;

const BASE_URL_PATH_LENGTH: usize = 5;

impl<R: Rng> NexmarkGenerator<R> {
    fn get_new_channel_instance(&mut self, channel_number: usize) -> (String, String) {
        // Manually check the cache. Note: using a manual SizedCache because the
        // `cached` library doesn't allow using the proc_macro `cached` with
        // `self`.
        self.bid_channel_cache
            .cache_get_or_set_with(channel_number, || {
                let mut url = get_base_url(&mut self.rng);
                // Just following the Java implementation: 1 in 10 chance that
                // the URL is returned as is, otherwise a channel_id query param is
                // added to the URL. Also following the Java implementation
                // which uses `Integer.reverse` to get a deterministic channel_id.
                url = match self.rng.gen_range(0..10) {
                    9 => url,
                    _ => format!("{}&channel_id={}", url, channel_number.reverse_bits()),
                };

                (format!("channel-{}", channel_number), url)
            })
            .clone()
    }
}

fn get_base_url<R: Rng>(rng: &mut R) -> String {
    format!(
        "https://www.nexmark.com/{}/item.htm?query=1",
        next_string(rng, BASE_URL_PATH_LENGTH)
    )
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::generator::tests::make_test_generator;
    use rand::rngs::mock::StepRng;
    use regex::Regex;

    #[test]
    fn test_get_base_url() {
        let mut rng = StepRng::new(0, 1);
        assert_eq!(
            get_base_url(&mut rng),
            String::from("https://www.nexmark.com/AAA/item.htm?query=1")
        );
    }

    #[test]
    fn test_get_new_channel_instance_cached() {
        let mut ng = make_test_generator();

        let channel = ng.get_new_channel_instance(1234);
        let re = Regex::new(
            r"^https://www.nexmark.com/(\w+)/item.htm\?query=1(&channel_id=5413326752099336192)?$",
        )
        .unwrap();

        assert_eq!(channel.0, "channel-1234");

        assert!(
            re.is_match(&channel.1),
            "{} did not match {}",
            channel.1,
            re
        );

        // Ensure the length of the captured base path is correct.
        let caps = re.captures(&channel.1).unwrap();

        // Three captures - first is the complete string, second is the random channel
        // URL path and the third is the optional channel id query param.
        // The random URL path which should be between 3 and 5 characters.
        assert_eq!(caps.len(), 3);
        let url_path = caps.get(1).unwrap().as_str();
        assert!(
            match url_path.len() {
                3..=5 => true,
                _ => false,
            },
            "got: {}, want: 3..=5",
            url_path.len()
        );

        // Finally, since the function is using a memory cache, the same result
        // should be returned on subsequent calls for the same channel number.
        let channel_cached = ng.get_new_channel_instance(1234);
        assert_eq!(
            channel.1, channel_cached.1,
            "got: {}, want: {}",
            channel_cached.1, channel.1
        );
    }
}
