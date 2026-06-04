#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    superseedr::fuzzing::reduce_dht_lifecycle(data);
});
