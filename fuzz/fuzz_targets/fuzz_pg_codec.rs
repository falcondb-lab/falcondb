#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Fuzz the PG wire protocol decoder with arbitrary bytes.
    // This exercises both the startup and regular message decoders
    // to ensure no panics, out-of-bounds reads, or infinite loops.

    // 1. Fuzz decode_startup (initial connection phase)
    {
        let mut buf = BytesMut::from(data);
        let _ = falcon_protocol_pg::codec::decode_startup(&mut buf);
    }

    // 2. Fuzz decode_message (post-startup phase)
    {
        let mut buf = BytesMut::from(data);
        // Decode as many messages as possible from the buffer
        loop {
            match falcon_protocol_pg::codec::decode_message(&mut buf) {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }
});
