My implementation of the 1brc challenge in Rust.

Major improvements came from
- Multithreading into blocks
- Reading each block in smaller subblocks
- Hashing the station name bytes, and parsing the reading from the raw bytes, only parsing the utf-8 when the block is done.
- Hashbrown over std HashMap
- Parsing the the reading first, then using that function to return the split index, as oppose to splitting then parsing each.
- Storing max,min,avg as u16's instead of f32.
