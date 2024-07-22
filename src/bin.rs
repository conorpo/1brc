#![feature(buf_read_has_data_left)]
#![feature(slice_split_once)]
#![feature(inline_const)]
#![feature(duration_millis_float)]

// TODO: Try faster hash algorithim


use std::{
    fs::File, hash::{BuildHasher, Hasher}, io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, mem, path::{Path, PathBuf}, thread::{self, JoinHandle}, time::Duration
};
use hashbrown::{HashMap, hash_map::Entry};
use timer_buddy::*;


const BLOCK_SIZE_GUESS_BYTES: i64 = 100 * 3000000;
const SUBBLOCK_SIZE_BYTES: usize = 2048 * 2048;
const DATA_DIRECTORY: &str = "./1brc.data/";
const CURRENT_FILE: &str = "measurements-1000000000";
const OUTPUT_DIRECTORY: &str = "./my_output/";

struct StationStats {
    temperature_sum: i32,
    entry_count: u32,
    min: i16,
    max: i16,
}


impl Default for StationStats {
    fn default() -> Self {
        Self {
            temperature_sum: 0,
            entry_count: 0,
            min: 0,
            max: 0,
        }
    }
}

type StationMap = HashMap<String, StationStats>;

fn parse_reading(input_slice: &[u8]) -> (i16, usize) {
    let length = input_slice.len();
    let mut reading_val = (input_slice[length-1] - b'0') as i16 + 
                            (input_slice[length-3] - b'0') as i16 * 10;

    let negative_index = match input_slice[length-4].is_ascii_digit() {
        true => {
            reading_val += 100 * (input_slice[length-4] - b'0')  as i16;
            length - 5
        },
        false => {
            length - 4
        }
    };

    match input_slice[negative_index] {
        b'-' => {
            (-reading_val, negative_index - 1)
        },
        _ => {
            (reading_val, negative_index)
        }
    }
}

fn input_block(
    path: PathBuf,
    block_start_offset: u64,
    block_end_offset: u64,
) -> Vec<(String, StationStats)> {
    let input_file = File::open(path).expect("Can't open the file we already opened?");
    let mut buf_reader = BufReader::new(input_file);
    let _ = buf_reader.seek(SeekFrom::Start(block_start_offset));

    let mut buffer = Vec::with_capacity(SUBBLOCK_SIZE_BYTES + 300); //400 extra bytes incase we end in the middle of entry

    let mut cur_subblock_offset = block_start_offset;

    let mut local_hashmap = HashMap::<Vec<u8>, StationStats>::with_capacity(0);
    while cur_subblock_offset < block_end_offset {
        unsafe {
            // SAFETY: All of these willized when file is readl be initia
            buffer.set_len(SUBBLOCK_SIZE_BYTES.min((block_end_offset - cur_subblock_offset) as usize));
        }
        let _ = buf_reader.read_exact(&mut buffer);
        let _ = buf_reader.read_until(b'\n', &mut buffer);
        buffer.pop();

        cur_subblock_offset = buf_reader.stream_position().unwrap();

        for line in buffer.split(|char| char == &b'\n') {
            let (reading_val, split_index) = parse_reading(line);

            let station_name = &line[..split_index];

            let stats = match local_hashmap.get_mut(station_name) {
                Some(stats) => stats,
                None => {
                    let _ = local_hashmap.insert(station_name.to_owned(), StationStats::default());
                    
                    local_hashmap.get_mut(station_name).unwrap()
                }
            };
            
            
            // SAFETY: Entry Count & Temperature Sum will never be over u32::MAX
            stats.entry_count += 1;
            stats.temperature_sum += reading_val as i32;
            
            stats.min = stats.min.min(reading_val);
            stats.max = stats.max.max(reading_val);
        }

        
    }

    
    // All expensive comparisons done, lets own the actual strings now.
    let mut owned_stats = Vec::<(String, StationStats)>::with_capacity(local_hashmap.len());
    for (name_bytes, stats) in local_hashmap.into_iter() {
        let name = String::from_utf8(name_bytes.to_vec()).unwrap();
        owned_stats.push((name, stats));
    }
    owned_stats
}

fn input_multithreaded(mut station_map: StationMap, path: PathBuf) -> StationMap {
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) => {
            dbg!(e);
            panic!("Failed to load file");
        }
    };
    let file_length = file.metadata().unwrap().len();
    let mut main_reader = BufReader::new(file);
    let mut buffer = Vec::with_capacity(120);

    let mut block_start_offset = 0;

    // Dispatch Blocks
    let mut handles = Vec::new();
    while block_start_offset < file_length {
        // Skip over the block and guess a byte
        let  block_end_offset = main_reader
            .seek(SeekFrom::Current(BLOCK_SIZE_GUESS_BYTES))
            .unwrap();

        // Find a good breaking point
        let block_end_offset = if block_end_offset < file_length {
            block_end_offset + main_reader.read_until(b'\n', &mut buffer).unwrap() as u64 - 1
        } else {
            // We overshot, go back to the end
            let _ = main_reader.seek(SeekFrom::End(0));
            file_length - 1
        };

        // Setup Thread
        let path_clone = path.clone();
        let handle =
            thread::spawn(move || input_block(path_clone, block_start_offset, block_end_offset));

        handles.push(handle);

        block_start_offset = block_end_offset + 1; //Skip the newline
        buffer.clear();
    }
    // Collect Results
    for handle in handles {
        let thread_hashmap = handle.join().expect("Thread failed somewhere?");

        for (name, stats) in thread_hashmap.into_iter() {
            match station_map.entry(name) {
                Entry::Occupied(entry) => {
                    let existing_stats = entry.into_mut();
                    existing_stats.entry_count += stats.entry_count;
                    existing_stats.temperature_sum += stats.temperature_sum;
                    existing_stats.min = existing_stats.min.min(stats.min);
                    existing_stats.max = existing_stats.max.max(stats.max);
                }
                Entry::Vacant(slot) => {
                    slot.insert(stats);
                }
            }
        }
    }

    station_map
}

fn flatten_and_sort_ref<'a>(station_map: &StationMap) -> Vec<&String> {
    let mut indirection: Vec<&String> = station_map.keys().collect();
    indirection.sort();
    indirection
}

fn output_ref(
    station_map: &StationMap,
    sorted_keys: Vec<&String>,
    mut buf_writer: BufWriter<File>,
) {
    let mut peekable_entries = sorted_keys.into_iter().peekable();

    let _ = buf_writer.write(&[b'{']);
    while let Some(station) = peekable_entries.next() {
        let stats = station_map
            .get(station)
            .expect(&format!("Can't find the stats for station {}", station));

        let min = (stats.min as f64) / 10.0;
        let max = (stats.max as f64) / 10.0;
        let entry_count = stats.entry_count;
        let temperature_sum = (stats.temperature_sum as f32) / (10.0);

        let mean = temperature_sum / (entry_count as f32);

        let _ = buf_writer.write_fmt(format_args!("{station}={min:.1}/{mean:.1}/{max:.1}"));
        if peekable_entries.peek().is_some() {
            let _ = buf_writer.write(&[b',', b' ']);
        }
    }
    let _ = buf_writer.write(&[b'}']);
}

fn main() {
    let station_map = HashMap::<String, StationStats>::new();

    let mut t = TimerBuddy::start();

    // INPUT
    let path_string = DATA_DIRECTORY.to_owned() + CURRENT_FILE + ".txt";
    let path = Path::new(&path_string).to_owned();
    let station_map = input_multithreaded(station_map, path);
    println!("{:05} ms for Inputting", t.lap().as_millis());

    // FLATTENING + SORTING
    let sorted_keys = flatten_and_sort_ref(&station_map);
    println!("{:05} ms for Flattening and Sorting", t.lap().as_millis());

    // OUTPUT
    let path = OUTPUT_DIRECTORY.to_owned() + CURRENT_FILE + ".my_out";
    let path = Path::new(&path);
    let output_file = File::create(path).expect("Failed to create output file (open w/ Write)");
    let writer = BufWriter::new(output_file);
    output_ref(&station_map, sorted_keys, writer);

    println!("{:05} ms for Outputting", t.lap().as_millis());
}
