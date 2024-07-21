#![feature(const_float_bits_conv)]
#![feature(buf_read_has_data_left)]
#![feature(seek_stream_len)]
#![feature(read_buf)]
#![feature(core_io_borrowed_buf)]
#![feature(slice_split_once)]

//use core::panicking::panic_fmt;
use std::{collections::hash_map::Entry, fs::{read, File}, io::{BorrowedBuf, BorrowedCursor, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, panic::panic_any, path::{Path, PathBuf}, process::Termination, thread, time::{Duration, Instant}};
use std::sync::atomic::*;

use std::collections::HashMap;


use timer_buddy::*;

const BLOCK_SIZE_GUESS_BYTES: i64 = 100 * 100000;


// 10_000_000 in about 1s rn
// Goal is 1_000_000_000, with a target of <10s
const DATA_DIRECTORY: &str = "./1brc.data/";
const CURRENT_FILE: &str = "measurements-1000000000";
const OUTPUT_DIRECTORY: &str = "./my_output/"; 


// IDEA: use arena to allocate stats, change hashmap target as we move stations from low-precision to high-precision
// I belive making these less precise will require expensive typecasting every row, so sacrifice memory for now.
// TODO: Try increasing these when results are barely off
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

type StationMap = HashMap::<String, StationStats>;

fn parse_reading(input_slice: &[u8]) -> i16 {
    let mut reading_val = 0_i16;
    let negative = (input_slice[0] == b'-') as usize;
    input_slice[negative..].into_iter().filter(|&c| *c != b'.').for_each(|c| {
        reading_val *= 10;
        reading_val += (*c - b'0') as i16;
    });
    if negative == 1 {
        reading_val *= -1;
    }
    reading_val
}

fn input_block(path: PathBuf, block_start_offset: u64, block_end_offset: u64) -> Vec<(String, StationStats)> {
    let input_file = File::open(path).expect("Can't open the file we already opened?");
    let mut buf_reader = BufReader::new(input_file);
    let _ = buf_reader.seek(SeekFrom::Start(block_start_offset));

    let mut buffer = Vec::with_capacity((block_end_offset - block_start_offset) as usize);
    unsafe {
        // SAFETY: All of these will be initialized when file is read
        buffer.set_len(buffer.capacity());
        let _ = buf_reader.read_exact(buffer.as_mut_slice());
    }

    let mut local_hashmap = HashMap::<&[u8], StationStats>::new();
    //let hashmap = HashMap::<
    for line in buffer.split(|&char| char == b'\n'){

        let (station_name, reading) = line.rsplit_once(|&char| char == b';').expect("Couldn't find ';'");

        let stats = match local_hashmap.get_mut(station_name) {
            Some(stats) => stats,
            None => {
                let _ = local_hashmap.insert(station_name, StationStats::default());
                
                local_hashmap.get_mut(station_name).unwrap()
            }
        };

        let reading_val = parse_reading(reading);

        // SAFETY: Entry Count & Temperature Sum will never be over u32::MAX
        stats.entry_count += 1;
        stats.temperature_sum += reading_val as i32;

        stats.min = stats.min.min(reading_val);
        stats.max = stats.max.max(reading_val);
    }

    // All expensive comparisons done, lets let the hashmap own the names now
    let mut owned_stats = Vec::<(String, StationStats)>::with_capacity(local_hashmap.len());
    for (name_bytes, stats) in local_hashmap.into_iter() {
        let name = String::from_utf8(name_bytes.to_vec()).unwrap();
        owned_stats.push((name, stats));
    }
    owned_stats
}

fn input_multithreaded(mut station_map: StationMap, path: PathBuf) -> StationMap {
    let file = match  File::open(&path) {
        Ok(file) => file,
        Err(e) => {
            dbg!(e);
            panic!("Failed to load file");
        }
    };
    let mut main_reader = BufReader::new(file);
    let mut buffer =  Vec::with_capacity(120);
    
    let mut block_start_offset = 0;
    
    // Dispatch Blocks
    let mut handles = Vec::new();
    while main_reader.has_data_left().expect("Failed to read") {
        // Skip over the block and guess a byte
        main_reader.seek(SeekFrom::Current(BLOCK_SIZE_GUESS_BYTES)).unwrap();
        
        // Find a good breaking point
        if main_reader.has_data_left().expect("wtf") {
            let _ = main_reader.read_until(b'\n', &mut buffer);
        } else {
            // We overshot, go back to the end
            let _ = main_reader.seek(SeekFrom::End(0));
        };
        let block_end_offset = main_reader.stream_position().unwrap() - 1; // Don't want the block reader to see the last newline

        // Setup Thread
        let path_clone = path.clone();
        let handle = thread::spawn(move || input_block(path_clone, block_start_offset, block_end_offset));

        handles.push(handle);

        block_start_offset = block_end_offset + 1; //Skip the newline
        buffer.clear();
    }

    // Collect Results
    for handle in handles {
        let thread_hashmap = handle.join().expect("Thread failed somewhere?");
        //dbg!(&thread_hashmap[0].0);

        for (name, stats) in thread_hashmap.into_iter() {
            match station_map.entry(name) {
                Entry::Occupied(entry) => {
                    let existing_stats = entry.into_mut();
                    existing_stats.entry_count += stats.entry_count;
                    existing_stats.temperature_sum += stats.temperature_sum;
                    existing_stats.min = existing_stats.min.min(stats.min);
                    existing_stats.max = existing_stats.max.max(stats.max);
                },
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

fn output_ref(station_map: &StationMap, sorted_keys: Vec<&String>, mut buf_writer: BufWriter<File>) {
    let mut peekable_entries = sorted_keys.into_iter().peekable();

    let _ = buf_writer.write(&[b'{']);
    while let Some(station) = peekable_entries.next() {
        let stats = station_map.get(station).expect(&format!("Can't find the stats for station {}", station));
        
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
}


fn main() {
    let station_map = HashMap::<String, StationStats>::new();

    let mut t = TimerBuddy::start();

    // INPUT
    let path = DATA_DIRECTORY.to_owned() + CURRENT_FILE + ".txt";
    let path = Path::new(&path).to_owned();

    let station_map = input_multithreaded(station_map, path);

    // let input_file = File::open(path);
    
    // let file = match input_file {
    //     Ok(file) => file,
    //     Err(e) => {
    //         dbg!(e);
    //         panic!("Failed to load file");
    //     }
    // };

    // let input_size = file.metadata().unwrap().len();

    // let reader = BufReader::new(file);
    
    // let station_map = input(station_map, reader, input_size as usize);

    let time_taken = t.lap();
    println!("{:05} ms for Inputting", time_taken.as_millis());
    // let average_bandwidth = (input_size as f64) / time_taken.as_secs_f64();
    // println!("{:05.2}MB/s", average_bandwidth / 1_000_000.0);
        

    // FLATTENING + SORTING
    let sorted_keys = flatten_and_sort_ref(&station_map);
    
    println!("{:05} ms for Flattening and Sorting", t.lap().as_millis());



    // OUTPUT
    let path = OUTPUT_DIRECTORY.to_owned() + CURRENT_FILE + ".my_out";
    let path = Path::new(&path);

    let output_file = File::create(path);

    let file = output_file.expect("Failed to create file (open w/ Write)");

    let writer = BufWriter::new(file);

    output_ref(&station_map, sorted_keys, writer);

    println!("{:05} ms for Outputting", t.lap().as_millis());

}
