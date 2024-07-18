#![feature(const_float_bits_conv)]

//use core::panicking::panic_fmt;
use std::{collections::{hash_map::Entry, HashMap}, fs::File, io::{BufRead, BufReader, BufWriter, Read}, panic::panic_any, path::Path, process::Termination};
use std::sync::atomic::*;

const CURRENT_FILE: &str = "measurements-10"; 


// IDEA: use arena to allocate stats, change hashmap target as we move stations from low-precision to high-precision
// I belive making these less precise will require expensive typecasting every row, so sacrifice memory for now.
// TODO: Try increasing these when results are barely off
struct StationStats {
    temperature_sum: AtomicU64, // Actually a typecast f64
    entry_count: AtomicU64,
    min: AtomicU64, // Actually a typecast f64,
    max: AtomicU64, // Actually a typecast f64 
}

const ZERO_FLOAT_BITS: u64 = (0.0 as f64).to_bits();
const MAX_FLOAT_BITS: u64 = (-100.0 as f64).to_bits();
const MIN_FLOAT_BITS: u64 = (100.0 as f64).to_bits();


impl Default for StationStats {
    fn default() -> Self {
        Self {
            temperature_sum: AtomicU64::new(ZERO_FLOAT_BITS),
            entry_count: AtomicU64::new(0),
            min: AtomicU64::new(MIN_FLOAT_BITS),
            max: AtomicU64::new(MAX_FLOAT_BITS),
        }
    }
}


fn main() {
    dbg!(ZERO_FLOAT_BITS.to_be_bytes());

    let input_file = File::open(Path::new(&(CURRENT_FILE.to_owned() + ".txt")));
    
    let file = match input_file {
        Ok(file) => file,
        Err(e) => {
            dbg!(e);
            panic!("Failed to load file");
        }
    };

    let mut temperature_map = HashMap::<String, StationStats>::new();
    
    let mut reader = BufReader::new(file);
    let mut reading_bytes = Vec::<u8>::new();
    let mut name_bytes = Vec::<u8>::new();
    while let Ok(_) = reader.read_until(b';', &mut name_bytes) {
        reader.read_until(b'\n', &mut reading_bytes).expect(" Failed to read reading");

        let station_name = std::str::from_utf8(name_bytes.as_slice())
            .expect("station was not considered valid utf8");
        let stats = match temperature_map.get(station_name) {
            Some(stats) => stats,
            None => {
                let _ = temperature_map.insert(station_name.to_owned(), StationStats::default());

                temperature_map.get(station_name).unwrap()
            }
        };
        
        let reading = std::str::from_utf8(reading_bytes.as_slice())
            .expect("Temperature reading was not considered valid utf8")
            .parse::<f64>()
            .expect("Failed to parse reading string into an f64");


        // Update Entries
        stats.entry_count.fetch_add(1, Ordering::Relaxed);

        // Update Temp Sum
        let cur_sum = f64::from_bits(stats.temperature_sum.load(Ordering::Relaxed));     
        let new_sum = cur_sum + reading;
        stats.temperature_sum.store(new_sum.to_bits(), Ordering::Relaxed);

        // Update Min or Max
        if reading < f64::from_bits(stats.min.load(Ordering::Relaxed)) {
            stats.min.store(reading.to_bits(), Ordering::Relaxed)
        } else if reading > f64::from_bits(stats.max.load(Ordering::Relaxed)) {
            stats.max.store(reading.to_bits(), Ordering::Relaxed)
        }
        
        name_bytes.clear();
        reading_bytes.clear();
    }



    todo!();
}
