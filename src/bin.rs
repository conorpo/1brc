#![feature(const_float_bits_conv)]
#![feature(buf_read_has_data_left)]

//use core::panicking::panic_fmt;
use std::{collections::{hash_map::{self, Entry}, HashMap, VecDeque}, fs::{read, File}, io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},panic::panic_any, path::{Path, PathBuf}, process::Termination, thread, time::{Duration, Instant}};
use std::sync::atomic::*;

use timer_buddy::*;


// 10_000_000 in about 1s rn
// Goal is 1_000_000_000, with a target of <10s
const DATA_DIRECTORY: &str = "./1brc.data/";
const CURRENT_FILE: &str = "measurements-complex-utf8";
const OUTPUT_DIRECTORY: &str = "./my_output/"; 


// IDEA: use arena to allocate stats, change hashmap target as we move stations from low-precision to high-precision
// I belive making these less precise will require expensive typecasting every row, so sacrifice memory for now.
// TODO: Try increasing these when results are barely off
struct StationStats {
    temperature_sum: AtomicU32, // Actually a typecast f64
    entry_count: AtomicU64,
    min: AtomicU32, // Actually a typecast f64,
    max: AtomicU32, // Actually a typecast f64 
}

const ZERO_FLOAT_BITS: u32 = (0.0_f32).to_bits();
const MAX_FLOAT_BITS: u32 = (-100.0_f32).to_bits();
const MIN_FLOAT_BITS: u32 = (100.0_f32).to_bits();


impl Default for StationStats {
    fn default() -> Self {
        Self {
            temperature_sum: AtomicU32::new(ZERO_FLOAT_BITS),
            entry_count: AtomicU64::new(0),
            min: AtomicU32::new(MIN_FLOAT_BITS),
            max: AtomicU32::new(MAX_FLOAT_BITS),
        }
    }
}

type StationMap = HashMap::<String, StationStats>;

fn input_multitreaded(mut station_map: StationMap, path: PathBuf) -> StationMap {
    let mut handles = Vec::new();

    let main_file = File::open(&path);

    let file = match main_file {
        Ok(file) => file,
        Err(e) => {
            dbg!(e);
            panic!("Failed to load file");
        }
    };

    let reader = BufReader::new(file);

    for _ in 0..20 {
        let path_clone = path.clone();
        let handle = thread::spawn(move || {
            let input_file = File::open(&path_clone);
            
            let file = match input_file {
                Ok(file) => file,
                Err(e) => {
                    dbg!(e);
                    panic!("Failed to load file");
                }
            };

            let reader = BufReader::new(file);
            //let station_map = input(station_map, reader);
        });
        handles.push(handle);
    }
    todo!();
}

fn input(mut station_map: StationMap, mut reader: BufReader<File>) -> StationMap {
    let mut line: String = String::new();

    let mut input = String::new();
    reader.read_to_string(&mut input);

    for line in input.lines() {
        let delimeter_index = line.rfind(';').unwrap();

        let station_name = &line[..delimeter_index];
        let stats = match station_map.get(station_name) {
            Some(stats) => stats,
            None => {
                let _ = station_map.insert(station_name.to_owned(), StationStats::default());
                
                station_map.get(station_name).unwrap()
            }
        };
        
        //dbg!(&line[delimeter_index..]);
        let reading = line[(delimeter_index+1)..] //Everying but the \n
            .parse::<f32>()
            .expect("Failed to parse reading string into an f32");


        // Update Entries
        stats.entry_count.fetch_add(1, Ordering::Relaxed);

        // Update Temp Sum
        let cur_sum = f32::from_bits(stats.temperature_sum.load(Ordering::Relaxed));     
        let new_sum = cur_sum + reading;
        stats.temperature_sum.store(new_sum.to_bits(), Ordering::Relaxed);

        // Update Min or Max
        if reading < f32::from_bits(stats.min.load(Ordering::Relaxed)) {
            stats.min.store(reading.to_bits(), Ordering::Relaxed)
        }
        if reading > f32::from_bits(stats.max.load(Ordering::Relaxed)) {
            stats.max.store(reading.to_bits(), Ordering::Relaxed)
        }
    }

    station_map
} 

fn flatten_and_srt<'a>(station_map: StationMap) -> Vec<(String, StationStats)> {
    todo!()
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
        
        let min = f32::from_bits(stats.min.load(Ordering::Relaxed));
        let max = f32::from_bits(stats.max.load(Ordering::Relaxed));
        let entry_count = stats.entry_count.load(Ordering::Relaxed);
        let temperature_sum = f32::from_bits(stats.temperature_sum.load(Ordering::Relaxed));
        
        let mean = temperature_sum / entry_count as f32;
        
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
    let path = DATA_DIRECTORY.to_owned() + CURRENT_FILE + ".txt";
    let path = Path::new(&path).to_owned();

    let station_map = input_multitreaded(station_map, path);

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
    
    // let station_map = input(station_map, reader);

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
