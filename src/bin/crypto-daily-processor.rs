use regex::Regex;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::io::prelude::*;
use std::str::FromStr;
use std::{
    collections::{HashMap, HashSet},
    env,
    hash::Hash,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

use chrono::prelude::*;
use chrono::DateTime;
use crypto_msg_parser::{extract_symbol, parse_l2, parse_trade, MarketType, MessageType};
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::glob;
use log::*;
use rlimit::{setrlimit, Resource};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use threadpool::ThreadPool;

#[derive(Serialize, Deserialize)]
pub struct Message {
    /// The exchange name, unique for each exchage
    pub exchange: String,
    /// Market type
    pub market_type: MarketType,
    /// Message type
    pub msg_type: MessageType,
    /// Unix timestamp in milliseconds
    pub received_at: u64,
    /// the original message
    pub json: String,
}

fn get_day(unix_timestamp: i64) -> String {
    let naive = NaiveDateTime::from_timestamp(unix_timestamp, 0);
    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
    datetime.format("%Y-%m-%d").to_string()
}

// Output to a raw file and a parsed file.
#[derive(Clone)]
#[allow(clippy::type_complexity)]
struct Output(
    Arc<
        Mutex<(
            Box<dyn std::io::Write + Send>,
            Box<dyn std::io::Write + Send>,
        )>,
    >,
);

/// Split a file by symbol and write to multiple files.
///
/// This function does split, dedup and parse together, and it is
/// thread-safe, each `input_file` will launch a thread.
///
/// ## Arguments:
///
/// - input_file A `.json.gz` file downloaded from AWS S3
/// - day `yyyy-MM-dd` string, all messages beyond [day-5min, day+5min] will be dropped
/// - output_dir_raw Where raw messages will be written to
/// - output_dir_parsed Where parsed messages will be written to
/// - split_files A HashMap that tracks opened files, key is `msg.symbol`, value is file of
///  `output_dir/exchange.market_type.msg_type.symbol.day.json.gz`. Each `exchange, msg_type, market_type`
///  has one `split_files` HashMap
/// - visited A HashSet for deduplication, each `exchange, msg_type, market_type` has one `visited` Hashset
fn split_file<P>(
    input_file: P,
    day: String,
    output_dir_raw: P,
    output_dir_parsed: P,
    split_files: Arc<Mutex<HashMap<String, Output>>>,
    visited: Arc<Mutex<HashSet<u64>>>,
) -> (i64, i64)
where
    P: AsRef<Path>,
{
    let file_name = input_file.as_ref().file_name().unwrap();
    let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
    let exchange = v[0];
    let market_type_str = v[1];
    let market_type = MarketType::from_str(market_type_str).unwrap();
    let msg_type_str = v[2];
    let msg_type = MessageType::from_str(msg_type_str).unwrap();
    let f_in = std::fs::File::open(&input_file)
        .unwrap_or_else(|_| panic!("{:?} does not exist", input_file.as_ref().display()));
    let buf_reader = std::io::BufReader::new(GzDecoder::new(f_in));
    let mut total_lines = 0;
    let mut error_lines = 0;
    let re = Regex::new(r"[():.\\/]+").unwrap();
    for line in buf_reader.lines() {
        if let Ok(line) = line {
            total_lines += 1;
            if let Ok(msg) = serde_json::from_str::<Message>(&line) {
                debug_assert_eq!(msg.exchange, exchange);
                debug_assert_eq!(msg.market_type, market_type);
                debug_assert_eq!(msg.msg_type, msg_type);
                let is_new = {
                    let hashcode = {
                        let mut hasher = DefaultHasher::new();
                        msg.json.hash(&mut hasher);
                        hasher.finish()
                    };
                    let mut visited = visited.lock().unwrap();
                    visited.insert(hashcode)
                };
                if is_new {
                    if let Some(symbol) = extract_symbol(exchange, market_type, &msg.json) {
                        let output = {
                            let mut m = split_files.lock().unwrap();
                            if !m.contains_key(&symbol) {
                                let buf_writer_raw = {
                                    let output_file_name = format!(
                                        "{}.{}.{}.{}.{}.json.gz",
                                        exchange,
                                        market_type_str,
                                        msg_type_str,
                                        re.replace_all(&symbol, "_"),
                                        day
                                    );
                                    let f_out = std::fs::OpenOptions::new()
                                        .create(true)
                                        .write(true)
                                        .truncate(true)
                                        .open(
                                            Path::new(output_dir_raw.as_ref())
                                                .join(output_file_name),
                                        )
                                        .unwrap();
                                    std::io::BufWriter::new(GzEncoder::new(
                                        f_out,
                                        Compression::default(),
                                    ))
                                };
                                let buf_writer_parsed = {
                                    let pair =
                                        crypto_pair::normalize_pair(&symbol, exchange).unwrap();
                                    let pair = re.replace_all(&pair, "_");
                                    let output_file_name = format!(
                                        "{}.{}.{}.{}.{}.{}.json.gz",
                                        exchange,
                                        market_type_str,
                                        msg_type_str,
                                        re.replace_all(&pair, "_"),
                                        re.replace_all(&symbol, "_"),
                                        day
                                    );
                                    let f_out = std::fs::OpenOptions::new()
                                        .create(true)
                                        .write(true)
                                        .truncate(true)
                                        .open(
                                            Path::new(output_dir_parsed.as_ref())
                                                .join(output_file_name),
                                        )
                                        .unwrap();
                                    std::io::BufWriter::new(GzEncoder::new(
                                        f_out,
                                        Compression::default(),
                                    ))
                                };
                                m.insert(
                                    symbol.clone(),
                                    Output(Arc::new(Mutex::new((
                                        Box::new(buf_writer_raw),
                                        Box::new(buf_writer_parsed),
                                    )))),
                                );
                            }
                            m.get(&symbol).unwrap().clone()
                        };
                        let mut writers = output.0.lock().unwrap();
                        // raw
                        if day == get_day((msg.received_at / 1000_u64) as i64) {
                            writeln!(writers.0, "{}", line).unwrap();
                        }
                        match msg.msg_type {
                            MessageType::L2Event => {
                                // Skip unsupported markets
                                if market_type != MarketType::QuantoFuture
                                    && market_type == MarketType::QuantoSwap
                                    && market_type == MarketType::Move
                                    && market_type == MarketType::BVOL
                                {
                                    if let Ok(messages) = parse_l2(
                                        exchange,
                                        msg.market_type,
                                        &msg.json,
                                        Some(msg.received_at as i64),
                                    ) {
                                        for message in messages {
                                            if get_day(message.timestamp / 1000) == day {
                                                writeln!(
                                                    writers.1,
                                                    "{}",
                                                    serde_json::to_string(&message).unwrap()
                                                )
                                                .unwrap();
                                            }
                                        }
                                    }
                                }
                            }
                            MessageType::Trade => {
                                if let Ok(messages) =
                                    parse_trade(&msg.exchange, msg.market_type, &msg.json)
                                {
                                    for message in messages {
                                        if get_day(message.timestamp / 1000) == day {
                                            writeln!(
                                                writers.1,
                                                "{}",
                                                serde_json::to_string(&message).unwrap()
                                            )
                                            .unwrap();
                                        }
                                    }
                                }
                            }
                            _ => panic!("Unknown msg_type {}", msg.msg_type),
                        };
                    } else {
                        warn!("{}", line);
                        error_lines += 1;
                    }
                }
            } else {
                warn!("{}", line);
                error_lines += 1;
            }
        } else {
            error!("malformed file {}", input_file.as_ref().display());
            error_lines += 1;
        }
    }
    (error_lines, total_lines)
}

fn sort_file<P>(input_file: P, output_file: P) -> (i64, i64)
where
    P: AsRef<Path>,
{
    assert!(input_file.as_ref().to_str().unwrap().ends_with(".json.gz"));
    assert!(output_file.as_ref().to_str().unwrap().ends_with(".json.xz"));
    let f_in = std::fs::File::open(&input_file)
        .unwrap_or_else(|_| panic!("{:?} does not exist", input_file.as_ref().display()));
    let buf_reader = std::io::BufReader::new(GzDecoder::new(f_in));
    let mut total_lines = 0;
    let mut error_lines = 0;
    let mut lines: Vec<(i64, String)> = Vec::new();
    for line in buf_reader.lines() {
        if let Ok(line) = line {
            total_lines += 1;
            if let Ok(msg) = serde_json::from_str::<HashMap<String, Value>>(&line) {
                if msg.contains_key("received_at") || msg.contains_key("timestamp") {
                    let timestamp = if msg.contains_key("received_at") {
                        msg.get("received_at").unwrap().as_i64().unwrap()
                    } else {
                        msg.get("timestamp").unwrap().as_i64().unwrap()
                    };
                    lines.push((timestamp, line))
                } else {
                    warn!("{}", line);
                    error_lines += 1;
                }
            } else {
                warn!("{}", line);
                error_lines += 1;
            }
        } else {
            error!("malformed file {}", input_file.as_ref().display());
            error_lines += 1;
        }
    }
    if error_lines == 0 {
        lines.sort_by_key(|x| x.0); // sort by timestamp

        let mut writer = {
            let f_out = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output_file)
                .unwrap();
            let e = xz2::write::XzEncoder::new(f_out, 9);
            std::io::BufWriter::new(e)
        };
        for line in lines {
            writeln!(writer, "{}", line.1).unwrap();
        }
        writer.flush().unwrap();
        std::fs::remove_file(input_file).unwrap();
    }
    (error_lines, total_lines)
}

/// Process files of one day of the same exchange, msg_type, market_type.
///
/// Each `(exchange, msg_type, market_type, day)` will launch a process.
fn process_files_of_day(
    exchange: &str,
    msg_type: MessageType,
    market_type: MarketType,
    day: &str,
    input_dir: &str,
    output_dir_raw: &str,
    output_dir_parsed: &str,
) -> bool {
    let num_threads = num_cpus::get();
    let thread_pool = ThreadPool::new(num_threads);

    // split
    {
        let glob_pattern = format!(
            "{}/**/{}.{}.{}.{}-??-??.json.gz",
            input_dir, exchange, market_type, msg_type, day
        );
        let mut paths: Vec<PathBuf> = glob(&glob_pattern)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        if paths.is_empty() {
            warn!("There are no files of pattern {}", glob_pattern);
            return false;
        }
        {
            // Add addtional files of tomorrow, because there might be some messages belong to today
            let next_day_first_hour = {
                let day_timestamp =
                    DateTime::parse_from_rfc3339(format!("{}T00:00:00Z", day).as_str())
                        .unwrap()
                        .timestamp_millis()
                        / 1000;
                let next_day = NaiveDateTime::from_timestamp(day_timestamp + 24 * 3600, 0);
                let next_day: DateTime<Utc> = DateTime::from_utc(next_day, Utc);
                next_day.format("%Y-%m-%d-%H").to_string()
            };
            let mut paths_of_next_day: Vec<PathBuf> = glob(
                format!(
                    "{}/**/{}.{}.{}.{}-??.json.gz",
                    input_dir, exchange, market_type, msg_type, next_day_first_hour
                )
                .as_str(),
            )
            .unwrap()
            .filter_map(Result::ok)
            .collect();
            paths.append(&mut paths_of_next_day);
        }

        info!(
            "Started split {} {} {} {}",
            exchange, market_type, msg_type, day
        );
        #[allow(clippy::type_complexity)]
        let (tx, rx): (Sender<(i64, i64)>, Receiver<(i64, i64)>) = mpsc::channel();
        let start_timstamp = Instant::now();
        // small files get processed first
        paths.sort_by_cached_key(|path| std::fs::metadata(path).unwrap().len());
        let mut visited_map: HashMap<String, Arc<Mutex<HashSet<u64>>>> = HashMap::new();
        let mut split_files_map: HashMap<String, Arc<Mutex<HashMap<String, Output>>>> =
            HashMap::new();
        for path in paths {
            let file_name = path.as_path().file_name().unwrap();
            let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
            assert_eq!(exchange, v[0]);
            let market_type_str = v[1];
            assert_eq!(market_type, MarketType::from_str(market_type_str).unwrap());
            let msg_type_str = v[2];
            assert_eq!(msg_type, MessageType::from_str(msg_type_str).unwrap());
            let output_dir_raw = Path::new(output_dir_raw)
                .join(msg_type_str)
                .join(exchange)
                .join(market_type_str);
            std::fs::create_dir_all(output_dir_raw.as_path()).unwrap();
            let output_dir_parsed = Path::new(output_dir_parsed)
                .join(msg_type_str)
                .join(exchange)
                .join(market_type_str);
            std::fs::create_dir_all(output_dir_parsed.as_path()).unwrap();
            let key = format!("{}.{}.{}", exchange, market_type_str, msg_type_str);
            if !visited_map.contains_key(&key) {
                visited_map.insert(key.clone(), Arc::new(Mutex::new(HashSet::new())));
            }
            if !split_files_map.contains_key(&key) {
                split_files_map.insert(key.clone(), Arc::new(Mutex::new(HashMap::new())));
            }

            let visited_of_market = visited_map.get(&key).unwrap().clone();
            let split_files_of_market = split_files_map.get(&key).unwrap().clone();
            let day_clone = day.to_string();
            let thread_tx = tx.clone();
            thread_pool.execute(move || {
                let t = split_file(
                    path.as_path(),
                    day_clone,
                    output_dir_raw.as_path(),
                    output_dir_parsed.as_path(),
                    split_files_of_market,
                    visited_of_market,
                );
                thread_tx.send(t).unwrap();
            });
        }
        thread_pool.join();
        drop(tx); // drop the sender
        let mut total_lines = 0;
        let mut error_lines = 0;
        for t in rx {
            error_lines += t.0;
            total_lines += t.1;
        }
        for m in split_files_map.values() {
            for output in m.lock().unwrap().values() {
                let mut output = output.0.lock().unwrap();
                output.0.flush().unwrap();
                output.1.flush().unwrap();
            }
        }
        let error_ratio = (error_lines as f64) / (total_lines as f64);
        if error_ratio > 0.01 {
            // error ratio > 1%
            error!(
                "Failed to split {} {} {} {}, because error ratio {}/{}={}% is higher than 1% !",
                exchange,
                market_type,
                msg_type,
                day,
                error_lines,
                total_lines,
                error_ratio * 100.0
            );
            return false;
        } else {
            info!("Finished split {} {} {} {}, dropped {} malformed lines out of {} lines, time elapsed {} seconds", exchange, market_type, msg_type, day, error_lines, total_lines, start_timstamp.elapsed().as_secs());
        }
    }

    // sort by timestamp
    {
        let glob_pattern = format!(
            "{}/**/{}.{}.{}.*.{}.json.gz",
            output_dir_raw, exchange, market_type, msg_type, day
        );
        let paths_raw: Vec<PathBuf> = glob(&glob_pattern)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        if paths_raw.is_empty() {
            warn!("There are no files of pattern {}", glob_pattern);
            return false;
        }

        let glob_pattern = format!(
            "{}/**/{}.{}.{}.*.{}.json.gz",
            output_dir_parsed, exchange, market_type, msg_type, day
        );
        let paths_parsed: Vec<PathBuf> = glob(&glob_pattern)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        if paths_parsed.is_empty() {
            warn!("There are no files of pattern {}", glob_pattern);
            return false;
        }

        info!(
            "Started sort {} {} {} {}",
            exchange, market_type, msg_type, day
        );
        #[allow(clippy::type_complexity)]
        let (tx, rx): (Sender<(i64, i64)>, Receiver<(i64, i64)>) = mpsc::channel();
        let mut paths: Vec<PathBuf> = paths_raw
            .into_iter()
            .chain(paths_parsed.into_iter())
            .collect();
        let start_timstamp = Instant::now();
        // small files get processed first
        paths.sort_by_cached_key(|path| std::fs::metadata(path).unwrap().len());
        for input_file in paths {
            let file_name = input_file.as_path().file_name().unwrap();
            let output_file_name = format!(
                "{}.json.xz",
                file_name
                    .to_str()
                    .unwrap()
                    .strip_suffix(".json.gz")
                    .unwrap()
            );
            let output_file = Path::new(input_file.parent().unwrap()).join(output_file_name);
            let thread_tx = tx.clone();
            thread_pool.execute(move || {
                let t = sort_file(input_file, output_file);
                thread_tx.send(t).unwrap();
            });
        }
        thread_pool.join();
        drop(tx); // drop the sender
        let mut total_lines = 0;
        let mut error_lines = 0;
        for t in rx {
            error_lines += t.0;
            total_lines += t.1;
        }
        if error_lines == 0 {
            info!(
                "Finished sort {} {} {} {}, time elapsed {} seconds",
                exchange,
                market_type,
                msg_type,
                day,
                start_timstamp.elapsed().as_secs()
            );
            true
        } else {
            error!(
                "Failed to sort {} {} {} {}, found {} malformed lines out of {} lines, time elapsed {} seconds",
                exchange,
                market_type,
                msg_type,
                day,
                error_lines, total_lines,
                start_timstamp.elapsed().as_secs()
            );
            false
        }
    }
}

fn main() {
    env_logger::init();
    assert!(setrlimit(Resource::NOFILE, 4096, 4096).is_ok());

    let args: Vec<String> = env::args().collect();
    if args.len() != 8 {
        eprintln!("Usage: crypto-daily-processor <exchange> <msg_type> <market_type> <day> <input_dir> <output_dir_raw> <output_dir_parsed>");
        std::process::exit(1);
    }

    let exchange: &'static str = Box::leak(args[1].clone().into_boxed_str());

    let msg_type = MessageType::from_str(&args[2]);
    if msg_type.is_err() {
        eprintln!("Unknown msg type: {}", &args[2]);
        std::process::exit(1);
    }
    let msg_type = msg_type.unwrap();

    let market_type = MarketType::from_str(&args[3]);
    if market_type.is_err() {
        eprintln!("Unknown market type: {}", &args[3]);
        std::process::exit(1);
    }
    let market_type = market_type.unwrap();

    let day: &'static str = Box::leak(args[4].clone().into_boxed_str());
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    if !re.is_match(day) {
        eprintln!("{} is invalid, day should be yyyy-MM-dd", day);
        std::process::exit(1);
    }

    let input_dir: &'static str = Box::leak(args[5].clone().into_boxed_str());
    if !Path::new(input_dir).is_dir() {
        eprintln!("{} does NOT exist", input_dir);
        std::process::exit(1);
    }
    let output_dir_raw: &'static str = Box::leak(args[6].clone().into_boxed_str());
    let output_dir_parsed: &'static str = Box::leak(args[7].clone().into_boxed_str());
    std::fs::create_dir_all(Path::new(output_dir_raw)).unwrap();
    std::fs::create_dir_all(Path::new(output_dir_parsed)).unwrap();

    if !process_files_of_day(
        exchange,
        msg_type,
        market_type,
        day,
        input_dir,
        output_dir_raw,
        output_dir_parsed,
    ) {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod test {
    use regex::Regex;

    #[test]
    fn test_clean_symbol() {
        let symbol = "a(b)c:d.e/f";
        let re = Regex::new(r"[():.\\/]+").unwrap();
        let cleaned = re.replace_all(symbol, "_");
        assert_eq!("a_b_c_d_e_f", cleaned);
    }
}
