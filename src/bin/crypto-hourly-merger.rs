#![allow(clippy::type_complexity)]
use regex::Regex;
use std::io::prelude::*;
use std::{
    collections::hash_map::DefaultHasher,
    collections::HashMap,
    env,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        mpsc::{
            self, {Receiver, Sender},
        },
        Arc, Mutex,
    },
    time::Instant,
};

use chrono::prelude::*;
use chrono::{DateTime, TimeZone, Utc};
use crypto_market_type::MarketType;
use crypto_msg_parser::{extract_symbol, extract_timestamp};
use crypto_msg_type::MessageType;
use dashmap::{DashMap, DashSet};
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::glob;
use log::*;
use rlimit::{setrlimit, Resource};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use threadpool::ThreadPool;
use urlencoding::encode;

/// Copied from crypto-crawler/src/msg.rs
#[derive(Serialize, Deserialize)]
pub struct Message {
    /// The exchange name, unique for each exchage
    pub exchange: String,
    /// Market type
    pub market_type: MarketType,
    /// Message type
    pub msg_type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Unix timestamp in milliseconds
    pub received_at: u64,
    /// the original message
    pub json: String,
}

fn get_hour(timestamp_millis: i64) -> String {
    let dt = Utc.timestamp(timestamp_millis / 1000, 0);
    dt.format("%Y-%m-%d-%H").to_string()
}

// Output to a raw file and a parsed file.
#[derive(Clone)]
struct Output(Arc<Mutex<Box<dyn std::io::Write + Send>>>);

fn get_real_market_type(exchange: &str, market_type: MarketType, symbol: &str) -> MarketType {
    if market_type == MarketType::Unknown {
        crypto_pair::get_market_type(symbol, exchange, None)
    } else if exchange == "deribit" && market_type == MarketType::InverseFuture {
        crypto_pair::get_market_type(symbol, "deribit", None)
    } else {
        market_type
    }
}

// Check if a line is a valid message.
fn validate_line(line: &str) -> bool {
    if let Ok(msg) = serde_json::from_str::<Message>(line) {
        serde_json::from_str::<HashMap<String, Value>>(msg.json.as_str()).is_ok()
            || serde_json::from_str::<Vec<Value>>(msg.json.as_str()).is_ok()
    } else {
        false
    }
}

// exchanges in exempted list will suceed even if error ratio is greater than threshold
fn is_exempted(exchange: &str, market_type: MarketType, msg_type: MessageType) -> bool {
    exchange == "bitget"
        || (exchange == "binance"
            && market_type == MarketType::EuropeanOption
            && msg_type == MessageType::Trade)
}

/// Split a file by symbol and write to multiple files.
///
/// This function does split, dedup and parse together, and it is
/// thread-safe, each `input_file` will launch a thread.
///
/// ## Arguments:
///
/// - input_file A `.json.gz` file downloaded from AWS S3
/// - hour `yyyy-MM-dd-HH` string, all messages beyond [day-5min, day+5min] will be dropped
/// - output_dir Where raw messages will be written to
/// - splitted_files A HashMap that tracks opened files, key is `msg.symbol`, value is file of
///  `output_dir/exchange.market_type.msg_type.symbol.hour.csv.gz`. Each `exchange, msg_type, market_type`
///  has one `splitted_files` HashMap
/// - visited A HashSet for deduplication, each `exchange, msg_type, market_type` has one
///  `visited` Hashset
fn split_file(
    input_file: PathBuf,
    hour: String,
    output_dir: String,
    splitted_files: Arc<DashMap<String, Output>>,
    visited: Arc<DashSet<u64>>,
) -> (i64, i64, i64, i64, i64) {
    let (exchange, market_type, msg_type) = {
        let file_name = input_file.file_name().unwrap();
        let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
        (
            v[0],
            MarketType::from_str(v[1]).unwrap(),
            MessageType::from_str(v[2]).unwrap(),
        )
    };

    let f_in = std::fs::File::open(input_file.as_path())
        .unwrap_or_else(|_| panic!("{:?} does not exist", input_file.display()));
    let buf_reader = std::io::BufReader::new(GzDecoder::new(f_in));

    let mut total_lines = 0;
    let mut unique_lines = 0;
    let mut duplicated_lines = 0;
    let mut error_lines = 0;
    let mut expired_lines = 0;

    for line in buf_reader.lines() {
        if let Ok(line) = line {
            total_lines += 1;
            if !validate_line(&line) {
                warn!("Not a valid Message: {}", line);
                error_lines += 1;
                continue;
            }

            let msg = serde_json::from_str::<Message>(&line).unwrap();
            {
                // optional validation
                assert_eq!(msg.exchange, exchange);
                if exchange != "bitmex" {
                    assert_eq!(msg.market_type, market_type);
                }
                assert_eq!(msg.msg_type, msg_type);
            }

            // deduplicate
            let hashcode = {
                let mut hasher = DefaultHasher::new();
                msg.json.hash(&mut hasher);
                hasher.finish()
            };

            if !visited.insert(hashcode) {
                duplicated_lines += 1;
                continue;
            }

            // timestamp
            let ret = extract_timestamp(exchange, market_type, &msg.json);
            if ret.is_err() {
                warn!("Failed to extract timestamp from {}", msg.json);
                error_lines += 1;
                continue;
            }
            let timestamp = if let Ok(Some(t)) = ret {
                t
            } else {
                msg.received_at as i64
            };
            if hour != get_hour(timestamp) {
                expired_lines += 1;
                continue;
            }

            // symbol
            let symbol = {
                if let Some(s) = msg.symbol {
                    Some(s)
                } else if let Ok(s) = extract_symbol(exchange, market_type, &msg.json) {
                    match s.as_str() {
                        "NONE" => None,
                        "ALL" => Some("ALL".to_string()),
                        _ => Some(s),
                    }
                } else {
                    None
                }
            };
            if symbol.is_none() {
                error!("No symbol found in {}", line);
                error_lines += 1;
                continue;
            }
            let symbol = symbol.unwrap();

            let real_market_type = get_real_market_type(exchange, msg.market_type, &symbol);

            let output_file_name = {
                let real_exchange = match msg.exchange.as_str() {
                    "mxc" => "mexc",
                    "okex" => "okx",
                    _ => msg.exchange.as_str(),
                };
                let hour = get_hour(timestamp);
                format!(
                    "{}.{}.{}.{}.{}.csv.gz",
                    real_exchange,
                    real_market_type,
                    msg_type,
                    encode_symbol(&symbol),
                    hour
                )
            };
            let output = {
                let output_dir = Path::new(output_dir.as_str()).join(real_market_type.to_string());
                // `.entry().or_insert_with()` is atomic, see https://github.com/xacrimon/dashmap/issues/78
                let entry = splitted_files
                    .entry(output_file_name.clone())
                    .or_insert_with(move || {
                        let buf_writer = {
                            std::fs::create_dir_all(output_dir.as_path()).unwrap();
                            let f_out = std::fs::OpenOptions::new()
                                .create(true)
                                .write(true)
                                .truncate(true)
                                .open(Path::new(output_dir.as_path()).join(output_file_name))
                                .unwrap();
                            std::io::BufWriter::new(GzEncoder::new(f_out, Compression::default()))
                        };
                        Output(Arc::new(Mutex::new(Box::new(buf_writer))))
                    });
                entry.value().clone()
            };
            let mut writer = output.0.lock().unwrap();
            writeln!(writer, "{}\t{}\t{}", msg.received_at, timestamp, msg.json).unwrap();
            unique_lines += 1;
        } else {
            error!("malformed file {}", input_file.display());
            error_lines += 1;
            total_lines += 1;
        }
    }
    (
        total_lines,
        unique_lines,
        duplicated_lines,
        error_lines,
        expired_lines,
    )
}

fn sort_file_in_place<P>(input_file: P) -> bool
where
    P: AsRef<Path>,
{
    assert!(input_file.as_ref().to_str().unwrap().ends_with(".csv.gz"));
    if !input_file.as_ref().exists() {
        panic!("{:?} does not exist", input_file.as_ref().display());
    }
    let buf_reader = {
        let f_in = std::fs::File::open(&input_file).unwrap();
        std::io::BufReader::new(GzDecoder::new(f_in))
    };

    let mut lines: Vec<(i64, i64, String)> = Vec::new();
    for line in buf_reader.lines() {
        if let Ok(line) = line {
            let arr = line.split('\t').collect::<Vec<&str>>();
            if arr.len() == 3 {
                let received_at = arr[0].parse::<i64>().unwrap();
                let timestamp = arr[1].parse::<i64>().unwrap();
                lines.push((received_at, timestamp, line));
            } else {
                panic!(
                    "Malformed line {} in {:?}",
                    line,
                    input_file.as_ref().display()
                );
            }
        } else {
            panic!("Corrupted file {}", input_file.as_ref().display());
        }
    }

    // sort by timestamp first, then received_at
    lines.sort_by(|a, b| {
        if a.1 == b.1 {
            a.0.cmp(&b.0)
        } else {
            a.1.cmp(&b.1)
        }
    });

    // write back
    let mut writer = {
        let f_out = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(input_file)
            .unwrap();
        let encoder = GzEncoder::new(f_out, Compression::fast());
        std::io::BufWriter::new(encoder)
    };

    for line in lines {
        writeln!(writer, "{}", line.2).unwrap();
    }
    writer.flush().unwrap();

    true
}

/// Search files of the given hour in the given directory.
fn search_files(hour: &str, input_dir: &str) -> Vec<PathBuf> {
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}-\d{2}$").unwrap();
    if !re.is_match(hour) {
        eprintln!("{} is invalid, hour should be yyyy-MM-dd-HH", hour);
        std::process::exit(1);
    }

    let glob_pattern = format!("{}/*.{}-??.json.gz", input_dir, hour);
    let mut paths: Vec<PathBuf> = glob(&glob_pattern)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    {
        // Add addtional files of tomorrow, because there might be some messages belong to today
        let next_hour = {
            let day = &hour[..10];
            let hour = &hour[11..];
            let hour_timestamp =
                DateTime::parse_from_rfc3339(format!("{}T{}:00:00Z", day, hour).as_str())
                    .unwrap()
                    .timestamp_millis()
                    / 1000;
            let next_hour = NaiveDateTime::from_timestamp(hour_timestamp + 3600, 0);
            let utc_time: DateTime<Utc> = DateTime::from_utc(next_hour, Utc);
            utc_time.format("%Y-%m-%d-%H").to_string()
        };
        let glob_pattern = format!("{}/*.{}-??.json.gz", input_dir, next_hour);
        for path in glob(&glob_pattern).unwrap().filter_map(Result::ok).take(2) {
            paths.push(path);
        }
    }
    paths
}

/// Search files in multiple directories.
fn search_files_multi(hour: &str, input_dirs: &[&str]) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = Vec::new();
    for input_dir in input_dirs {
        let mut new_paths = search_files(hour, input_dir);
        paths.append(&mut new_paths);
    }

    // Sort by file name, so that earlier files get processed first
    paths.sort();
    paths
}

/// Process files of the given hour with the same exchange, msg_type, market_type.
///
/// Each `(exchange, msg_type, market_type, hour)` will launch a process.
fn process_files_of_hour(hour: &str, input_dirs: &[&str], output_dir: &str) -> bool {
    let num_threads = num_cpus::get();
    let thread_pool = ThreadPool::new(num_threads);

    // split
    let (exchange, market_type, msg_type) = {
        let paths = search_files_multi(hour, input_dirs);
        if paths.is_empty() {
            warn!("There are no files belonged to {}", hour);
            return false;
        }

        let (exchange, market_type, msg_type) = {
            let file_name = paths[0].as_path().file_name().unwrap();
            let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
            (
                v[0].to_string(),
                MarketType::from_str(v[1]).unwrap(),
                MessageType::from_str(v[2]).unwrap(),
            )
        };
        info!(
            "Started to split {} {} {} {}",
            exchange, market_type, msg_type, hour
        );

        let (tx, rx): (
            Sender<(i64, i64, i64, i64, i64)>,
            Receiver<(i64, i64, i64, i64, i64)>,
        ) = mpsc::channel();
        let start_timstamp = Instant::now();

        let visited: Arc<DashSet<u64>> = Arc::new(DashSet::new());
        let splitted_files: Arc<DashMap<String, Output>> = Arc::new(DashMap::new());

        for input_file in paths {
            let (exchange_, market_type_, msg_type_) = {
                let file_name = input_file.as_path().file_name().unwrap();
                let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
                (
                    v[0],
                    MarketType::from_str(v[1]).unwrap(),
                    MessageType::from_str(v[2]).unwrap(),
                )
            };
            assert_eq!(exchange, exchange_);
            assert_eq!(market_type, market_type_);
            assert_eq!(msg_type, msg_type_);

            let hour_clone = hour.to_string();
            let output_dir_clone = output_dir.to_string();
            let splitted_files_clone = splitted_files.clone();
            let visited_clone = visited.clone();
            let tx_clone = tx.clone();
            thread_pool.execute(move || {
                let t = split_file(
                    input_file,
                    hour_clone,
                    output_dir_clone,
                    splitted_files_clone,
                    visited_clone,
                );
                tx_clone.send(t).unwrap();
            });
        }
        thread_pool.join();
        drop(tx); // drop the sender to unblock receiver

        let success = {
            let mut total_lines = 0;
            let mut unique_lines = 0;
            let mut duplicated_lines = 0;
            let mut error_lines = 0;
            let mut expired_lines = 0;
            for t in rx {
                total_lines += t.0;
                unique_lines += t.1;
                duplicated_lines += t.2;
                error_lines += t.3;
                expired_lines += t.4;
            }
            assert_eq!(
                total_lines,
                unique_lines + duplicated_lines + error_lines + expired_lines
            );

            for entry in splitted_files.iter() {
                let output = entry.value();
                output.0.lock().unwrap().flush().unwrap();
            }
            let error_ratio = (error_lines as f64) / (total_lines as f64);
            if error_ratio > 0.01 && !is_exempted(exchange.as_str(), market_type, msg_type) {
                // error ratio > 1%
                error!("Failed to split {} {} {} {}, error ratio {}%, total_lines {} unique_lines {} duplicated_lines {} error_lines {} expired_lines {}, time elapsed {} seconds", exchange, market_type, msg_type, hour, error_ratio * 100.0, total_lines, unique_lines , duplicated_lines , error_lines , expired_lines, start_timstamp.elapsed().as_secs());
                false
            } else {
                info!("Succeeded to split {} {} {} {}, error ratio {}%, total_lines {} unique_lines {} duplicated_lines {} error_lines {} expired_lines {}, time elapsed {} seconds", exchange, market_type, msg_type, hour, error_ratio * 100.0, total_lines, unique_lines , duplicated_lines , error_lines , expired_lines, start_timstamp.elapsed().as_secs());
                true
            }
        };
        if !success {
            return false;
        }

        (exchange, market_type, msg_type)
    };

    // Sort each hourly file by timestamp
    {
        let glob_pattern = if market_type == MarketType::Unknown {
            // MarketType::Unknown means all markets
            format!("/*/{}.*.{}.*.{}.csv.gz", exchange, msg_type, hour)
        } else if exchange == "deribit"
            && market_type == MarketType::InverseFuture
            && msg_type == MessageType::Trade
        {
            // inverse_future + inverse_swap
            format!("/inverse_*/{}.*.{}.*.{}.csv.gz", exchange, msg_type, hour)
        } else {
            format!(
                "/{}/{}.{}.{}.*.{}.csv.gz",
                market_type, exchange, market_type, msg_type, hour
            )
        };

        let paths: Vec<PathBuf> = glob(format!("{}{}", output_dir, glob_pattern).as_str())
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        if paths.is_empty() {
            warn!("There are no files to sort, pattern: {}", glob_pattern);
            return true;
        }

        for path in paths.iter() {
            assert!(!path.as_path().to_str().unwrap().contains(".unknown."));
        }
        let total_files = paths.len();

        info!(
            "Started to sort hourly files of {} {} {} {}",
            exchange, market_type, msg_type, hour
        );
        let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
        let start_timstamp = Instant::now();

        for hourly_file in paths {
            let tx_clone = tx.clone();
            thread_pool.execute(move || {
                let success = sort_file_in_place(hourly_file);
                tx_clone.send(success).unwrap();
            });
        }
        thread_pool.join();
        drop(tx); // drop the sender

        let mut all_success = true;
        for success in rx {
            if !success {
                all_success = false;
                break;
            }
        }
        if all_success {
            info!(
                "Succeeded to sort {} hourly files of {} {} {} {}, time elapsed {} seconds",
                total_files,
                exchange,
                market_type,
                msg_type,
                hour,
                start_timstamp.elapsed().as_secs()
            );
        } else {
            error!(
                "Failed to sort {} hourly files of {} {} {} {}, time elapsed {} seconds",
                total_files,
                exchange,
                market_type,
                msg_type,
                hour,
                start_timstamp.elapsed().as_secs()
            );
        }

        all_success
    }
}

/** The CLI tool merges, deduplicates and sorts messages of the given hour with the same exchange, msg_type and market_type.

For example, for the hour of 2022-05-31-01,

machine1 has the following files:

machine1/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-00.json.gz
machine1/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-15.json.gz
machine1/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-30.json.gz
machine1/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-45.json.gz


machine2 has the following files:

machine2/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-00.json.gz
machine2/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-15.json.gz
machine2/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-30.json.gz
machine2/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-01-45.json.gz


Since some messages of 2022-05-31-01 might arrived at the next hour, so we need to read two extra files:

machine1/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-02-00.json.gz
machine1/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-02-15.json.gz

machine2/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-02-00.json.gz
machine2/l2_event/binance/linear_swap/binance.linear_swap.l2_event.2022-05-31-02-15.json.gz

This tools reads the 12 files above, and outputs the following files:

binance.linear_swap.l2_event.XXXYYY.2022-05-31-01.csv.gz

`XXXYYY` is the symbol.
*/
fn main() {
    env_logger::init();
    assert!(setrlimit(Resource::NOFILE, 131072, 131072).is_ok());

    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Merge, deduplicate and sort messages of the given hour with the same exchange, msg_type and market_type.\n");
        eprintln!(
            "Usage: crypto-hourly-merger <yyyy-MM-dd-HH> <input_dirs(comma separated)> <output_dir>"
        );
        std::process::exit(1);
    }

    let hour: &'static str = Box::leak(args[1].clone().into_boxed_str());
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}-\d{2}$").unwrap();
    if !re.is_match(hour) {
        eprintln!("{} is invalid, hour should be yyyy-MM-dd-HH", hour);
        std::process::exit(1);
    }

    let input_dirs = Box::leak(args[2].clone().into_boxed_str())
        .split(',')
        .collect::<Vec<&str>>();

    let output_dir: &'static str = Box::leak(args[3].clone().into_boxed_str());
    std::fs::create_dir_all(Path::new(output_dir)).unwrap();

    if !process_files_of_hour(hour, &input_dirs, output_dir) {
        std::process::exit(1);
    }
}

fn encode_symbol(symbol: &str) -> String {
    let new_symbol = encode(symbol).to_string(); // equivalent to urllib.parse.quote_plus()
    new_symbol.replace('.', "%2E") // escape the dot '.'
}

#[cfg(test)]
mod test {
    #[test]
    fn test_clean_symbol() {
        let symbol = "a(b)c:d.-_e/f";
        let encoded_symbol = super::encode_symbol(symbol);
        assert_eq!("a%28b%29c%3Ad%2E-_e%2Ff", encoded_symbol);
    }
}
