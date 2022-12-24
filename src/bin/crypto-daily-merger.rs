#![allow(clippy::type_complexity)]
use regex::Regex;
use std::io::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
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
use rand::Rng;
use rlimit::{getrlimit, setrlimit, Resource};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use threadpool::ThreadPool;
use urlencoding::encode;

const MAX_XZ: usize = 4;

const USE_XZ: bool = true;

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

fn get_day(timestamp_millis: i64) -> String {
    let dt = Utc.timestamp_opt(timestamp_millis / 1000, 0).unwrap();
    dt.format("%Y-%m-%d").to_string()
}

fn get_hour(timestamp_millis: i64) -> String {
    let dt = Utc.timestamp_opt(timestamp_millis / 1000, 0).unwrap();
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

/// Split a file by symbol and write to multiple files.
///
/// This function does split, dedup and parse together, and it is
/// thread-safe, each `input_file` will launch a thread.
///
/// ## Arguments:
///
/// - input_file A `.json.gz` or `.json.xz` file downloaded from AWS S3
/// - day `yyyy-MM-dd` string, all messages beyond [day-5min, day+5min] will be dropped
/// - output_dir Where raw messages will be written to
/// - splitted_files A HashMap that tracks opened files, key is `msg.symbol`, value is file of
///  `output_dir/exchange.market_type.msg_type.symbol.hour.csv.gz`. Each `exchange, msg_type, market_type`
///  has one `splitted_files` HashMap
/// - visited A HashSet for deduplication, each `exchange, msg_type, market_type` has one
///  `visited` Hashset
fn split_file(
    input_file: PathBuf,
    day: String,
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
    let extension = input_file.extension().unwrap();
    let buf_reader: Box<dyn std::io::BufRead> = if extension == "gz" {
        let d = GzDecoder::new(f_in);
        Box::new(std::io::BufReader::new(d))
    } else if extension == "xz" {
        let d = xz2::read::XzDecoder::new_multi_decoder(f_in);
        Box::new(std::io::BufReader::new(d))
    } else {
        Box::new(std::io::BufReader::new(f_in))
    };

    let mut total_lines = 0;
    let mut unique_lines = 0;
    let mut duplicated_lines = 0;
    let mut error_lines = 0;
    let mut expired_lines = 0;

    for line in buf_reader.lines() {
        if let Ok(line) = line {
            let line = line.as_str().trim();
            if line.is_empty() {
                // ignore empty lines
                continue;
            }
            total_lines += 1;
            if !validate_line(line) {
                warn!("Not a valid Message: {}", line);
                error_lines += 1;
                continue;
            }

            let msg = serde_json::from_str::<Message>(line).unwrap();

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
            if day != get_day(timestamp) {
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
                if !(exchange == "zbg"
                    && market_type == MarketType::Spot
                    && msg_type == MessageType::Ticker)
                {
                    // don't increase error_lines for zbg spot, and
                    // drop old zbg spot messages without symbols
                    error!("No symbol found in {}", line);
                    error_lines += 1;
                } else {
                    total_lines -= 1; // to make assert_eq of line 579 pass
                }
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
            writeln!(
                writer,
                "{}\t{}\t{}",
                msg.received_at,
                timestamp,
                msg.json.trim()
            )
            .unwrap();
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

fn sort_file<P>(input_file: P, writer: &mut dyn std::io::Write) -> bool
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

    let mut success = true;
    let mut lines: Vec<(i64, i64, String)> = Vec::new();
    for line in buf_reader.lines() {
        if let Ok(line) = line {
            let arr = line.split('\t').collect::<Vec<&str>>();
            if arr.len() == 3 {
                let received_at = arr[0].parse::<i64>().unwrap();
                let timestamp = arr[1].parse::<i64>().unwrap();
                lines.push((received_at, timestamp, line));
            } else {
                error!(
                    "Malformed line {} in {:?}",
                    line,
                    input_file.as_ref().display()
                );
                success = false;
                break;
            }
        } else {
            error!("Corrupted file {}", input_file.as_ref().display());
            success = false;
            break;
        }
    }

    if !success {
        return false;
    }

    // sort by timestamp first, then received_at
    lines.sort_by(|a, b| {
        if a.1 == b.1 {
            a.0.cmp(&b.0)
        } else {
            a.1.cmp(&b.1)
        }
    });

    for line in lines {
        writeln!(writer, "{}", line.2).unwrap();
    }
    writer.flush().unwrap();
    std::fs::remove_file(input_file.as_ref()).unwrap();

    true
}

// Use xz if use_xz is true, and semaphore allows only two xz processes
fn sort_and_merge_files<P>(
    mut hourly_files: Vec<P>,
    output_file: P,
    use_xz: bool,
    semaphore: Arc<AtomicUsize>,
) -> bool
where
    P: AsRef<Path>,
{
    let output_file = output_file.as_ref();
    assert!(output_file.to_str().unwrap().ends_with(".csv.xz"));

    if hourly_files.len() < 24 {
        warn!(
            "There are only {} files for {}",
            hourly_files.len(),
            output_file.display()
        );
    }
    hourly_files.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));

    let mut writer: Box<dyn std::io::Write> = if !use_xz {
        let f_out = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_file)
            .unwrap();
        let e = xz2::write::XzEncoder::new(f_out, 6);
        Box::new(std::io::BufWriter::new(e))
    } else {
        let tmp_file = {
            let output_dir = output_file.parent().unwrap().to_path_buf();
            let filename = output_file.file_name().unwrap().to_str().unwrap();
            output_dir.join(&filename[..filename.len() - ".xz".len()])
        };
        let f_out = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_file.as_path())
            .unwrap();
        Box::new(std::io::BufWriter::new(f_out))
    };

    let mut all_success = true;
    for input_file in hourly_files {
        let success = sort_file(input_file, writer.as_mut());
        if !success {
            all_success = false;
            break;
        }
    }
    drop(writer);

    if all_success {
        if use_xz {
            {
                // Wait for the semaphore which allows only two xz processes
                let mut rng = rand::thread_rng();
                while semaphore.load(Ordering::SeqCst) == 0 {
                    let millis = rng.gen_range(4000_u64..16000_u64);
                    debug!("Waiting for semaphore");
                    std::thread::sleep(Duration::from_millis(millis));
                }
                semaphore.fetch_sub(1_usize, Ordering::SeqCst);
            }
            let tmp_file = {
                let output_dir = output_file.parent().unwrap().to_path_buf();
                let filename = output_file.file_name().unwrap().to_str().unwrap();
                output_dir.join(&filename[..filename.len() - ".xz".len()])
            };

            // level 9 only uses 5 cores maximum, and level 8 only uses 9 cores, while level 7 can utilize all cores
            // see https://github.com/phoronix-test-suite/test-profiles/issues/76
            match std::process::Command::new("xz")
                .args(["-7", "-f", "-T0", tmp_file.as_path().to_str().unwrap()])
                .output()
            {
                Ok(output) => {
                    if !output.status.success() {
                        panic!("{}", String::from_utf8_lossy(&output.stderr));
                    }
                }
                Err(err) => panic!("{}", err),
            }
            semaphore.fetch_add(1_usize, Ordering::SeqCst);
        }
    } else {
        // cleanup
        if use_xz {
            let tmp_file = {
                let output_dir = output_file.parent().unwrap().to_path_buf();
                let filename = output_file.file_name().unwrap().to_str().unwrap();
                output_dir.join(&filename[..filename.len() - ".xz".len()])
            };
            std::fs::remove_file(tmp_file.as_path()).unwrap();
        } else {
            std::fs::remove_file(output_file).unwrap();
        }
    }

    all_success
}

/// Search files of the given date and directory.
fn search_files(day: &str, input_dir: &str, suffix: &str) -> Vec<PathBuf> {
    let glob_pattern = format!("{}/*.{}-??-??.json.{}", input_dir, day, suffix);
    let mut paths: Vec<PathBuf> = glob(&glob_pattern)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    {
        // Add addtional files of tomorrow, because there might be some messages belong to today
        let next_day_first_hour = {
            let day_timestamp = DateTime::parse_from_rfc3339(format!("{}T00:00:00Z", day).as_str())
                .unwrap()
                .timestamp_millis()
                / 1000;
            let next_day = NaiveDateTime::from_timestamp_opt(day_timestamp + 24 * 3600, 0).unwrap();
            let next_day: DateTime<Utc> = DateTime::from_utc(next_day, Utc);
            next_day.format("%Y-%m-%d-%H").to_string()
        };
        let glob_pattern = format!("{}/*.{}-??.json.{}", input_dir, next_day_first_hour, suffix);
        let mut paths_of_next_day: Vec<PathBuf> = glob(&glob_pattern)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        paths.append(&mut paths_of_next_day);
    }
    paths
}

/// Search files in multiple directories.
fn search_files_multi(day: &str, input_dirs: &[&str]) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = Vec::new();
    for input_dir in input_dirs {
        let mut new_paths = search_files(day, input_dir, "gz");
        paths.append(&mut new_paths);
        new_paths = search_files(day, input_dir, "xz");
        paths.append(&mut new_paths);
    }

    let zero_hour = Regex::new(r"\d{4}-\d{2}-\d{2}-00-\d{2}\.json").unwrap();
    if paths
        .iter()
        .filter(|s| !zero_hour.is_match(s.file_name().unwrap().to_str().unwrap()))
        .count()
        == 0
    {
        Vec::new()
    } else {
        // Sort by file name, so that earlier files get processed first
        paths.sort();
        paths
    }
}

/// Process files of one day of the same exchange, msg_type, market_type.
///
/// Each `(exchange, msg_type, market_type, day)` will launch a process.
fn process_files_of_day(day: &str, input_dirs: &[&str], output_dir: &str) -> bool {
    let num_threads = num_cpus::get();
    let thread_pool = ThreadPool::new(num_threads);

    // split
    let (exchange, market_type, msg_type) = {
        let paths = search_files_multi(day, input_dirs);
        if paths.is_empty() {
            warn!("There are no files belonged to {}", day);
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
            exchange, market_type, msg_type, day
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

            let day_clone = day.to_string();
            let output_dir_clone = output_dir.to_string();
            let splitted_files_clone = splitted_files.clone();
            let visited_clone = visited.clone();
            let tx_clone = tx.clone();
            thread_pool.execute(move || {
                let t = split_file(
                    input_file,
                    day_clone,
                    output_dir_clone,
                    splitted_files_clone,
                    visited_clone,
                );
                tx_clone.send(t).unwrap();
            });
        }
        thread_pool.join();
        drop(tx); // drop the sender to unblock receiver
        if thread_pool.panic_count() > 0 {
            return false;
        }

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
            if total_lines > 1000
                && error_ratio > 0.01
                // dYdX open_interest returns HTML sometimes
                && exchange != "dydx"
                && msg_type != MessageType::OpenInterest
            // && !is_exempted(exchange.as_str(), market_type, msg_type)
            {
                // error ratio > 1%
                error!("Failed to split {} {} {} {}, error ratio {}%, total_lines {} unique_lines {} duplicated_lines {} error_lines {} expired_lines {}, time elapsed {} seconds", exchange, market_type, msg_type, day, error_ratio * 100.0, total_lines, unique_lines , duplicated_lines , error_lines , expired_lines, start_timstamp.elapsed().as_secs());
                false
            } else {
                info!("Succeeded to split {} {} {} {}, error ratio {}%, total_lines {} unique_lines {} duplicated_lines {} error_lines {} expired_lines {}, time elapsed {} seconds", exchange, market_type, msg_type, day, error_ratio * 100.0, total_lines, unique_lines , duplicated_lines , error_lines , expired_lines, start_timstamp.elapsed().as_secs());
                true
            }
        };
        if !success {
            return false;
        }

        (exchange, market_type, msg_type)
    };

    // sort by timestamp
    {
        let glob_pattern = if market_type == MarketType::Unknown {
            // MarketType::Unknown means all markets
            format!("/*/{}.*.{}.*.{}-??.csv.gz", exchange, msg_type, day)
        } else if exchange == "deribit"
            && market_type == MarketType::InverseFuture
            && msg_type == MessageType::Trade
        {
            // inverse_future + inverse_swap
            format!("/inverse_*/{}.*.{}.*.{}-??.csv.gz", exchange, msg_type, day)
        } else {
            format!(
                "/{}/{}.{}.{}.*.{}-??.csv.gz",
                market_type, exchange, market_type, msg_type, day
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

        let total_files = paths.len();

        let groups = {
            // files with the same symbol are in one group
            let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
            for path in paths {
                let file_name = path.as_path().file_name().unwrap().to_str().unwrap();
                let key = &file_name[0..(file_name.len() - "-??.csv.gz".len())];
                if !groups.contains_key(key) {
                    groups.insert(key.to_string(), vec![]);
                }
                groups.get_mut(key).unwrap().push(path);
            }
            let mut groups: Vec<Vec<PathBuf>> = groups.values().cloned().collect();
            // Smaller groups get processed first
            groups.sort_by_cached_key(|group| {
                group
                    .iter()
                    .map(|file| std::fs::metadata(file).unwrap().len())
                    .sum::<u64>()
            });
            groups
        };

        info!(
            "Started to sort and merge hourly files of {} {} {} {}",
            exchange, market_type, msg_type, day
        );
        let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
        let start_timstamp = Instant::now();
        let semaphore = Arc::new(AtomicUsize::new(MAX_XZ));

        for files_of_same_symbol in groups {
            let output_file = {
                let file_name = files_of_same_symbol[0]
                    .as_path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap();
                let output_file_name = format!(
                    "{}.csv.xz",
                    &file_name[0..(file_name.len() - "-??.csv.gz".len())]
                );
                Path::new(files_of_same_symbol[0].parent().unwrap()).join(output_file_name)
            };
            let tx_clone = tx.clone();
            let semaphore_clone = semaphore.clone();

            thread_pool.execute(move || {
                let success = sort_and_merge_files(
                    files_of_same_symbol,
                    output_file,
                    USE_XZ,
                    semaphore_clone,
                );
                tx_clone.send(success).unwrap();
            });
        }
        thread_pool.join();
        drop(tx); // drop the sender
        if thread_pool.panic_count() > 0 {
            return false;
        }

        let mut all_success = true;
        for success in rx {
            if !success {
                all_success = false;
                break;
            }
        }
        if all_success {
            info!(
                "Succeeded to sort and merge {} files of {} {} {} {}, time elapsed {} seconds",
                total_files,
                exchange,
                market_type,
                msg_type,
                day,
                start_timstamp.elapsed().as_secs()
            );
        } else {
            error!(
                "Failed to sort and merge {} files of {} {} {} {}, time elapsed {} seconds",
                total_files,
                exchange,
                market_type,
                msg_type,
                day,
                start_timstamp.elapsed().as_secs()
            );
        }

        all_success
    }
}

// Merge files of a given day with the same exchange, msg_type and market_type
fn main() {
    env_logger::init();
    if let Err(err) = setrlimit(Resource::NOFILE, 131072, 131072) {
        error!("setrlimit() failed, {}", err);
        error!("getrlimit(): {:?}", getrlimit(Resource::NOFILE).unwrap());
        std::process::exit(1);
    }

    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Merge files of a given day with the same exchange, msg_type and market_type.\n");
        eprintln!(
            "Usage: crypto-daily-merger <yyyy-MM-dd> <input_dirs(comma separated)> <output_dir>"
        );
        std::process::exit(1);
    }

    if USE_XZ {
        if !Path::new("/usr/bin/xz").exists() {
            eprintln!("/usr/bin/xz NOT found, please install it!");
            return;
        } else {
            debug!("/usr/bin/xz found");
        }
    }

    let day: &'static str = Box::leak(args[1].clone().into_boxed_str());
    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    if !re.is_match(day) {
        eprintln!("{} is invalid, day should be yyyy-MM-dd", day);
        std::process::exit(1);
    }

    let input_dirs = Box::leak(args[2].clone().into_boxed_str())
        .split(',')
        .collect::<Vec<&str>>();

    let output_dir: &'static str = Box::leak(args[3].clone().into_boxed_str());
    std::fs::create_dir_all(Path::new(output_dir)).unwrap();

    if !process_files_of_day(day, &input_dirs, output_dir) {
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
