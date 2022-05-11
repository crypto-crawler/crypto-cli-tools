#![allow(clippy::type_complexity)]
use regex::Regex;
use std::io::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{
    cmp::Reverse,
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
use crypto_msg_parser::{extract_symbol, parse_l2, parse_trade};
use crypto_msg_type::MessageType;
use dashmap::{DashMap, DashSet};
use flate2::write::GzEncoder;
use flate2::{read::GzDecoder, Compression};
use glob::glob;
use log::*;
use rand::Rng;
use rlimit::{setrlimit, Resource};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use threadpool::ThreadPool;
use urlencoding::encode;

const MAX_PIXZ: usize = 2;
// exchanges in exempted list will suceed even if error ratio is greater than threshold
const EXEMPTED_EXCHANGES: &[&str] = &["bitget"];

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

fn get_day(timestamp_millis: i64) -> String {
    let dt = Utc.timestamp(timestamp_millis / 1000, 0);
    dt.format("%Y-%m-%d").to_string()
}

fn get_hour(timestamp_millis: i64) -> String {
    let dt = Utc.timestamp(timestamp_millis / 1000, 0);
    dt.format("%Y-%m-%d-%H").to_string()
}

// Output to a raw file and a parsed file.
#[derive(Clone)]
struct Output(Arc<Mutex<Box<dyn std::io::Write + Send>>>);

fn get_real_market_type(exchange: &str, market_type: MarketType, symbol: &str) -> MarketType {
    if exchange == "bitmex" && market_type == MarketType::Unknown {
        crypto_pair::get_market_type(symbol, "bitmex", None)
    } else if exchange == "deribit" && symbol.ends_with("-PERPETUAL") {
        MarketType::InverseSwap
    } else {
        market_type
    }
}

fn is_blocked_market(market_type: MarketType) -> bool {
    market_type == MarketType::QuantoFuture
        || market_type == MarketType::QuantoSwap
        || market_type == MarketType::EuropeanOption // TODO: need to figure out how to parse option data
}

/// Split a file by symbol and write to multiple files.
///
/// This function does split, dedup and parse together, and it is
/// thread-safe, each `input_file` will launch a thread.
///
/// ## Arguments:
///
/// - input_file A `.json.gz` file downloaded from AWS S3
/// - day `yyyy-MM-dd` string, all messages beyond [day-5min, day+5min] will be dropped
/// - output_dir Where raw messages will be written to
/// - splitted_files A HashMap that tracks opened files, key is `msg.symbol`, value is file of
///  `output_dir/exchange.market_type.msg_type.symbol.hour.json.gz`. Each `exchange, msg_type, market_type`
///  has one `splitted_files` HashMap
/// - visited A HashSet for deduplication, each `exchange, msg_type, market_type` has one
///  `visited` Hashset
fn split_file_raw<P>(
    input_file: P,
    day: String,
    output_dir: P,
    splitted_files: Arc<DashMap<String, Output>>,
    visited: Arc<DashSet<u64>>,
) -> (i64, i64, i64, i64, i64)
where
    P: AsRef<Path>,
{
    let file_name = input_file.as_ref().file_name().unwrap();
    let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
    let exchange = v[0];
    let market_type = MarketType::from_str(v[1]).unwrap();
    let msg_type_str = v[2];
    let msg_type = MessageType::from_str(msg_type_str).unwrap();
    let f_in = std::fs::File::open(&input_file)
        .unwrap_or_else(|_| panic!("{:?} does not exist", input_file.as_ref().display()));
    let buf_reader = std::io::BufReader::new(GzDecoder::new(f_in));
    let mut total_lines = 0;
    let mut unique_lines = 0;
    let mut duplicated_lines = 0;
    let mut error_lines = 0;
    let mut expired_lines = 0;
    for line in buf_reader.lines() {
        if let Ok(line) = line {
            total_lines += 1;
            if let Ok(mut msg) = serde_json::from_str::<Message>(&line) {
                assert_eq!(msg.exchange, exchange);
                if market_type != MarketType::Unknown {
                    assert_eq!(msg.market_type, market_type);
                }
                assert_eq!(msg.msg_type, msg_type);
                let hashcode = {
                    let mut hasher = DefaultHasher::new();
                    msg.json.hash(&mut hasher);
                    hasher.finish()
                };
                if let Ok(symbol) = extract_symbol(exchange, market_type, &msg.json) {
                    let real_market_type = get_real_market_type(exchange, msg.market_type, &symbol);

                    if day == get_day(msg.received_at as i64) {
                        // raw
                        if visited.insert(hashcode) {
                            unique_lines += 1;
                            let output_file_name = {
                                let hour = get_hour(msg.received_at as i64);
                                format!(
                                    "{}.{}.{}.{}.{}.json.gz",
                                    exchange,
                                    real_market_type,
                                    msg_type_str,
                                    encode_symbol(&symbol),
                                    hour
                                )
                            };
                            let output = {
                                let output_dir_clone = output_dir.as_ref().to_path_buf();
                                // `.entry().or_insert_with()` is atomic, see https://github.com/xacrimon/dashmap/issues/78
                                let entry = splitted_files
                                    .entry(output_file_name.clone())
                                    .or_insert_with(move || {
                                        let buf_writer_raw = {
                                            let output_dir =
                                                output_dir_clone.join(real_market_type.to_string());
                                            std::fs::create_dir_all(output_dir.as_path()).unwrap();
                                            let f_out = std::fs::OpenOptions::new()
                                                .create(true)
                                                .write(true)
                                                .truncate(true)
                                                .open(
                                                    Path::new(output_dir.as_path())
                                                        .join(output_file_name),
                                                )
                                                .unwrap();
                                            std::io::BufWriter::new(GzEncoder::new(
                                                f_out,
                                                Compression::default(),
                                            ))
                                        };
                                        Output(Arc::new(Mutex::new(Box::new(buf_writer_raw))))
                                    });
                                entry.value().clone()
                            };
                            let mut writer = output.0.lock().unwrap();
                            if msg.market_type != real_market_type
                                || msg.exchange == "mxc"
                                || msg.exchange == "okex"
                            {
                                msg.market_type = real_market_type;
                                if msg.exchange == "mxc" {
                                    msg.exchange = "mexc".to_string();
                                } else if msg.exchange == "okex" {
                                    msg.exchange = "okx".to_string();
                                }
                                writeln!(writer, "{}", serde_json::to_string(&msg).unwrap())
                                    .unwrap();
                            } else {
                                writeln!(writer, "{}", line).unwrap();
                            }
                        } else {
                            duplicated_lines += 1;
                        }
                    } else {
                        expired_lines += 1;
                    }
                } else {
                    warn!("No symbol: {}", line);
                    error_lines += 1;
                }
            } else {
                warn!("Not a valid Message: {}", line);
                error_lines += 1;
            }
        } else {
            error!("malformed file {}", input_file.as_ref().display());
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

fn split_file_parsed<P>(
    input_file: P,
    day: String,
    output_dir: P,
    splitted_files: Arc<DashMap<String, Output>>,
    visited: Arc<DashSet<u64>>,
) -> (i64, i64, i64, i64, i64)
where
    P: AsRef<Path>,
{
    let file_name = input_file.as_ref().file_name().unwrap();
    let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
    let exchange = v[0];
    let market_type = MarketType::from_str(v[1]).unwrap();
    let msg_type_str = v[2];
    let msg_type = MessageType::from_str(msg_type_str).unwrap();
    let f_in = std::fs::File::open(&input_file)
        .unwrap_or_else(|_| panic!("{:?} does not exist", input_file.as_ref().display()));
    let buf_reader = std::io::BufReader::new(GzDecoder::new(f_in));
    let mut total_lines = 0;
    let mut unique_lines = 0;
    let mut duplicated_lines = 0;
    let mut error_lines = 0;
    let mut expired_lines = 0;
    for line in buf_reader.lines() {
        if let Ok(line) = line {
            total_lines += 1;
            if let Ok(msg) = serde_json::from_str::<Message>(&line) {
                assert_eq!(msg.exchange, exchange);
                if market_type != MarketType::Unknown {
                    assert_eq!(msg.market_type, market_type);
                }
                assert_eq!(msg.msg_type, msg_type);
                let hashcode = {
                    let mut hasher = DefaultHasher::new();
                    msg.json.hash(&mut hasher);
                    hasher.finish()
                };
                if let Ok(symbol) = extract_symbol(exchange, market_type, &msg.json) {
                    let real_market_type = get_real_market_type(exchange, msg.market_type, &symbol);

                    if visited.insert(hashcode) {
                        unique_lines += 1;
                        // parsed
                        let write_parsed =
                            |market_type: MarketType, json: String, timestamp: i64| {
                                let output_file_name = {
                                    let hour = get_hour(timestamp);
                                    let pair = crypto_pair::normalize_pair(&symbol, exchange)
                                        .expect(
                                            format!("{} {} {}", symbol, exchange, line).as_str(),
                                        );
                                    let (base, quote) = {
                                        let v = pair.as_str().split('/').collect::<Vec<&str>>();
                                        (v[0], v[1])
                                    };
                                    format!(
                                        "{}.{}.{}.{}.{}.{}.{}.json.gz",
                                        exchange,
                                        market_type,
                                        msg_type_str,
                                        encode_symbol(base),
                                        encode_symbol(quote),
                                        encode_symbol(&symbol),
                                        hour
                                    )
                                };
                                let output = {
                                    let output_dir_clone = output_dir.as_ref().to_path_buf();
                                    // `.entry().or_insert_with()` is atomic, see https://github.com/xacrimon/dashmap/issues/78
                                    let entry = splitted_files
                                        .entry(output_file_name.clone())
                                        .or_insert_with(move || {
                                            let buf_writer_parsed = {
                                                let output_dir =
                                                    output_dir_clone.join(market_type.to_string());
                                                std::fs::create_dir_all(output_dir.as_path())
                                                    .unwrap();
                                                let f_out = std::fs::OpenOptions::new()
                                                    .create(true)
                                                    .write(true)
                                                    .truncate(true)
                                                    .open(
                                                        Path::new(output_dir.as_path())
                                                            .join(output_file_name),
                                                    )
                                                    .unwrap();
                                                std::io::BufWriter::new(GzEncoder::new(
                                                    f_out,
                                                    Compression::default(),
                                                ))
                                            };
                                            Output(Arc::new(Mutex::new(Box::new(
                                                buf_writer_parsed,
                                            ))))
                                        });
                                    entry.value().clone()
                                };
                                let mut writer = output.0.lock().unwrap();
                                writeln!(writer, "{}", json).unwrap();
                            };

                        match msg.msg_type {
                            MessageType::L2Event => {
                                // Skip unsupported markets
                                if !is_blocked_market(real_market_type) {
                                    if let Ok(messages) = parse_l2(
                                        exchange,
                                        msg.market_type,
                                        &msg.json,
                                        Some(msg.received_at as i64),
                                    ) {
                                        for mut message in messages {
                                            assert_eq!(real_market_type, message.market_type);
                                            if message.exchange == "mxc" {
                                                message.exchange = "mexc".to_string();
                                            } else if message.exchange == "okex" {
                                                message.exchange = "okx".to_string();
                                            }
                                            if get_day(message.timestamp) == day {
                                                write_parsed(
                                                    message.market_type,
                                                    serde_json::to_string(&message).unwrap(),
                                                    message.timestamp,
                                                );
                                            } else {
                                                expired_lines += 1;
                                            }
                                        }
                                    } else {
                                        warn!("parse_l2 failed: {}", line);
                                    }
                                }
                            }
                            MessageType::Trade => {
                                if let Ok(messages) =
                                    parse_trade(&msg.exchange, msg.market_type, &msg.json)
                                {
                                    for mut message in messages {
                                        if !(real_market_type == MarketType::InverseSwap
                                            && message.market_type == MarketType::InverseFuture
                                            && message.exchange == "deribit")
                                        {
                                            // For deribit, inverse_swap is included in inverse_future
                                            assert_eq!(
                                                real_market_type, message.market_type,
                                                "{}, {}",
                                                real_market_type, line,
                                            );
                                        }
                                        if message.exchange == "mxc" {
                                            message.exchange = "mexc".to_string();
                                        } else if message.exchange == "okex" {
                                            message.exchange = "okx".to_string();
                                        }
                                        if get_day(message.timestamp) == day {
                                            write_parsed(
                                                message.market_type,
                                                serde_json::to_string(&message).unwrap(),
                                                message.timestamp,
                                            );
                                        } else {
                                            expired_lines += 1;
                                        }
                                    }
                                } else {
                                    warn!("parse_trade failed: {}", line);
                                }
                            }
                            _ => panic!("Unknown msg_type {}", msg.msg_type),
                        };
                    } else {
                        duplicated_lines += 1;
                    }
                } else {
                    warn!("No symbol: {}", line);
                    error_lines += 1;
                }
            } else {
                warn!("Not a valid Message: {}", line);
                error_lines += 1;
            }
        } else {
            error!("malformed file {}", input_file.as_ref().display());
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

fn sort_file<P>(input_file: P, writer: &mut dyn std::io::Write) -> (i64, i64)
where
    P: AsRef<Path>,
{
    assert!(input_file.as_ref().to_str().unwrap().ends_with(".json.gz"));
    if !input_file.as_ref().exists() {
        panic!("{:?} does not exist", input_file.as_ref().display());
    }
    let buf_reader = {
        let f_in = std::fs::File::open(&input_file).unwrap();
        std::io::BufReader::new(GzDecoder::new(f_in))
    };
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
                    warn!("Can NOT find received_at nor timestamp: {}", line);
                    error_lines += 1;
                }
            } else {
                warn!("Not a JSON object: {}", line);
                error_lines += 1;
            }
        } else {
            error!("malformed file {}", input_file.as_ref().display());
            error_lines += 1;
        }
    }
    std::fs::remove_file(input_file.as_ref()).unwrap();
    if error_lines == 0 {
        lines.sort_by_key(|x| x.0); // sort by timestamp

        for line in lines {
            writeln!(writer, "{}", line.1).unwrap();
        }
        writer.flush().unwrap();
    } else {
        error!(
            "Found {} malformed lines out of total {} lines in file {}",
            error_lines,
            total_lines,
            input_file.as_ref().display()
        );
    }
    (error_lines, total_lines)
}

// Use pixz if use_pixz is true, and semaphore allows only two pixz processes
fn sort_files<P>(
    mut hourly_files: Vec<P>,
    output_file: P,
    use_pixz: bool,
    semaphore: Arc<AtomicUsize>,
) -> (i64, i64)
where
    P: AsRef<Path>,
{
    for input_file in hourly_files.iter() {
        assert!(input_file.as_ref().to_str().unwrap().ends_with(".json.gz"));
        if !input_file.as_ref().exists() {
            panic!("{:?} does not exist", input_file.as_ref().display());
        }
    }
    if hourly_files.len() < 24 {
        warn!(
            "There are only {} files for {}",
            hourly_files.len(),
            output_file.as_ref().display()
        );
    }
    hourly_files.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    assert!(output_file.as_ref().to_str().unwrap().ends_with(".json.xz"));

    let mut writer: Box<dyn std::io::Write> = if !use_pixz {
        let f_out = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_file.as_ref())
            .unwrap();
        let e = xz2::write::XzEncoder::new(f_out, 6);
        Box::new(std::io::BufWriter::new(e))
    } else {
        let json_file = {
            let output_dir = output_file.as_ref().parent().unwrap().to_path_buf();
            let filename = output_file.as_ref().file_name().unwrap().to_str().unwrap();
            output_dir.join(&filename[..filename.len() - 3])
        };
        let f_out = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(json_file.as_path())
            .unwrap();
        Box::new(std::io::BufWriter::new(f_out))
    };

    let mut total_lines = 0;
    let mut error_lines = 0;
    for input_file in hourly_files.iter() {
        let (e, t) = sort_file(input_file, writer.as_mut());
        total_lines += t;
        error_lines += e;
    }
    drop(writer);
    if error_lines == 0 {
        if use_pixz {
            {
                // Wait for the semaphore which allows only two pixz processes
                let mut rng = rand::thread_rng();
                while semaphore.load(Ordering::SeqCst) == 0 {
                    let millis = rng.gen_range(3000_u64..10000_u64);
                    debug!("Waiting for semaphore");
                    std::thread::sleep(Duration::from_millis(millis));
                }
                semaphore.fetch_sub(1_usize, Ordering::SeqCst);
            }
            let json_file = {
                let output_dir = output_file.as_ref().parent().unwrap().to_path_buf();
                let filename = output_file.as_ref().file_name().unwrap().to_str().unwrap();
                output_dir.join(&filename[..filename.len() - 3])
            };
            match std::process::Command::new("pixz")
                .args(["-6", json_file.as_path().to_str().unwrap()])
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
        error!(
            "Found {} malformed lines out of total {} total lines for {}",
            error_lines,
            total_lines,
            output_file.as_ref().display()
        );
        // cleanup
        if use_pixz {
            let json_file = {
                let output_dir = output_file.as_ref().parent().unwrap().to_path_buf();
                let filename = output_file.as_ref().file_name().unwrap().to_str().unwrap();
                output_dir.join(&filename[..filename.len() - 3])
            };
            std::fs::remove_file(json_file.as_path()).unwrap();
        } else {
            std::fs::remove_file(output_file.as_ref()).unwrap();
        }
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
        let glob_pattern = if market_type == MarketType::Unknown {
            // MarketType::Unknown means all markets
            format!(
                "{}/*/{}/{}/*/{}.*.{}.{}-??-??.json.gz",
                input_dir, msg_type, exchange, exchange, msg_type, day
            )
        } else {
            format!(
                "{}/*/{}/{}/{}/{}.{}.{}.{}-??-??.json.gz",
                input_dir, msg_type, exchange, market_type, exchange, market_type, msg_type, day
            )
        };
        let mut paths: Vec<PathBuf> = glob(&glob_pattern)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
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
            let glob_pattern = if market_type == MarketType::Unknown {
                // MarketType::Unknown means all markets
                format!(
                    "{}/*/{}/{}/*/{}.*.{}.{}-??.json.gz",
                    input_dir, msg_type, exchange, exchange, msg_type, next_day_first_hour
                )
            } else {
                format!(
                    "{}/*/{}/{}/{}/{}.{}.{}.{}-??.json.gz",
                    input_dir,
                    msg_type,
                    exchange,
                    market_type,
                    exchange,
                    market_type,
                    msg_type,
                    next_day_first_hour
                )
            };
            let mut paths_of_next_day: Vec<PathBuf> = glob(&glob_pattern)
                .unwrap()
                .filter_map(Result::ok)
                .collect();
            paths.append(&mut paths_of_next_day);
        }
        if paths
            .iter()
            .filter(|s| !s.ends_with("-00-00.json.gz"))
            .count()
            == 0
        {
            warn!("There are no files of pattern {}", glob_pattern);
            return true;
        }

        info!(
            "Started split {} {} {} {}",
            exchange, market_type, msg_type, day
        );
        let (tx_raw, rx_raw): (
            Sender<(i64, i64, i64, i64, i64)>,
            Receiver<(i64, i64, i64, i64, i64)>,
        ) = mpsc::channel();
        let (tx_parsed, rx_parsed): (
            Sender<(i64, i64, i64, i64, i64)>,
            Receiver<(i64, i64, i64, i64, i64)>,
        ) = mpsc::channel();
        let start_timstamp = Instant::now();
        // Larger files get processed first
        paths.sort_by_cached_key(|path| Reverse(std::fs::metadata(path).unwrap().len()));

        let written_to_raw: Arc<DashSet<u64>> = Arc::new(DashSet::new());
        let written_to_parsed: Arc<DashSet<u64>> = Arc::new(DashSet::new());
        let splitted_files_raw: Arc<DashMap<String, Output>> = Arc::new(DashMap::new());
        let splitted_files_parsed: Arc<DashMap<String, Output>> = Arc::new(DashMap::new());

        for input_file in paths {
            let file_name = input_file.as_path().file_name().unwrap();
            let v: Vec<&str> = file_name.to_str().unwrap().split('.').collect();
            assert_eq!(exchange, v[0]);
            if market_type != MarketType::Unknown {
                assert_eq!(market_type, MarketType::from_str(v[1]).unwrap());
            }
            let msg_type_str = v[2];
            assert_eq!(msg_type, MessageType::from_str(msg_type_str).unwrap());

            let input_file_clone = input_file.clone();
            let day_clone = day.to_string();
            let exchange_output_dir_raw =
                Path::new(output_dir_raw).join(msg_type_str).join(exchange);
            let splitted_files_raw_clone = splitted_files_raw.clone();
            let written_to_raw_clone = written_to_raw.clone();
            let tx_raw_clone = tx_raw.clone();
            thread_pool.execute(move || {
                let t = split_file_raw(
                    input_file_clone,
                    day_clone,
                    exchange_output_dir_raw,
                    splitted_files_raw_clone,
                    written_to_raw_clone,
                );
                tx_raw_clone.send(t).unwrap();
            });

            let input_file_clone = input_file.clone();
            let day_clone = day.to_string();
            let splitted_files_parsed_clone = splitted_files_parsed.clone();
            let exchange_output_dir_parsed = Path::new(output_dir_parsed)
                .join(msg_type_str)
                .join(exchange);
            let written_to_parsed_clone = written_to_parsed.clone();
            let tx_parsed_clone = tx_parsed.clone();
            thread_pool.execute(move || {
                let t = split_file_parsed(
                    input_file_clone,
                    day_clone,
                    exchange_output_dir_parsed,
                    splitted_files_parsed_clone,
                    written_to_parsed_clone,
                );
                tx_parsed_clone.send(t).unwrap();
            });
        }
        thread_pool.join();
        let finishing = move |tx: Sender<(i64, i64, i64, i64, i64)>,
                              rx: Receiver<(i64, i64, i64, i64, i64)>,
                              splitted_files: Arc<DashMap<String, Output>>,
                              is_parsed: bool|
              -> bool {
            drop(tx); // drop the sender to unblock receiver
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
            if is_parsed {
                assert_eq!(total_lines, unique_lines + duplicated_lines + error_lines);
            } else {
                assert_eq!(
                    total_lines,
                    unique_lines + duplicated_lines + error_lines + expired_lines
                );
            }

            for entry in splitted_files.iter() {
                let output = entry.value();
                output.0.lock().unwrap().flush().unwrap();
            }
            let error_ratio = (error_lines as f64) / (total_lines as f64);
            if error_ratio > 0.01 && !EXEMPTED_EXCHANGES.contains(&exchange) {
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
                false
            } else {
                info!("Finished split {} {} {} {}, total {} lines, {} unique lines, {} duplicated lines, {} expired lines,  {} malformed lines, time elapsed {} seconds", exchange, market_type, msg_type, day, total_lines, unique_lines, duplicated_lines, expired_lines, error_lines, start_timstamp.elapsed().as_secs());
                true
            }
        };
        if !finishing(tx_raw, rx_raw, splitted_files_raw, false)
            || !finishing(tx_parsed, rx_parsed, splitted_files_parsed, true)
        {
            return false;
        }
    }

    // sort by timestamp
    {
        let glob_pattern = if market_type == MarketType::Unknown {
            // MarketType::Unknown means all markets
            format!(
                "/{}/{}/*/{}.*.{}.*.{}-??.json.gz",
                msg_type, exchange, exchange, msg_type, day
            )
        } else if exchange == "deribit"
            && market_type == MarketType::InverseFuture
            && msg_type == MessageType::Trade
        {
            format!(
                "/{}/{}/{{invere_future,inverse_swap}}/{}.*.{}.*.{}-??.json.gz",
                msg_type, exchange, exchange, msg_type, day
            )
        } else {
            format!(
                "/{}/{}/{}/{}.{}.{}.*.{}-??.json.gz",
                msg_type, exchange, market_type, exchange, market_type, msg_type, day
            )
        };

        let paths_raw: Vec<PathBuf> = glob(format!("{}{}", output_dir_raw, glob_pattern).as_str())
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        if paths_raw.is_empty() {
            warn!("There are no files of pattern {}", glob_pattern);
            return true;
        }

        let paths_parsed = glob(format!("{}{}", output_dir_parsed, glob_pattern).as_str())
            .unwrap()
            .filter_map(Result::ok);

        let paths: Vec<PathBuf> = paths_raw.into_iter().chain(paths_parsed).collect();
        for path in paths.iter() {
            assert!(!path.as_path().to_str().unwrap().contains(".unknown."));
        }
        let total_files = paths.len();

        let paths_by_day = {
            // group by day
            let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
            for path in paths {
                let file_name = path.as_path().file_name().unwrap().to_str().unwrap();
                let key = &file_name[0..(file_name.len() - "-??.json.gz".len())];
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
            "Started sort {} {} {} {}",
            exchange, market_type, msg_type, day
        );
        let (tx, rx): (Sender<(i64, i64)>, Receiver<(i64, i64)>) = mpsc::channel();
        let start_timstamp = Instant::now();
        let percentile_90 = ((paths_by_day.len() as f64) * 0.9) as usize;
        let pixz_exists = Path::new("/usr/bin/pixz").exists();
        let semaphore = Arc::new(AtomicUsize::new(MAX_PIXZ));
        for (index, input_files) in paths_by_day.into_iter().enumerate() {
            let file_name = input_files[0]
                .as_path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let output_file_name = format!(
                "{}.json.xz",
                &file_name[0..(file_name.len() - "-??.json.gz".len())]
            );
            let output_file = Path::new(input_files[0].parent().unwrap()).join(output_file_name);
            let tx_clone = tx.clone();
            let semaphore_clone = semaphore.clone();
            if pixz_exists && index >= percentile_90 {
                thread_pool.execute(move || {
                    let t = sort_files(input_files, output_file, true, semaphore_clone);
                    tx_clone.send(t).unwrap();
                });
            } else {
                thread_pool.execute(move || {
                    let t = sort_files(input_files, output_file, false, semaphore_clone);
                    tx_clone.send(t).unwrap();
                });
            }
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
                "Finished sort {} {} {} {}, {} files, total {} lines, time elapsed {} seconds",
                exchange,
                market_type,
                msg_type,
                day,
                total_files,
                total_lines,
                start_timstamp.elapsed().as_secs()
            );
            true
        } else {
            error!(
                "Failed to sort {} {} {} {}, found {} malformed lines out of total {} lines, time elapsed {} seconds",
                exchange,
                market_type,
                msg_type,
                day,
                error_lines, total_lines,
                start_timstamp.elapsed().as_secs()
            );
            // if error ratio is less than 0.00001, the function is considered successful
            (error_lines as f64) / (total_lines as f64) < 0.00001
        }
    }
}

fn main() {
    env_logger::init();
    assert!(setrlimit(Resource::NOFILE, 131072, 131072).is_ok());

    let args: Vec<String> = env::args().collect();
    if args.len() != 8 {
        eprintln!("Usage: crypto-daily-processor <exchange> <msg_type> <market_type> <day> <input_dir> <output_dir_raw> <output_dir_parsed>");
        std::process::exit(1);
    }

    let exchange: &'static str = Box::leak(args[1].clone().into_boxed_str());
    if exchange == "okex" || exchange == "mxc" {
        eprintln!("exchange should NOT be okex nor mxc");
        std::process::exit(1);
    }

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

fn encode_symbol(symbol: &str) -> String {
    let new_symbol = encode(symbol).to_string(); // equivalent to urllib.parse.quote_plus()
    new_symbol.replace(".", "%2E") // escape the dot '.'
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
