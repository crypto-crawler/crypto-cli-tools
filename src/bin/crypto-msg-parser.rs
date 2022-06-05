use chrono::prelude::*;
use chrono::DateTime;
use crypto_market_type::MarketType;
use crypto_msg_parser::{parse_l2, parse_trade, OrderBookMsg, TradeMsg};
use crypto_msg_type::MessageType;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use regex::Regex;
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::path::Path;
use std::str::FromStr;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    env,
};

fn string_hash(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

fn match_month_or_day(timestamp_millis: i64, month_or_day: &str) -> bool {
    let naive = NaiveDateTime::from_timestamp(timestamp_millis / 1000, 0);
    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
    let datetime_str = if month_or_day.len() == 7 {
        datetime.format("%Y-%m").to_string()
    } else if month_or_day.len() == 10 {
        datetime.format("%Y-%m-%d").to_string()
    } else {
        panic!("Invalid time {}", month_or_day);
    };
    month_or_day == datetime_str
}

fn parse_lines(
    buf_reader: &mut dyn std::io::BufRead,
    writer: &mut dyn std::io::Write,
    month_or_day: Option<&str>,
) -> (i64, i64) {
    let mut total_lines = 0;
    let mut error_lines = 0;
    let mut visited = HashSet::<u64>::new();
    let capacity = 2048; // max number of elements in min heap
    let mut heap = BinaryHeap::<(Reverse<i64>, String)>::with_capacity(capacity);
    let mut write = |timestamp: i64, line: String| {
        if month_or_day.is_some() && !match_month_or_day(timestamp, month_or_day.unwrap()) {
            return;
        }
        if heap.len() >= capacity {
            let t = heap.pop().unwrap();
            writeln!(writer, "{}", t.1).unwrap();
            let hashcode = string_hash(&t.1);
            visited.remove(&hashcode);
        }
        if visited.insert(string_hash(&line)) {
            heap.push((Reverse(timestamp), line));
        }
    };
    let mut is_reparse = false;

    for line in buf_reader.lines() {
        match line {
            Ok(line) => {
                total_lines += 1;
                if let Ok(json_obj) = serde_json::from_str::<HashMap<String, Value>>(&line) {
                    if !json_obj.contains_key("exchange")
                        && !json_obj.contains_key("market_type")
                        && !json_obj.contains_key("msg_type")
                    {
                        error_lines += 1;
                    } else {
                        let exchange = json_obj
                            .get("exchange")
                            .expect("No exchange field!")
                            .as_str()
                            .unwrap();
                        let market_type = json_obj
                            .get("market_type")
                            .expect("No market_type field!")
                            .as_str()
                            .unwrap();
                        let msg_type = json_obj
                            .get("msg_type")
                            .expect("No msg_type field!")
                            .as_str()
                            .unwrap();
                        let market_type = MarketType::from_str(market_type).unwrap();
                        let msg_type = MessageType::from_str(msg_type).unwrap();
                        if json_obj.contains_key("received_at") {
                            // raw messages from crypto-crawler
                            let timestamp = json_obj
                                .get("received_at")
                                .expect("No received_at field!")
                                .as_i64();
                            let raw = json_obj
                                .get("json")
                                .expect("No json field!")
                                .as_str()
                                .unwrap();
                            match msg_type {
                                MessageType::L2Event => {
                                    if let Ok(messages) =
                                        parse_l2(exchange, market_type, raw, timestamp)
                                    {
                                        for message in messages {
                                            let json_str = serde_json::to_string(&message).unwrap();
                                            write(message.timestamp, json_str);
                                        }
                                    } else {
                                        error_lines += 1;
                                    }
                                }
                                MessageType::Trade => {
                                    if let Ok(messages) = parse_trade(exchange, market_type, raw) {
                                        for message in messages {
                                            let json_str = serde_json::to_string(&message).unwrap();
                                            write(message.timestamp, json_str);
                                        }
                                    } else {
                                        error_lines += 1;
                                    }
                                }
                                _ => panic!("Unknown msg_type {}", msg_type),
                            }
                        } else {
                            // re-parse OrderBookMsg and TradeMsg
                            is_reparse = true;
                            match msg_type {
                                MessageType::L2Event => {
                                    let msg = serde_json::from_str::<OrderBookMsg>(&line).unwrap();
                                    if let Ok(messages) = parse_l2(
                                        exchange,
                                        market_type,
                                        &msg.json,
                                        Some(msg.timestamp),
                                    ) {
                                        for message in messages {
                                            let json_str = serde_json::to_string(&message).unwrap();
                                            write(message.timestamp, json_str);
                                        }
                                    } else {
                                        error_lines += 1;
                                    }
                                }
                                MessageType::Trade => {
                                    let msg = serde_json::from_str::<TradeMsg>(&line).unwrap();
                                    if let Ok(messages) =
                                        parse_trade(exchange, market_type, &msg.json)
                                    {
                                        for message in messages {
                                            let json_str = serde_json::to_string(&message).unwrap();
                                            write(message.timestamp, json_str);
                                        }
                                    } else {
                                        error_lines += 1;
                                    }
                                }
                                _ => panic!("Unknown msg_type {}", msg_type),
                            }
                        }
                    }
                } else {
                    error_lines += 1;
                }
            }
            Err(err) => {
                // Err(Custom { kind: Other, error: "corrupt xz stream" })
                error_lines += 1;
                if is_reparse {
                    panic!("{:?}", err);
                } else {
                    break;
                }
            }
        }
    }
    while let Some(t) = heap.pop() {
        writeln!(writer, "{}", t.1).unwrap();
        let hashcode = string_hash(&t.1);
        visited.remove(&hashcode);
    }
    debug_assert!(visited.is_empty());
    writer.flush().unwrap();
    (error_lines, total_lines)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 && args.len() != 4 {
        eprintln!("Usage: crypto-msg-parser <input_file> <output_file> [yyyy-MM(-dd)]]");
        std::process::exit(1);
    }

    if !Path::new("/usr/bin/xz").exists() {
        eprintln!("/usr/bin/xz not found, please install it!");
        return;
    }

    let input_file: &'static str = Box::leak(args[1].clone().into_boxed_str());
    let output_file: &'static str = Box::leak(args[2].clone().into_boxed_str());
    if !input_file.ends_with(".json")
        && !input_file.ends_with(".json.gz")
        && !input_file.ends_with(".json.xz")
    {
        eprintln!(
            "{} suffix should be .json, .json.gz or .json.xz",
            input_file
        );
        std::process::exit(1);
    }
    if !output_file.ends_with(".json")
        && !output_file.ends_with(".json.gz")
        && !output_file.ends_with(".json.xz")
    {
        eprintln!(
            "{} suffix should be .json, .json.gz or .json.xz",
            output_file
        );
        std::process::exit(1);
    }
    let month_or_day = if args.len() == 4 {
        let m: &'static str = Box::leak(args[3].clone().into_boxed_str());
        let re = Regex::new(r"^\d{4}-\d{2}(-\d{2})?$").unwrap();
        if !re.is_match(m) {
            eprintln!(
                "{} is invalid, timestamp should be yyyy-MM or yyyy-MM-dd",
                m
            );
            std::process::exit(1);
        }
        Some(m)
    } else {
        None
    };

    let f_in =
        std::fs::File::open(input_file).unwrap_or_else(|_| panic!("{} does not exist", input_file));
    let mut buf_reader: Box<dyn std::io::BufRead> = if input_file.ends_with(".json.gz") {
        let d = GzDecoder::new(f_in);
        Box::new(std::io::BufReader::new(d))
    } else if input_file.ends_with(".json.xz") {
        let d = xz2::read::XzDecoder::new_multi_decoder(f_in);
        Box::new(std::io::BufReader::new(d))
    } else {
        Box::new(std::io::BufReader::new(f_in))
    };

    let output_dir = std::path::Path::new(output_file).parent().unwrap();
    std::fs::create_dir_all(output_dir).unwrap();
    let f_out = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(output_file)
        .unwrap();
    let mut writer: Box<dyn std::io::Write> = if output_file.ends_with(".json.gz") {
        let encoder = GzEncoder::new(f_out, Compression::best());
        Box::new(std::io::BufWriter::new(encoder))
    } else if output_file.ends_with(".json.xz") {
        let e = xz2::write::XzEncoder::new(f_out, 9);
        Box::new(std::io::BufWriter::new(e))
    } else {
        Box::new(std::io::BufWriter::new(f_out))
    };

    let (error_lines, total_lines) =
        parse_lines(buf_reader.as_mut(), writer.as_mut(), month_or_day);
    let error_ratio = (error_lines as f64) / (total_lines as f64);
    if error_ratio > 0.01 {
        eprintln!(
                "Parse failed, dropped {} malformed lines out of {} lines, error ratio {}% is higher than 1%",
                error_lines, total_lines,
                error_ratio * 100.0,
            );
        std::process::exit(1);
    } else {
        println!(
            "Parse succeeded, dropped {} malformed lines out of {} lines",
            error_lines, total_lines
        );
    }
}
