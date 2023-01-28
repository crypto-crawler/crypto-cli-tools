use crypto_market_type::MarketType;
use crypto_msg_parser::{parse_l2, parse_l2_topk, parse_trade};
use crypto_msg_type::MessageType;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use log::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::env;
use std::io::prelude::*;
use std::path::Path;
use std::str::FromStr;

fn parse_lines(
    exchange: &str,
    market_type: MarketType,
    msg_type: MessageType,
    buf_reader: &mut dyn std::io::BufRead,
    writer: &mut dyn std::io::Write,
) -> bool {
    let capacity = 2048; // max number of elements in min heap
    let mut heap = BinaryHeap::<(Reverse<i64>, String)>::with_capacity(capacity);
    let mut write = |timestamp: i64, line: String| {
        if heap.len() >= capacity {
            let t = heap.pop().unwrap();
            writeln!(writer, "{}", t.1).unwrap();
        }
        heap.push((Reverse(timestamp), line));
    };

    for line in buf_reader.lines() {
        let line = line.unwrap();

        let arr = line.split('\t').collect::<Vec<&str>>();
        assert_eq!(arr.len(), 3, "Not 3 columns: {line}");
        let timestamp = arr[1].parse::<i64>().unwrap();
        let raw = arr[2];

        match msg_type {
            MessageType::Trade => {
                let messages =
                    parse_trade(exchange, market_type, raw).unwrap_or_else(|_| panic!("{}", line));
                for message in messages {
                    let csv_str = message.to_csv_string();
                    write(message.timestamp, csv_str);
                }
            }
            MessageType::L2Event => {
                let messages = parse_l2(exchange, market_type, raw, Some(timestamp))
                    .unwrap_or_else(|_| panic!("{}", line));
                for message in messages {
                    let csv_str = message.to_csv_string();
                    write(message.timestamp, csv_str);
                }
            }
            MessageType::L2TopK => {
                let messages = parse_l2_topk(exchange, market_type, raw, Some(timestamp))
                    .unwrap_or_else(|_| panic!("{}", line));
                for message in messages {
                    let csv_str = message.to_csv_string();
                    write(message.timestamp, csv_str);
                }
            }
            _ => panic!("Unsupported msg_type {msg_type}"),
        }
    }

    while let Some(t) = heap.pop() {
        writeln!(writer, "{}", t.1).unwrap();
    }
    writer.flush().unwrap();

    true
}

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: crypto-msg-parser <input_file> <output_file>");
        std::process::exit(1);
    }

    if !Path::new("/usr/bin/xz").exists() {
        eprintln!("/usr/bin/xz not found, please install it!");
        return;
    }

    let input_file: &'static str = Box::leak(args[1].clone().into_boxed_str());
    let output_file: &'static str = Box::leak(args[2].clone().into_boxed_str());
    if !input_file.ends_with(".csv")
        && !input_file.ends_with(".csv.gz")
        && !input_file.ends_with(".csv.xz")
    {
        eprintln!("{input_file} suffix should be .csv, .csv.gz or .csv.xz");
        std::process::exit(1);
    }
    if !output_file.ends_with(".csv")
        && !output_file.ends_with(".csv.gz")
        && !output_file.ends_with(".csv.xz")
    {
        eprintln!("{output_file} suffix should be .csv, .csv.gz or .csv.xz");
        std::process::exit(1);
    }

    let (exchange, market_type, msg_type) = {
        let file_name = Path::new(input_file).file_name().unwrap().to_str().unwrap();
        let v: Vec<&str> = file_name.split('.').collect();
        (
            v[0],
            MarketType::from_str(v[1]).unwrap(),
            MessageType::from_str(v[2]).unwrap(),
        )
    };
    match msg_type {
        MessageType::Trade | MessageType::L2Event | MessageType::L2TopK => (),
        _ => panic!("Unsupported msg_type {msg_type}, file {input_file}"),
    }
    match market_type {
        MarketType::EuropeanOption
        | MarketType::QuantoSwap
        | MarketType::QuantoFuture
        | MarketType::Unknown => panic!(
            "Unsupported market_type {market_type}, file {input_file}"
        ),
        _ => (),
    }

    let f_in =
        std::fs::File::open(input_file).unwrap_or_else(|_| panic!("{input_file} does not exist"));
    let mut buf_reader: Box<dyn std::io::BufRead> = if input_file.ends_with(".gz") {
        let d = GzDecoder::new(f_in);
        Box::new(std::io::BufReader::new(d))
    } else if input_file.ends_with(".xz") {
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
    let mut writer: Box<dyn std::io::Write> = if output_file.ends_with(".gz") {
        let encoder = GzEncoder::new(f_out, Compression::best());
        Box::new(std::io::BufWriter::new(encoder))
    } else if output_file.ends_with(".xz") {
        let e = xz2::write::XzEncoder::new(f_out, 9);
        Box::new(std::io::BufWriter::new(e))
    } else {
        Box::new(std::io::BufWriter::new(f_out))
    };

    let success = parse_lines(
        exchange,
        market_type,
        msg_type,
        buf_reader.as_mut(),
        writer.as_mut(),
    );

    if success {
        info!("Succeeded to parse {}", input_file);
    } else {
        error!("Failed to parse {}", input_file);
        std::process::exit(1);
    }
}
