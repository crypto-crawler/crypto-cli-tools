use crypto_message::{OrderBookMsg, TradeMsg};
use crypto_msg_type::MessageType;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use log::*;
use std::env;
use std::io::prelude::*;
use std::path::Path;
use std::str::FromStr;

#[allow(clippy::too_many_arguments)]
fn parse_lines(
    exchange: &str,
    market_type: &str,
    msg_type: &str,
    base: &str,
    quote: &str,
    symbol: &str,
    buf_reader: &mut dyn std::io::BufRead,
    writer: &mut dyn std::io::Write,
) -> bool {
    let pair = format!("{base}/{quote}");
    let msg_type_enum = MessageType::from_str(msg_type).unwrap();
    for line in buf_reader.lines() {
        let line = line.unwrap();
        match msg_type_enum {
            MessageType::Trade => {
                let trade_msg = TradeMsg::from_csv_string(
                    exchange,
                    market_type,
                    msg_type,
                    pair.as_str(),
                    symbol,
                    line.as_str(),
                );
                let proto_msg = trade_msg.to_proto();
                delimited_protobuf::write(&proto_msg, writer).unwrap();
            }
            MessageType::L2Event | MessageType::L2TopK => {
                let orderbook_msg = OrderBookMsg::from_csv_string(
                    exchange,
                    market_type,
                    msg_type,
                    pair.as_str(),
                    symbol,
                    line.as_str(),
                );
                let proto_msg = orderbook_msg.to_proto();
                delimited_protobuf::write(&proto_msg, writer).unwrap();
            }
            _ => panic!("Unsupported msg_type {msg_type}"),
        }
    }

    writer.flush().unwrap();

    true
}

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: crypto-msg-parser <csv_file> <proto_file>");
        std::process::exit(1);
    }

    // e.g., binance.linear_swap.trade.BTC.USDT.BTCUSDT.2022-06.csv.xz
    let csv_file: &'static str = Box::leak(args[1].clone().into_boxed_str());
    // e.g., binance.linear_swap.trade.BTC.USDT.BTCUSDT.2022-06.proto.xz
    let proto_file: &'static str = Box::leak(args[2].clone().into_boxed_str());
    if !csv_file.ends_with(".csv")
        && !csv_file.ends_with(".csv.gz")
        && !csv_file.ends_with(".csv.xz")
    {
        eprintln!("{csv_file} suffix should be .csv, .csv.gz or .csv.xz");
        std::process::exit(1);
    }
    if !proto_file.ends_with(".proto")
        && !proto_file.ends_with(".proto.gz")
        && !proto_file.ends_with(".proto.xz")
    {
        eprintln!("{proto_file} suffix should be .proto, .proto.gz or .proto.xz");
        std::process::exit(1);
    }

    let (exchange, market_type, msg_type, base, quote, symbol) = {
        let file_name = Path::new(csv_file).file_name().unwrap().to_str().unwrap();
        let v: Vec<&str> = file_name.split('.').collect();
        (v[0], v[1], v[2], v[3], v[4], v[5])
    };

    let f_in =
        std::fs::File::open(csv_file).unwrap_or_else(|_| panic!("{csv_file} does not exist"));
    let mut buf_reader: Box<dyn std::io::BufRead> = if csv_file.ends_with(".gz") {
        let d = GzDecoder::new(f_in);
        Box::new(std::io::BufReader::new(d))
    } else if csv_file.ends_with(".xz") {
        let d = xz2::read::XzDecoder::new_multi_decoder(f_in);
        Box::new(std::io::BufReader::new(d))
    } else {
        Box::new(std::io::BufReader::new(f_in))
    };

    let output_dir = std::path::Path::new(proto_file).parent().unwrap();
    std::fs::create_dir_all(output_dir).unwrap();
    let f_out = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(proto_file)
        .unwrap();
    let mut writer: Box<dyn std::io::Write> = if proto_file.ends_with(".gz") {
        let encoder = GzEncoder::new(f_out, Compression::best());
        Box::new(std::io::BufWriter::new(encoder))
    } else if proto_file.ends_with(".xz") {
        let e = xz2::write::XzEncoder::new(f_out, 7);
        Box::new(std::io::BufWriter::new(e))
    } else {
        Box::new(std::io::BufWriter::new(f_out))
    };

    let success = parse_lines(
        exchange,
        market_type,
        msg_type,
        base,
        quote,
        symbol,
        buf_reader.as_mut(),
        writer.as_mut(),
    );

    if success {
        info!("Succeeded to parse {}", csv_file);
    } else {
        error!("Failed to parse {}", csv_file);
        std::process::exit(1);
    }
}
