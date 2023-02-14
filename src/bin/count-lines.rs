use flate2::read::GzDecoder;
use std::env;
use std::io::prelude::*;
use xz2::read::XzDecoder;

#[allow(clippy::too_many_arguments)]
fn count_lines(buf_reader: &mut dyn std::io::BufRead) -> (u64, u64) {
    let mut total_lines = 0;
    let mut error_lines = 0;
    for line in buf_reader.lines() {
        if line.is_ok() {
            total_lines += 1;
        } else {
            error_lines += 1;
            total_lines += 1;
        }
    }
    (total_lines, error_lines)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: count-lines <file>");
        std::process::exit(1);
    }

    // e.g., binance.linear_swap.trade.BTC.USDT.BTCUSDT.2022-06.csv.xz
    let input_file: &'static str = Box::leak(args[1].clone().into_boxed_str());

    let f_in =
        std::fs::File::open(input_file).unwrap_or_else(|_| panic!("{input_file} does not exist"));
    let mut buf_reader: Box<dyn std::io::BufRead> = if input_file.ends_with(".gz") {
        let d = GzDecoder::new(f_in);
        Box::new(std::io::BufReader::new(d))
    } else if input_file.ends_with(".xz") {
        let d = XzDecoder::new_multi_decoder(f_in);
        Box::new(std::io::BufReader::new(d))
    } else {
        Box::new(std::io::BufReader::new(f_in))
    };

    let (total_lines, error_lines) = count_lines(buf_reader.as_mut());
    println!("total lines {}, error lines {}", total_lines, error_lines)
}
