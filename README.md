# crypto-cli-tools

A collection of CLI tools to process cryptocurrency data

## crypto-msg-parser

Parse raw strings from `crypto-crawler` or re-parse messages generated by `crypto-msg-parser`. Output messages are sorted by timestamp and deduplicated.

Usage: `crypto-msg-parser <input_file> <output_file> [yyyy-MM]`.

If month is specified, only messages of the month will be kept.

## crypto-daily-processor

Usage: `crypto-daily-processor <exchange> <msg_type> <market_type> <day> <input_dir> <output_dir_raw> <output_dir_parsed>`

For example:

```bash
crypto-daily-processor bitstamp trade spot 2021-09-02 /mnt/dpool/download /mnt/dpool/daily-raw /mnt/dpool/daily-parsed
```
