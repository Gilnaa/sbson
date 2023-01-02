use std::str::FromStr;
use sbson::serializer::{Serialize, SerializationOptions};
use sbson;
use serde_json;

fn main() {
    let foo: Vec<_> = std::env::args().collect();
    if foo.len() != 3 {
        eprintln!("Usage: encode <input>.json <output>.json");
        std::process::exit(1);
    }

    let s = std::fs::read_to_string(&foo[1]).unwrap();

    let js_start = std::time::Instant::now();
    let value = serde_json::Value::from_str(&s).unwrap();
    let js_end = std::time::Instant::now();

    let options = SerializationOptions { chd_threshold: 512 };
    let mut output = Vec::<u8>::new();
    let sb_start = std::time::Instant::now();
    value.serialize(&options, &mut output).unwrap();
    let sb_end = std::time::Instant::now();

    eprintln!("{:?} {:?}", js_end.duration_since(js_start), sb_end.duration_since(sb_start));
    std::fs::write(&foo[2], output).unwrap();
}