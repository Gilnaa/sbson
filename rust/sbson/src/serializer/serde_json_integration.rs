use crate::{
    serializer::{serialize_chd, serialize_eytzinger, SerializationOptions, Serialize},
    ElementTypeCode,
};
use serde_json::Value;
use std::io::Write;

impl Serialize for serde_json::Map<String, Value> {
    fn serialize(
        &self,
        options: &SerializationOptions,
        output: &mut Vec<u8>,
    ) -> std::io::Result<usize> {
        let kvs = self.iter().map(|(k, v)| (k.as_ref(), v));
        if self.len() >= options.chd_threshold {
            serialize_chd(kvs, options, output)
        } else {
            serialize_eytzinger(kvs, options, output)
        }
    }
}

impl Serialize for Value {
    fn serialize(
        &self,
        options: &SerializationOptions,
        output: &mut Vec<u8>,
    ) -> std::io::Result<usize> {
        match self {
            Value::Null => output.write(&[ElementTypeCode::None as u8]),
            Value::Bool(b) => b.serialize(options, output),
            Value::String(s) => s.as_str().serialize(options, output),
            Value::Array(val) => val.as_slice().serialize(options, output),
            Value::Object(m) => m.serialize(options, output),
            Value::Number(num) => {
                if let Some(u) = num.as_u64() {
                    return u.serialize(options, output);
                }
                if let Some(i) = num.as_i64() {
                    return i.serialize(options, output);
                }
                if let Some(f) = num.as_f64() {
                    return f.serialize(options, output);
                }
                unreachable!("No variants left");
            }
        }
    }
}
