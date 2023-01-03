use std::io::Write;
use std::collections::HashMap;
use super::ElementTypeCode;

#[derive(Clone, Debug)]
pub struct SerializationOptions {
    /// Determines the minimum amount of map elements that will trigger CHD generation
    /// instead of using a binary search tree.
    /// 
    /// CHD is perfect-hashing-function algorithm that is faster to lookup,
    /// but it takes more time to generate and makse the output larger.
    pub chd_threshold: usize,
}

impl Default for SerializationOptions {
    fn default() -> Self {
        Self { chd_threshold: 8000 }
    }
}

pub trait Serialize {
    fn serialize<W: Write>(&self, options: &SerializationOptions, output: W) -> std::io::Result<usize>;
}

// /// Represents any valid JSON value.
// ///
// /// See the [`serde_json::value` module documentation](self) for usage examples.
// #[derive(Clone, Eq, PartialEq)]
// pub enum Value {
//     Null,
//     Bool(bool),
//     SignedInteger(i64),
//     UnsignedInteger(i64),
//     String(String),
//     Array(Vec<Value>),
//     Object(std::collections::HashMap<String, Value>),
// }

use serde_json::Value;

macro_rules! serialize_integer {
    ($integer_ty:ty, $type_code:expr) => {
        impl Serialize for $integer_ty {
            fn serialize<W: Write>(&self, _options: &SerializationOptions, mut output: W) -> std::io::Result<usize> {
                output.write(&[$type_code as u8])?;
                output.write(&self.to_le_bytes())?;
                Ok(1 + self.to_le_bytes().len())
            }
        }        
    };
}

serialize_integer!(u64, ElementTypeCode::UInt64);
serialize_integer!(i64, ElementTypeCode::Int64);
serialize_integer!(u32, ElementTypeCode::UInt32);
serialize_integer!(i32, ElementTypeCode::Int32);
serialize_integer!(f64, ElementTypeCode::Double);

impl Serialize for &str {
    fn serialize<W: Write>(&self, _options: &SerializationOptions, mut output: W) -> std::io::Result<usize> {
        let mut total = 0;
        total += output.write(&[ElementTypeCode::String as u8])?;
        total += output.write(self.as_bytes())?;
        total += output.write(b"\x00")?;
        Ok(total)
    }
}

impl Serialize for bool {
    fn serialize<W: Write>(&self, _options: &SerializationOptions, mut output: W) -> std::io::Result<usize> {
        output.write(&[
            if *self { ElementTypeCode::True } else { ElementTypeCode::False } as u8
        ])
    }
}

impl Serialize for Value {
    fn serialize<W: Write>(&self, options: &SerializationOptions, mut output: W) -> std::io::Result<usize> {
        match self {
            Value::Null => output.write(&[ElementTypeCode::None as u8]),
            Value::Bool(b) => b.serialize(options, output),
            // Value::SignedInteger(i) => i.serialize(options, output),
            // Value::UnsignedInteger(i) => i.serialize(options, output),
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
            },
        }
    }
}

impl Serialize for &[Value] {
    fn serialize<W: Write>(&self, options: &SerializationOptions, mut output: W) -> std::io::Result<usize> {       
        let mut values = Vec::<u8>::new();
        
        let mut total = 0;
        total += output.write(&[ElementTypeCode::Array as u8])?;
        total += output.write(&(self.len() as u32).to_le_bytes())?;
        
        let mut offset = total + 4 * self.len();
        for item in self.iter() {
            total += output.write(&(offset as u32).to_le_bytes())?;
            offset += item.serialize(options, &mut values)?;
        }

        total += output.write(values.as_slice())?;

        Ok(total)
    }
}

const DEFAULT_LAMBDA: usize = 5;
pub struct HashState {
    pub key: u32,
    pub disps: Vec<(u32, u32)>,
    pub map: Vec<usize>,
}
fn try_generate_hash<'a>(entries: impl Iterator<Item = &'a str>, key: u32) -> Option<HashState> {
    struct Bucket {
        idx: usize,
        keys: Vec<usize>,
    }

    let hashes: Vec<_> = entries
        .map(|entry| phf_shared::hash(entry, &(key as u64)))
        .collect();

    let buckets_len = (hashes.len() + DEFAULT_LAMBDA - 1) / DEFAULT_LAMBDA;
    let mut buckets = (0..buckets_len)
        .map(|i| Bucket {
            idx: i,
            keys: vec![],
        })
        .collect::<Vec<_>>();

    for (i, hash) in hashes.iter().enumerate() {
        buckets[(hash.g % (buckets_len as u32)) as usize]
            .keys
            .push(i);
    }

    // Sort descending
    buckets.sort_by(|a, b| a.keys.len().cmp(&b.keys.len()).reverse());

    let table_len = hashes.len();
    let mut map = vec![None; table_len];
    let mut disps = vec![(0u32, 0u32); buckets_len];

    // store whether an element from the bucket being placed is
    // located at a certain position, to allow for efficient overlap
    // checks. It works by storing the generation in each cell and
    // each new placement-attempt is a new generation, so you can tell
    // if this is legitimately full by checking that the generations
    // are equal. (A u64 is far too large to overflow in a reasonable
    // time for current hardware.)
    let mut try_map = vec![0u64; table_len];
    let mut generation = 0u64;

    // the actual values corresponding to the markers above, as
    // (index, key) pairs, for adding to the main map once we've
    // chosen the right disps.
    let mut values_to_add = vec![];

    'buckets: for bucket in &buckets {
        for d1 in 0..(table_len as u32) {
            'disps: for d2 in 0..(table_len as u32) {
                values_to_add.clear();
                generation += 1;

                for &key in &bucket.keys {
                    let idx = (phf_shared::displace(hashes[key].f1, hashes[key].f2, d1, d2)
                        % (table_len as u32)) as usize;
                    if map[idx].is_some() || try_map[idx] == generation {
                        continue 'disps;
                    }
                    try_map[idx] = generation;
                    values_to_add.push((idx, key));
                }

                // We've picked a good set of disps
                disps[bucket.idx] = (d1, d2);
                for &(idx, key) in &values_to_add {
                    map[idx] = Some(key);
                }
                continue 'buckets;
            }
        }

        // Unable to find displacements for a bucket
        return None;
    }

    Some(HashState {
        key,
        disps,
        map: map.into_iter().map(|i| i.unwrap()).collect(),
    })
}

fn encode_kvs<W: Write>(kvs: &[(&str, &Value, usize)], options: &SerializationOptions, mut output: W, descriptors_offset: usize) -> std::io::Result<usize> {
    let mut serialized_values = Vec::<u8>::new();

    let mut current_key_offset = descriptors_offset + 8 * kvs.len();
    let total_key_size: usize = kvs.iter().map(|(key, _value, _index)| key.len() + 1).sum();
    let mut current_value_offset = current_key_offset + total_key_size;
    let mut total_written = 0;

    for (key, value, _index) in kvs.iter() {
        let key_length = key.len();

        let value_length = value.serialize(options, &mut serialized_values)?;

        let key_data = ((key_length as u32) << 24) | (current_key_offset as u32);
        total_written += output.write(&key_data.to_le_bytes())?;
        total_written += output.write(&(current_value_offset as u32).to_le_bytes())?;

        current_key_offset += key_length + 1;
        current_value_offset += value_length;
    }

    for (key, _value, _index) in kvs.iter() {
        total_written += output.write(key.as_bytes())?;
        total_written += output.write(&[0u8])?;
    }
    total_written += output.write(serialized_values.as_ref())?;
    
    Ok(total_written)
}

fn serialize_chd<'a, W: Write>(map: impl Iterator<Item=(&'a str, &'a Value)>, options: &SerializationOptions, mut output: W) -> std::io::Result<usize> {
    let kvs: Vec<_> = map.map(|(k, v)| (k, v)).collect();
    let mut i = 0;
    let hash_state = loop {
        if let Some(hash_state) = try_generate_hash(kvs.iter().map(|(k, _v)| *k), 0x500+i) {
            break hash_state
        }
        i += 1;
        if i > 10 {
            Err(std::io::ErrorKind::InvalidData)?;
        }
    };
    let kvs: Vec<_> = hash_state.map.iter().map(|source_index| {
        let (k, v) = kvs[*source_index];
        (k, v, 0)
    }).collect();
    
    let mut total_written = 0;
    total_written += output.write(&[ElementTypeCode::MapCHD as u8])?;
    total_written += output.write(&(kvs.len() as u32).to_le_bytes())?;
    total_written += output.write(&hash_state.key.to_le_bytes())?;
    for (d1, d2) in hash_state.disps.into_iter() {
        total_written += output.write(&d1.to_le_bytes())?;
        total_written += output.write(&d2.to_le_bytes())?;
    }
    
    total_written += encode_kvs(&kvs, options, output, total_written)?;

    Ok(total_written)
}

fn serialize_eytzinger<'a, W: Write>(map: impl Iterator<Item=(&'a str, &'a Value)>, options: &SerializationOptions, mut output: W) -> std::io::Result<usize> {
    let mut kvs: Vec<_> = map.map(|(k, v)| (k, v, 0usize)).collect();
    kvs.sort_by_key(|(key, _value, _index)| *key);
    for (idx, idx1) in eytzinger::PermutationGenerator::new(kvs.len()).enumerate() {
        kvs[idx].2 = idx1;
    }
    kvs.sort_by_key(|(_key, _value, index)| *index);

    let mut total_written = 0;
    total_written += output.write(&[ElementTypeCode::Map as u8])?;
    total_written += output.write(&(kvs.len() as u32).to_le_bytes())?;

    total_written += encode_kvs(&kvs, options, output, total_written)?;

    Ok(total_written)
}

impl<S: AsRef<str>, HS> Serialize for HashMap<S, Value, HS> {
    fn serialize<W: Write>(&self, options: &SerializationOptions, output: W) -> std::io::Result<usize> {
        let kvs = self.iter().map(|(k, v)| (k.as_ref(), v));
        if self.len() >= options.chd_threshold {
            serialize_chd(kvs, options, output)
        } else {
            serialize_eytzinger(kvs, options, output)
        }
    }
}

impl Serialize for serde_json::Map<String, Value> {
    fn serialize<W: Write>(&self, options: &SerializationOptions, output: W) -> std::io::Result<usize> {
        let kvs = self.iter().map(|(k, v)| (k.as_ref(), v));
        if self.len() >= options.chd_threshold {
            serialize_chd(kvs, options, output)
        } else {
            serialize_eytzinger(kvs, options, output)
        }
    }
}