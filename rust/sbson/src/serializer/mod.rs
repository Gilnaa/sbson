use super::ElementTypeCode;
use std::collections::HashMap;
use std::io::Write;

mod serde_json_integration;

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
        Self {
            chd_threshold: 8000,
        }
    }
}

pub trait Serialize {
    fn serialize(
        &self,
        options: &SerializationOptions,
        output: &mut Vec<u8>,
    ) -> std::io::Result<usize>;
}

impl<T: Serialize> Serialize for &T {
    fn serialize(
        &self,
        options: &SerializationOptions,
        output: &mut Vec<u8>,
    ) -> std::io::Result<usize> {
        (*self).serialize(options, output)
    }
}

macro_rules! serialize_integer {
    ($integer_ty:ty, $type_code:expr) => {
        impl Serialize for $integer_ty {
            fn serialize(
                &self,
                _options: &SerializationOptions,
                output: &mut Vec<u8>,
            ) -> std::io::Result<usize> {
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
    fn serialize(
        &self,
        _options: &SerializationOptions,
        output: &mut Vec<u8>,
    ) -> std::io::Result<usize> {
        let mut total = 0;
        total += output.write(&[ElementTypeCode::String as u8])?;
        total += output.write(self.as_bytes())?;
        total += output.write(b"\x00")?;
        Ok(total)
    }
}

impl Serialize for bool {
    fn serialize(
        &self,
        _options: &SerializationOptions,
        output: &mut Vec<u8>,
    ) -> std::io::Result<usize> {
        output.write(&[if *self {
            ElementTypeCode::True
        } else {
            ElementTypeCode::False
        } as u8])
    }
}

impl<T: Serialize> Serialize for &[T] {
    fn serialize(
        &self,
        options: &SerializationOptions,
        output: &mut Vec<u8>,
    ) -> std::io::Result<usize> {
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

/// Encodes the specified `key_value_pairs` in the order given into `output`.
/// The output is appended with all of their descriptors, followed by their keys.
/// Finally, each of the values is serialized into the `output`.
///
/// The offsets in the descriptors are calculated relative to `descriptors_offset`,
/// which includes the size of all elements prior to the data encoded by this function.
///
/// In other words, this parameter described the amount of bytes that were already written as part of this node.
fn encode_kvs<V: Serialize>(
    key_value_pairs: &[&(&str, V)],
    options: &SerializationOptions,
    output: &mut Vec<u8>,
    descriptors_offset: usize,
) -> std::io::Result<usize> {
    let total_descriptor_size = 8 * key_value_pairs.len();
    let mut current_key_offset = descriptors_offset + total_descriptor_size;
    let total_key_size: usize = key_value_pairs
        .iter()
        .map(|(key, _value)| key.len() + 1)
        .sum();
    let mut current_value_offset = current_key_offset + total_key_size;
    let mut total_written = 0;

    // Save the current end of the buffer so we know where to return to later.
    let absolute_descriptor_offset = output.len();
    output.extend(std::iter::repeat(0u8).take(total_descriptor_size));
    total_written += total_descriptor_size;
    let mut descriptors = Vec::with_capacity(total_descriptor_size);

    for (key, _value) in key_value_pairs.iter() {
        total_written += output.write(key.as_bytes())?;
        total_written += output.write(&[0u8])?;
    }

    for (key, value) in key_value_pairs.iter() {
        let key_length = key.len();

        let value_length = value.serialize(options, output)?;
        total_written += value_length;

        let key_data = ((key_length as u32) << 24) | (current_key_offset as u32);
        descriptors.extend_from_slice(&key_data.to_le_bytes());
        descriptors.extend_from_slice(&(current_value_offset as u32).to_le_bytes());

        current_key_offset += key_length + 1;
        current_value_offset += value_length;
    }

    (&mut output[absolute_descriptor_offset..absolute_descriptor_offset + total_descriptor_size])
        .copy_from_slice(&descriptors);

    Ok(total_written)
}

fn serialize_chd<'a, V: Serialize>(
    map: impl Iterator<Item = (&'a str, V)>,
    options: &SerializationOptions,
    output: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let kvs: Vec<_> = map.map(|(k, v)| (k, v)).collect();
    let mut i = 0;
    let hash_state = loop {
        if let Some(hash_state) = try_generate_hash(kvs.iter().map(|(k, _v)| *k), 0x500 + i) {
            break hash_state;
        }
        i += 1;
        if i > 10 {
            Err(std::io::ErrorKind::InvalidData)?;
        }
    };
    let kvs: Vec<_> = hash_state
        .map
        .iter()
        .map(|source_index| &kvs[*source_index])
        .collect();

    let mut total_written = 0;
    total_written += output.write(&[ElementTypeCode::MapCHD as u8])?;
    total_written += output.write(&(kvs.len() as u32).to_le_bytes())?;
    total_written += output.write(&hash_state.key.to_le_bytes())?;
    for (d1, d2) in hash_state.disps.into_iter() {
        total_written += output.write(&d1.to_le_bytes())?;
        total_written += output.write(&d2.to_le_bytes())?;
    }

    total_written += encode_kvs(&kvs[..], options, output, total_written)?;

    Ok(total_written)
}

fn serialize_eytzinger<'a, V: Serialize>(
    map: impl Iterator<Item = (&'a str, V)>,
    options: &SerializationOptions,
    output: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let mut kvs: Vec<_> = map.map(|(k, v)| (k, v)).collect();
    kvs.sort_by_key(|(key, _value)| *key);

    let kvs: Vec<_> = eytzinger::PermutationGenerator::new(kvs.len())
        .map(|source_index| &kvs[source_index])
        .collect();

    let mut total_written = 0;
    total_written += output.write(&[ElementTypeCode::Map as u8])?;
    total_written += output.write(&(kvs.len() as u32).to_le_bytes())?;

    total_written += encode_kvs(&kvs[..], options, output, total_written)?;

    Ok(total_written)
}

impl<K: AsRef<str>, V: Serialize, HS> Serialize for HashMap<K, V, HS> {
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

#[cfg(test)]
mod tests {
    use crate::Cursor;

    use super::*;

    fn assert_serialized_equals<T: Serialize>(value: T, expected: &[u8]) {
        let mut buf = Vec::<u8>::new();

        buf.clear();
        value
            .serialize(&SerializationOptions::default(), &mut buf)
            .unwrap();
        assert_eq!(buf.as_slice(), expected);
    }

    #[test]
    #[rustfmt::skip]
    fn test_primitive_serialization() {
        assert_serialized_equals(1f64,                      b"\x01\x00\x00\x00\x00\x00\x00\xf0\x3f");
        assert_serialized_equals(false,                     b"\x08");
        assert_serialized_equals(true,                      b"\x09");
        assert_serialized_equals(-2i32,                     b"\x10\xFE\xFF\xFF\xFF");
        assert_serialized_equals(0xAABBCCDDu32,             b"\x11\xDD\xCC\xBB\xAA");
        assert_serialized_equals(-2i64,                     b"\x12\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
        assert_serialized_equals(0x00AA00BB00CC00DDu64,     b"\x13\xDD\x00\xCC\x00\xBB\x00\xAA\x00");
    }

    #[test]
    #[rustfmt::skip]
    fn test_array_serialization() {
        assert_serialized_equals(&[false][..],          b"\x04\x01\x00\x00\x00\x09\x00\x00\x00\x08");
        assert_serialized_equals(&[true][..],           b"\x04\x01\x00\x00\x00\x09\x00\x00\x00\x09");
        assert_serialized_equals(&[true, false][..],    b"\x04\x02\x00\x00\x00\x0D\x00\x00\x00\x0E\x00\x00\x00\x09\x08");
    }

    /// Test a super simple map to make sure it vaguely generates into
    /// our expected format.
    ///
    /// More complicated maps are tested elsewhere.
    #[test]
    fn test_simple_map_serialization() {
        let map = HashMap::from([("key", true)]);
        assert_serialized_equals(
            map,
            b"\x03\x01\x00\x00\x00\x0D\x00\x00\x03\x11\x00\x00\x00key\x00\x09",
        )
    }

    /// Maps are too complex to write by hand, so instead of creating a test vector,
    /// we serialize an object and test it using a cursor.
    #[test]
    fn test_map_serialization() {
        // Perform the test for both CHD and eytzinger representations.
        let option_sets = [
            SerializationOptions { chd_threshold: 500 },
            SerializationOptions {
                chd_threshold: 1500,
            },
        ];

        for options in option_sets {
            // Serialize
            let mut map = HashMap::new();
            for i in 0..1000u32 {
                map.insert(format!("item_{i}"), i);
            }

            let mut buf = vec![];
            map.serialize(&options, &mut buf).unwrap();

            let cursor = Cursor::new(&buf[..]).unwrap();
            // Test iteration
            let mut reconstructed_map = HashMap::new();
            for (k, v) in cursor.iter_map().unwrap() {
                reconstructed_map.insert(k.to_string(), v.get_u32().unwrap());
            }
            assert_eq!(map, reconstructed_map);

            // Test random access
            for (k, v) in map.iter() {
                let value_cursor = cursor.get_value_by_key(&k).unwrap();
                assert_eq!(value_cursor.get_u32().unwrap(), *v);
            }
        }
    }
}
