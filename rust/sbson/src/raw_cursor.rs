// Copyright (c) 2022 Gilad Naaman

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use super::{CursorError, ElementTypeCode};
use core::ops::Range;

pub const ELEMENT_TYPE_SIZE: usize = 1;
const U32_SIZE_BYTES: usize = core::mem::size_of::<u32>();
const ARRAY_DESCRIPTOR_SIZE: usize = U32_SIZE_BYTES;
const MAP_DESCRIPTOR_SIZE: usize = 2 * U32_SIZE_BYTES;

struct MapDescriptor {
    key_offset: usize,
    key_length: usize,
    value_offset: usize,
}

pub fn get_byte_array_at<const N: usize>(
    buffer: &[u8],
    offset: usize,
) -> Result<[u8; N], CursorError> {
    // Unfortunate double-checking for length.
    // The second check (in try-into) can never be wrong, since `get` already returns a len-4 slice.
    //
    // Maybe we can get a try_split_array_ref in the future:
    // https://github.com/rust-lang/rust/issues/90091
    buffer
        .get(offset..(offset + N))
        .ok_or(CursorError::DocumentTooShort)?
        .try_into()
        .map_err(|_| CursorError::DocumentTooShort)
}

pub fn get_u32_at_offset(buffer: &[u8], offset: usize) -> Result<u32, CursorError> {
    Ok(u32::from_le_bytes(get_byte_array_at(buffer, offset)?))
}

pub fn get_u32_pair_at_offset(buffer: &[u8], offset: usize) -> Result<(u32, u32), CursorError> {
    let qword = u64::from_le_bytes(get_byte_array_at::<8>(buffer, offset)?);
    let a = qword as u32;
    let b = (qword >> 32) as u32;
    Ok((a, b))
}

fn get_map_descriptor(descriptors: &[u8], index: usize) -> Result<MapDescriptor, CursorError> {
    let (key_data, value_offset) =
        get_u32_pair_at_offset(descriptors, MAP_DESCRIPTOR_SIZE * index as usize)?;
    let key_offset = (key_data & 0x00FFFFFF) as usize;
    let key_length = (key_data >> 24) as usize;
    let value_offset = value_offset as usize;
    Ok(MapDescriptor {
        key_offset,
        key_length,
        value_offset,
    })
}

const fn calculate_bucket_count(child_count: u32) -> usize {
    ((child_count + 4) / 5) as usize
}

const fn calculate_chd_descriptors_offset(child_count: u32) -> usize {
    // From Python:
    // ```python
    // _element_type, item_count, _seed, = struct.unpack_from("<BII", view)
    // bucket_count = (item_count + phf.LAMBDA - 1) // phf.LAMBDA
    // descriptor_offset = struct.calcsize(f"<BII{bucket_count}Q")
    // ```
    let bucket_count = calculate_bucket_count(child_count);
    // Element Type
    ELEMENT_TYPE_SIZE +
    // Child Count
    U32_SIZE_BYTES +
    // Seed
    U32_SIZE_BYTES +
    // CHD Displacements
    U32_SIZE_BYTES * 2 * bucket_count
}

/// This cursor contains the functionality needed in order to traverse
/// the document, but does not own, nor borrows the data.
///
/// This is a private implementation detail and should not be exposed to the
/// users of this crate.
#[derive(Debug, Clone)]
pub(crate) struct RawCursor {
    pub element_type: ElementTypeCode,
    pub child_count: u32,
}

pub struct MapIter<'a> {
    index: u32,
    max: u32,
    descriptors: &'a [u8],
    whole_buffer: &'a [u8],
    self_offset: usize,
}

impl RawCursor {
    /// Shorthand for validating that the cursor points to a particular SBSON node.
    pub fn ensure_element_type(&self, expected_type: ElementTypeCode) -> Result<(), CursorError> {
        if self.element_type != expected_type {
            return Err(CursorError::WrongElementType {
                actual: self.element_type,
            });
        }
        Ok(())
    }

    pub fn new<T: AsRef<[u8]> + ?Sized>(buffer: &T) -> Result<Self, CursorError> {
        let buffer = buffer.as_ref();

        let (first, buffer) = buffer.split_first().ok_or(CursorError::DocumentTooShort)?;
        let element_type = ElementTypeCode::try_from(*first)?;

        let child_count = match element_type {
            ElementTypeCode::Map | ElementTypeCode::Array | ElementTypeCode::MapCHD => {
                get_u32_at_offset(buffer, 0)?
            }
            _ => 0,
        };
        // TODO: Make sure we have at least a valid amount of bytes for headers (array/map descriptors, etc.)

        Ok(RawCursor {
            element_type,
            child_count,
        })
    }

    /// Returns a subcursor by indexing into a specific array/map item.
    pub fn get_value_by_index(
        &self,
        buffer: &[u8],
        index: usize,
    ) -> Result<(Range<usize>, RawCursor), CursorError> {
        let (descriptors_offset, descriptor_size, value_offset_within_header) =
            match self.element_type {
                ElementTypeCode::Array => {
                    (ELEMENT_TYPE_SIZE + U32_SIZE_BYTES, ARRAY_DESCRIPTOR_SIZE, 0)
                }
                ElementTypeCode::Map => (
                    ELEMENT_TYPE_SIZE + U32_SIZE_BYTES,
                    MAP_DESCRIPTOR_SIZE,
                    U32_SIZE_BYTES,
                ),
                ElementTypeCode::MapCHD => (
                    calculate_chd_descriptors_offset(self.child_count),
                    MAP_DESCRIPTOR_SIZE,
                    U32_SIZE_BYTES,
                ),
                _ => {
                    return Err(CursorError::WrongElementType {
                        actual: self.element_type,
                    })
                }
            };

        if index >= self.child_count as usize {
            return Err(CursorError::ItemIndexOutOfBounds);
        }

        // Offset I+1 dwords into the array to skip the item-count and irrelevant headers.
        let item_header_start =
            descriptors_offset + descriptor_size * index + value_offset_within_header;
        let item_offset_start = get_u32_at_offset(buffer, item_header_start)? as usize;
        let range = if index == self.child_count as usize - 1 {
            item_offset_start..buffer.len()
        } else {
            let next_item_header_start =
                descriptors_offset + descriptor_size * (index + 1) + value_offset_within_header;
            let next_item_offset_start =
                get_u32_at_offset(buffer, next_item_header_start)? as usize;
            item_offset_start..next_item_offset_start
        };

        let buffer = buffer
            .get(range.clone())
            .ok_or(CursorError::DocumentTooShort)?;
        Ok((range, RawCursor::new(buffer)?))
    }

    fn get_key_buffer_by_index<'a>(
        &self,
        buffer: &'a [u8],
        index: usize,
    ) -> Result<&'a [u8], CursorError> {
        if index >= self.child_count as usize {
            return Err(CursorError::ItemIndexOutOfBounds);
        }

        let descriptors = self.get_map_descriptors(buffer)?;
        let MapDescriptor {
            key_offset,
            key_length,
            ..
        } = get_map_descriptor(descriptors, index)?;

        buffer
            .get(key_offset..key_offset + key_length)
            .ok_or(CursorError::DocumentTooShort)
    }

    pub fn get_key_by_index<'a>(
        &self,
        buffer: &'a [u8],
        index: usize,
    ) -> Result<&'a str, CursorError> {
        self.get_key_buffer_by_index(buffer, index)
            .and_then(|key_buf| std::str::from_utf8(key_buf).map_err(|_| CursorError::Utf8Error))
    }

    fn get_map_descriptors<'a>(&self, buffer: &'a [u8]) -> Result<&'a [u8], CursorError> {
        let descriptor_start = match self.element_type {
            ElementTypeCode::Map => ELEMENT_TYPE_SIZE + U32_SIZE_BYTES,
            ElementTypeCode::MapCHD => calculate_chd_descriptors_offset(self.child_count),
            _ => {
                return Err(CursorError::WrongElementType {
                    actual: self.element_type,
                })
            }
        };

        // Offset I+1 dwords into the array to skip the item-count and irrelevant headers.
        let descriptor_end = descriptor_start + MAP_DESCRIPTOR_SIZE * self.child_count as usize;
        let descriptors = buffer
            .get((descriptor_start)..descriptor_end)
            .ok_or(CursorError::DocumentTooShort)?;
        Ok(descriptors)
    }

    /// Perform a CHD (compress-hash-displace) hashmap lookup in the given SBSON-node buffer.
    /// This is an O(1) operation.
    ///
    /// This implementation of CHD lookup works by deriving three parameters from the hash of the key:
    /// ```notest
    /// g, f1, f2 = hash(key, seed)
    /// ```
    ///
    /// The `seed` is an arbitrary per-hashmap value that is used in the generation process.
    ///
    /// `g` is used to distribute the keys into different buckets.
    ///
    /// Each bucket is serialized as a pair of 32bit values ("displacements"), `d1, d2`.
    /// The displacement values are generated such that `index = displace(f1, f2, d1, d2)` will
    /// be unique for each of the pre-defined keys.
    ///
    /// After we derive an index that points at a key-value pair, we can read the stored key and
    /// make sure that it is indeed the key that was given to the lookup.
    fn get_value_and_index_by_key_chd(
        &self,
        buffer: &[u8],
        key: &str,
    ) -> Result<(usize, Range<usize>, RawCursor), CursorError> {
        let chd_seed_offset = ELEMENT_TYPE_SIZE + U32_SIZE_BYTES;
        let chd_displacement_start = chd_seed_offset + U32_SIZE_BYTES;
        let bucket_count = calculate_bucket_count(self.child_count);

        // Retrieve the seed and displacemente values.
        let seed = get_u32_at_offset(buffer, chd_seed_offset)? as u64;
        let hashes = phf_shared::hash(key, &seed);
        let bucket_index = hashes.g as usize % bucket_count;
        let bucket_offset = chd_displacement_start + (U32_SIZE_BYTES * 2) * bucket_index;
        let (d1, d2) = get_u32_pair_at_offset(buffer, bucket_offset)?;

        // Displace to get an item index.
        let index = phf_shared::displace(hashes.f1, hashes.f2, d1, d2) % self.child_count;
        let index = index as usize;

        // Equate the stored key to the requested key; any non-existent key
        // will also reach *some* index.
        let stored_key = self.get_key_buffer_by_index(buffer, index)?;
        if key.as_bytes() != stored_key {
            Err(CursorError::KeyNotFound)
        } else {
            self.get_value_by_index(buffer, index)
                .map(|(range, cursor)| (index, range, cursor))
        }
    }

    /// Searches a map item by key, and return the item's index and cursor.
    /// The index can be used with `get_value_by_index`, or saved into a path-vector.
    pub fn get_value_and_index_by_key(
        &self,
        buffer: &[u8],
        key: &str,
    ) -> Result<(usize, Range<usize>, RawCursor), CursorError> {
        if self.element_type == ElementTypeCode::MapCHD {
            return self.get_value_and_index_by_key_chd(buffer, key);
        }

        self.ensure_element_type(ElementTypeCode::Map)?;
        let descriptors = self.get_map_descriptors(buffer)?;

        // Eytzinger scheme uses 1-based indicies. We decrease 1 just before indexing
        let key = key.as_bytes();
        let mut k = 1;

        while k <= self.child_count {
            let index = (k - 1) as usize;
            let MapDescriptor {
                key_offset,
                key_length,
                value_offset,
            } = get_map_descriptor(descriptors, index)?;
            let current_key = buffer
                .get(key_offset..key_offset + key_length)
                .ok_or(CursorError::EmbeddedOffsetOutOfBounds)?;

            match key.cmp(current_key) {
                std::cmp::Ordering::Less => k = k * 2,
                std::cmp::Ordering::Greater => k = k * 2 + 1,
                std::cmp::Ordering::Equal => {
                    // We already have the value offset, we just need to get the offset of the next value / buffer end.
                    let mut value_end = buffer.len();
                    if index + 1 < self.child_count as usize {
                        value_end = get_u32_at_offset(
                            descriptors,
                            MAP_DESCRIPTOR_SIZE * (index + 1) + U32_SIZE_BYTES,
                        )
                        .unwrap() as usize;
                    }
                    let value_range = value_offset as usize..value_end;
                    let buffer = buffer
                        .get(value_range.clone())
                        .ok_or(CursorError::DocumentTooShort)?;
                    return RawCursor::new(buffer).map(|cursor| (index, value_range, cursor));
                }
            }
        }

        Err(CursorError::KeyNotFound)
    }

    pub fn iter_map<'a>(
        &self,
        self_range: Range<usize>,
        buffer: &'a [u8],
    ) -> Result<MapIter<'a>, CursorError> {
        Ok(MapIter {
            index: 0,
            max: self.child_count,
            descriptors: self.get_map_descriptors(buffer)?,
            whole_buffer: buffer,
            self_offset: self_range.start,
        })
    }

    pub fn iter_array<'a>(
        &self,
        self_range: Range<usize>,
        buffer: &'a [u8],
    ) -> Result<impl Iterator<Item = Range<usize>> + 'a, CursorError> {
        self.ensure_element_type(ElementTypeCode::Array)?;
        let descriptor_start = ELEMENT_TYPE_SIZE + U32_SIZE_BYTES;
        let descriptor_end = descriptor_start + ARRAY_DESCRIPTOR_SIZE * self.child_count as usize;
        let descriptors = buffer
            .get(descriptor_start..descriptor_end)
            .ok_or(CursorError::DocumentTooShort)?;

        // TODO: Use `array_chunks` when stabilised to save the `try_into().unwrap()`.
        //  - https://github.com/rust-lang/rust/issues/74985
        let start_offsets = descriptors
            .chunks(ARRAY_DESCRIPTOR_SIZE)
            .map(|offset_slice| u32::from_le_bytes(offset_slice.try_into().unwrap()));
        let end_offsets = start_offsets
            .clone()
            .skip(1)
            .chain(Some(self_range.len() as u32));

        let self_offset = self_range.start;
        Ok(start_offsets
            .zip(end_offsets)
            .map(move |(start, end)| self_offset + start as usize..self_offset + end as usize))
    }
}

impl<'a> MapIter<'a> {
    fn get_item_at_index(&self) -> Result<(&'a str, Range<usize>), CursorError> {
        let MapDescriptor {
            key_offset,
            key_length,
            value_offset,
        } = get_map_descriptor(self.descriptors, self.index as usize)?;

        let key = &self.whole_buffer[key_offset..key_offset + key_length];
        let key = core::str::from_utf8(key).map_err(|_| CursorError::Utf8Error)?;

        let next_value_offset = if self.index < self.max - 1 {
            let MapDescriptor { value_offset, .. } =
                get_map_descriptor(self.descriptors, self.index as usize + 1)?;
            value_offset as usize
        } else {
            self.whole_buffer.len()
        };
        Ok((
            key,
            self.self_offset + value_offset..self.self_offset + next_value_offset,
        ))
    }
}

impl<'a> Iterator for MapIter<'a> {
    type Item = Result<(&'a str, Range<usize>), CursorError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.max {
            return None;
        }

        let item = self.get_item_at_index();
        self.index += 1;
        Some(item)
    }
}
