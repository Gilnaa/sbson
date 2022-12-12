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

pub fn get_u32_quad_at_offset(
    buffer: &[u8],
    offset: usize,
) -> Result<(u32, u32, u32, u32), CursorError> {
    let qword = u128::from_le_bytes(get_byte_array_at(buffer, offset)?);
    let a = qword as u32;
    let b = (qword >> 32) as u32;
    let c = (qword >> 64) as u32;
    let d = (qword >> 96) as u32;
    Ok((a, b, c, d))
}

/// This cursor contains the functionality needed in order to traverse
/// the document, but does not own, nor borrows the data.
///
/// This is a private implementation detail and should not be exposed to the
/// users of this crate.
#[derive(Debug, Clone)]
pub struct RawCursor {
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
            ElementTypeCode::Map | ElementTypeCode::Array => get_u32_at_offset(buffer, 0)?,
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
        let (descriptor_size, value_offset_within_header) = match self.element_type {
            ElementTypeCode::Array => (ARRAY_DESCRIPTOR_SIZE, 0),
            ElementTypeCode::Map => (MAP_DESCRIPTOR_SIZE, U32_SIZE_BYTES),
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
        let item_header_start = ELEMENT_TYPE_SIZE
            + U32_SIZE_BYTES
            + descriptor_size * index
            + value_offset_within_header;
        let item_offset_start = get_u32_at_offset(buffer, item_header_start)? as usize;
        let range = if index == self.child_count as usize - 1 {
            item_offset_start..buffer.len()
        } else {
            let next_item_header_start = ELEMENT_TYPE_SIZE
                + U32_SIZE_BYTES
                + descriptor_size * (index + 1)
                + value_offset_within_header;
            let next_item_offset_start =
                get_u32_at_offset(buffer, next_item_header_start)? as usize;
            item_offset_start..next_item_offset_start
        };

        let buffer = buffer
            .get(range.clone())
            .ok_or(CursorError::DocumentTooShort)?;
        Ok((range, RawCursor::new(buffer)?))
    }

    /// Searches a map item by key, and return the item's index and cursor.
    /// The index can be used with `get_value_by_index`, or saved into a path-vector.
    pub fn get_value_and_index_by_key(
        &self,
        buffer: &[u8],
        key: &str,
    ) -> Result<(usize, Range<usize>, RawCursor), CursorError> {
        self.ensure_element_type(ElementTypeCode::Map)?;
        let key = key.as_bytes();

        let descriptor_start = ELEMENT_TYPE_SIZE + U32_SIZE_BYTES;
        let descriptor_end = descriptor_start + MAP_DESCRIPTOR_SIZE * self.child_count as usize;

        // This slice contains, for each element, a descriptor that looks like `{key_offset: u32, value_offset: u32}`.
        // We cannot convert it to a `&[u32]` because we do not know that that the data is aligned.
        let descriptors = buffer
            .get(descriptor_start..descriptor_end)
            .ok_or(CursorError::DocumentTooShort)?;

        // Perform a binary search on the key descriptors to see if we can find our items.
        let mut window_size = self.child_count as usize;
        let mut left = 0;
        let mut right = window_size;
        while left < right {
            window_size = right - left;
            let mid = left + window_size / 2;

            let (current_key, value_range) = if mid == self.child_count as usize - 1 {
                // PANIC-SAFETY: Index calculation is bound not user dependent.
                let (key_offset, value_offset) =
                    get_u32_pair_at_offset(descriptors, MAP_DESCRIPTOR_SIZE * mid).unwrap();
                let key_offset = key_offset as usize;

                let key_slice = buffer
                    .get(key_offset..)
                    .ok_or(CursorError::UnterminatedString)?;
                let null_terminator =
                    memchr::memchr(0, key_slice).ok_or(CursorError::UnterminatedString)?;

                (
                    // PANIC-SAFETY: `null_terminator` was found after `key_offset` in the buffer, so they're both in range.
                    &buffer[key_offset..key_offset + null_terminator],
                    value_offset as usize..buffer.len(),
                )
            } else {
                // PANIC-SAFETY: Index calculation is bound not user dependent.
                //               Specifically since this is not the last descriptor, we know to have at least 128bits available.
                let (key_offset, value_offset, next_key_offset, next_value_offset) =
                    get_u32_quad_at_offset(descriptors, MAP_DESCRIPTOR_SIZE * mid).unwrap();
                let key_offset = key_offset as usize;
                let next_key_offset = next_key_offset as usize;
                // We must check the offset here as its payload-provided and unverified.
                let current_key = buffer
                    .get(key_offset..next_key_offset as usize - 1)
                    .ok_or(CursorError::EmbeddedOffsetOutOfBounds)?;

                (
                    current_key,
                    value_offset as usize..next_value_offset as usize,
                )
            };

            match current_key.cmp(key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => {
                    let buffer = buffer
                        .get(value_range.clone())
                        .ok_or(CursorError::DocumentTooShort)?;
                    return RawCursor::new(buffer).map(|cursor| (mid, value_range, cursor));
                }
            }
        }

        Err(CursorError::KeyNotFound)
    }

    pub fn iter_map<'a>(&self, self_range: Range<usize>, buffer: &'a [u8]) -> Result<MapIter<'a>, CursorError> {
        self.ensure_element_type(ElementTypeCode::Map)?;

        let descriptor_start = ELEMENT_TYPE_SIZE + U32_SIZE_BYTES;
        let descriptor_end = descriptor_start + MAP_DESCRIPTOR_SIZE * self.child_count as usize;

        // This slice contains, for each element, a descriptor that looks like `{key_offset: u32, value_offset: u32}`.
        // We cannot convert it to a `&[u32]` because we do not know that that the data is aligned.
        let descriptors = buffer
            .get(descriptor_start..descriptor_end)
            .ok_or(CursorError::DocumentTooShort)?;
        Ok(MapIter {
            index: 0,
            max: self.child_count,
            descriptors: descriptors,
            whole_buffer: buffer,
            self_offset: self_range.start,
        })
    }

    // pub fn iter_array<'a>(&self, self_range: Range<usize>, buffer: &'a [u8]) -> Result<impl Iterator<Item = Range<usize>>, CursorError> {
    //     self.ensure_element_type(ElementTypeCode::Array)?;
    //     let descriptor_start = ELEMENT_TYPE_SIZE + U32_SIZE_BYTES;
    //     let descriptor_end = descriptor_start + ARRAY_DESCRIPTOR_SIZE * self.child_count as usize;
    //     let descriptors = buffer
    //         .get(descriptor_start..descriptor_end)
    //         .ok_or(CursorError::DocumentTooShort)?;

    //     // TODO: Use `array_chunks` when stabilised to save the `try_into().unwrap()`.
    //     let d = descriptors.chunks(ARRAY_DESCRIPTOR_SIZE)
    //         .map(|offset_slice| u32::from_le_bytes(offset_slice.try_into().unwrap()))
    //         .chain(Some(self_range.len() as u32));

    //     Ok((0..self.child_count).map(|idx| {
    //         let offset = get_u32_at_offset(descriptors, idx * ARRAY_DESCRIPTOR_SIZE)
    //     }))
    // }
}

impl<'a> MapIter<'a> {
    fn get_item_at_index(&self) -> Result<(&'a str, Range<usize>), CursorError> {
        let (key_offset, value_offset) =
            get_u32_pair_at_offset(self.descriptors, MAP_DESCRIPTOR_SIZE * self.index as usize)
                .unwrap();
        let key_offset = key_offset as usize;
        let value_offset = value_offset as usize;

        let key_slice = self
            .whole_buffer
            .get(key_offset..)
            .ok_or(CursorError::UnterminatedString)?;
        let null_terminator =
            memchr::memchr(0, key_slice).ok_or(CursorError::UnterminatedString)?;
        let key = &self.whole_buffer[key_offset..key_offset + null_terminator];
        let key = core::str::from_utf8(key).map_err(|_| CursorError::Utf8Error)?;

        let next_value_offset = if self.index < self.max - 1 {
            get_u32_pair_at_offset(
                self.descriptors,
                MAP_DESCRIPTOR_SIZE * (self.index as usize + 1),
            )
            .unwrap()
            .1 as usize
        } else {
            self.whole_buffer.len()
        };
        Ok((key, self.self_offset+value_offset..self.self_offset+next_value_offset))
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
