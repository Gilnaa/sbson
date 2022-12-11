// Copyright (c) 2022 Gilad Naaman
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::CachedMapCursor;

use super::raw_cursor::{get_byte_array_at, RawCursor};
use super::{CursorError, ElementTypeCode};
use core::ffi::CStr;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArcCursor {
    /// A reference to the entire top-level document
    buffer: Arc<[u8]>,
    /// The range of the current pointed-to element inside the buffer.
    range: Range<usize>,
    raw_cursor: RawCursor,
}

impl ArcCursor {
    #[inline(always)]
    pub fn scoped_buffer(&self) -> &[u8] {
        &(*self.buffer).as_ref()[self.range.clone()]
    }

    pub fn payload_scoped_buffer(&self) -> &[u8] {
        let mut range = self.range.clone();
        // Skip the first element as it is the element type
        range.start += 1;
        &(*self.buffer).as_ref()[range]
    }
}

impl ArcCursor {
    pub fn new<T: Into<Arc<[u8]>>>(buffer: T) -> Result<Self, CursorError> {
        let buffer = buffer.into();
        let raw_cursor = RawCursor::new(&buffer)?;
        let range = 0..buffer.as_ref().len();
        Ok(Self {
            buffer,
            raw_cursor,
            range,
        })
    }

    pub fn new_with_range(buffer: Arc<[u8]>, range: Range<usize>) -> Result<Self, CursorError> {
        let raw_cursor = RawCursor::new(buffer.as_ref().get(range.clone()).ok_or(CursorError::DocumentTooShort)?)?;
        Ok(Self {
            buffer,
            raw_cursor,
            range,
        })
    }

    pub fn get_element_type(&self) -> ElementTypeCode {
        self.raw_cursor.element_type
    }

    /// Determinte the amount of child-elements this cursor has.
    ///
    /// This will always be 0 for non-container element types (i.e. not an array or a map).
    pub fn get_children_count(&self) -> usize {
        self.raw_cursor.child_count as usize
    }

    /// Returns a subcursor by indexing into a specific array/map item.
    pub fn get_value_by_index(&self, index: usize) -> Result<Self, CursorError> {
        let (mut range, raw_cursor) = self
            .raw_cursor
            .get_value_by_index(self.scoped_buffer(), index)?;
        range.start += self.range.start;
        range.end += self.range.start;
        Ok(Self {
            buffer: self.buffer.clone(),
            raw_cursor,
            range,
        })
    }

    /// Searches a map item by key, and return a cursor for that item.
    pub fn get_value_by_key(&self, key: &str) -> Result<Self, CursorError> {
        let (_index, cursor) = self.get_value_and_index_by_key(key)?;
        Ok(cursor)
    }

    /// Searches a map item by key, and return the item's index and cursor.
    /// The index can be used with `get_value_by_index`, or saved into a path-vector.
    pub fn get_value_and_index_by_key(&self, key: &str) -> Result<(usize, Self), CursorError> {
        let (index, mut range, raw_cursor) = self
            .raw_cursor
            .get_value_and_index_by_key(self.scoped_buffer(), key)?;
        range.start += self.range.start;
        range.end += self.range.start;
        Ok((
            index,
            Self {
                buffer: self.buffer.clone(),
                raw_cursor,
                range,
            },
        ))
    }

    pub fn parse_bool(&self) -> Result<bool, CursorError> {
        match self.raw_cursor.element_type {
            ElementTypeCode::True => Ok(true),
            ElementTypeCode::False => Ok(false),
            _ => Err(CursorError::WrongElementType {
                actual: self.raw_cursor.element_type,
            }),
        }
    }

    pub fn parse_none(&self) -> Result<(), CursorError> {
        self.raw_cursor.ensure_element_type(ElementTypeCode::None)?;
        Ok(())
    }

    pub fn parse_i32(&self) -> Result<i32, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Int32)?;

        Ok(i32::from_le_bytes(get_byte_array_at(
            self.payload_scoped_buffer(),
            0,
        )?))
    }

    pub fn parse_i64(&self) -> Result<i64, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Int64)?;

        Ok(i64::from_le_bytes(get_byte_array_at(
            self.payload_scoped_buffer(),
            0,
        )?))
    }

    /// Returns a pointer to the null-terminated string pointed to by the cursor
    pub fn parse_cstr(&self) -> Result<&CStr, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::String)?;

        // NOTE: Can also fail if there's an embedded null character; might want to use
        // `from_bytes_until_nul` when stabilisied.
        // https://github.com/rust-lang/rust/issues/95027
        CStr::from_bytes_with_nul(self.payload_scoped_buffer())
            .map_err(|_| CursorError::UnterminatedString)
    }

    /// Try to parse the string as a UTF-8 string.
    /// SBSON spec requires strings to be valid UTF-8 sans-nul; if you suspect
    /// your document is non-conforming, use `parse_cstr`.
    pub fn parse_str(&self) -> Result<&str, CursorError> {
        self.parse_cstr()?
            .to_str()
            .map_err(|_| CursorError::Utf8Error)
    }

    pub fn parse_binary(&self) -> Result<&[u8], CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Binary)?;

        Ok(self.payload_scoped_buffer())
    }

    pub fn cache_map(&self) -> Result<CachedMapCursor, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Map)?;
        CachedMapCursor::new(self.buffer.clone(), self.scoped_buffer(), self.raw_cursor.clone(), self.range.clone())
    }
}
