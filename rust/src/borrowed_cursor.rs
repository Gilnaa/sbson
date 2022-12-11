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

use super::raw_cursor::{get_byte_array_at, RawCursor, ELEMENT_TYPE_SIZE};
use super::{CursorError, ElementTypeCode};
use core::ffi::CStr;

/// A cursor into a SBSON object.
///
/// The cursor points to a single node in the document tree, and allows
/// creating sub-cursors when indexing into maps or arrays.
///
/// Leaves can be read using `parse_*` methods.
#[derive(Debug, Clone)]
pub struct BorrowedCursor<'a> {
    /// A buffer pointing to an SBSON element node, starting with the the type specifier.
    buffer: &'a [u8],

    raw_cursor: RawCursor,
}

impl<'a> BorrowedCursor<'a> {
    /// Create a new SBSON cursor referencing a buffer.
    pub fn new<T: AsRef<[u8]> + ?Sized>(buffer: &'a T) -> Result<Self, CursorError> {
        let buffer = buffer.as_ref();
        let raw_cursor = RawCursor::new(buffer)?;

        Ok(BorrowedCursor { buffer, raw_cursor })
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
    pub fn get_value_by_index(&self, index: usize) -> Result<BorrowedCursor<'a>, CursorError> {
        let (range, raw_cursor) = self.raw_cursor.get_value_by_index(self.buffer, index)?;
        Ok(BorrowedCursor {
            buffer: &self.buffer[range],
            raw_cursor,
        })
    }

    /// Searches a map item by key, and return a cursor for that item.
    pub fn get_value_by_key(&self, key: &str) -> Result<BorrowedCursor<'a>, CursorError> {
        let (_index, cursor) = self.get_value_and_index_by_key(key)?;
        Ok(cursor)
    }

    /// Searches a map item by key, and return the item's index and cursor.
    /// The index can be used with `get_value_by_index`, or saved into a path-vector.
    pub fn get_value_and_index_by_key(
        &self,
        key: &str,
    ) -> Result<(usize, BorrowedCursor<'a>), CursorError> {
        let (index, range, raw_cursor) = self
            .raw_cursor
            .get_value_and_index_by_key(self.buffer, key)?;
        Ok((
            index,
            BorrowedCursor {
                buffer: &self.buffer[range],
                raw_cursor,
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
            self.buffer,
            ELEMENT_TYPE_SIZE,
        )?))
    }

    pub fn parse_i64(&self) -> Result<i64, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Int64)?;

        Ok(i64::from_le_bytes(get_byte_array_at(
            self.buffer,
            ELEMENT_TYPE_SIZE,
        )?))
    }

    /// Returns a pointer to the null-terminated string pointed to by the cursor
    pub fn parse_cstr(&self) -> Result<&'a CStr, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::String)?;

        // NOTE: Can also fail if there's an embedded null character; might want to use
        // `from_bytes_until_nul` when stabilisied.
        // https://github.com/rust-lang/rust/issues/95027
        CStr::from_bytes_with_nul(&self.buffer[ELEMENT_TYPE_SIZE..])
            .map_err(|_| CursorError::UnterminatedString)
    }

    /// Try to parse the string as a UTF-8 string.
    /// SBSON spec requires strings to be valid UTF-8 sans-nul; if you suspect
    /// your document is non-conforming, use `parse_cstr`.
    pub fn parse_str(&self) -> Result<&'a str, CursorError> {
        self.parse_cstr()?
            .to_str()
            .map_err(|_| CursorError::Utf8Error)
    }

    pub fn parse_binary(&self) -> Result<&'a [u8], CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Binary)?;

        Ok(&self.buffer[ELEMENT_TYPE_SIZE..])
    }
}
