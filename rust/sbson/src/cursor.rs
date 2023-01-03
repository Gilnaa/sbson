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

use super::raw_cursor::{get_byte_array_at, RawCursor};
use super::{CursorError, ElementTypeCode, PathSegment};
use core::ffi::CStr;
use core::ops::Range;

/// An SBSON cursor over a buffer-type.
///
/// The supplied buffer type can be, for example, any of `&[u8]`, `Arc<[u8]>`, `Rc<[u8]>`, etc.
/// Beware of supplying `Vec<u8>` and the like, as creating sub-cursors may clone the entire buffer.
#[derive(Clone)]
pub struct Cursor<T> {
    /// A reference to the entire top-level document
    pub(crate) buffer: T,
    /// The range of the current pointed-to element inside the buffer.
    pub(crate) range: Range<usize>,
    pub(crate) raw_cursor: RawCursor,
}

impl<T> std::fmt::Debug for Cursor<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursor")
         .field("range", &self.range)
         .field("raw_cursor", &self.raw_cursor)
         .finish()
    }
}

impl<T: Clone + AsRef<[u8]>> Cursor<T> {
    #[inline(always)]
    pub fn scoped_buffer(&self) -> &[u8] {
        &self.buffer.as_ref()[self.range.clone()]
    }

    pub fn payload_scoped_buffer(&self) -> &[u8] {
        let mut range = self.range.clone();
        // Skip the first element as it is the element type
        range.start += 1;
        &self.buffer.as_ref()[range]
    }
}

impl<T: Clone + AsRef<[u8]>> Cursor<T> {
    pub fn new(buffer: T) -> Result<Self, CursorError> {
        let raw_cursor = RawCursor::new(buffer.as_ref())?;
        let range = 0..buffer.as_ref().len();
        Ok(Self {
            buffer,
            raw_cursor,
            range,
        })
    }

    pub fn new_with_range(buffer: T, range: Range<usize>) -> Result<Self, CursorError> {
        let raw_cursor = RawCursor::new(
            buffer
                .as_ref()
                .get(range.clone())
                .ok_or(CursorError::DocumentTooShort)?,
        )?;
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

    pub fn goto<'a>(
        &self,
        path_segments: impl Iterator<Item = PathSegment<'a>>,
    ) -> Result<Self, CursorError> {
        let mut buffer = &self.buffer.as_ref()[self.range.clone()];
        let mut raw_cursor = self.raw_cursor.clone();
        let mut range = self.range.clone();
        for segment in path_segments {
            let (mut sub_range, sub_cursor) = match segment {
                PathSegment::Key(key) => {
                    let (_index, sub_range, sub_cursor) =
                        raw_cursor.get_value_and_index_by_key(buffer, key)?;
                    (sub_range, sub_cursor)
                }
                PathSegment::Index(index) => raw_cursor.get_value_by_index(buffer, index)?,
            };

            buffer = &buffer[sub_range.clone()];
            raw_cursor = sub_cursor;

            sub_range.start += range.start;
            sub_range.end += range.start;
            range = sub_range;
        }
        Ok(Cursor {
            buffer: self.buffer.clone(),
            range,
            raw_cursor,
        })
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

    /// Returns the key of a key-value pair in map nodes by its index.
    /// Note that the exact position of a certain key is implementation defined.
    pub fn get_key_by_index(&self, index: usize) -> Result<&str, CursorError> {
        self.raw_cursor
            .get_key_by_index(self.scoped_buffer(), index)
    }

    pub fn get_bool(&self) -> Result<bool, CursorError> {
        match self.raw_cursor.element_type {
            ElementTypeCode::True => Ok(true),
            ElementTypeCode::False => Ok(false),
            _ => Err(CursorError::WrongElementType {
                actual: self.raw_cursor.element_type,
            }),
        }
    }

    pub fn get_none(&self) -> Result<(), CursorError> {
        self.raw_cursor.ensure_element_type(ElementTypeCode::None)?;
        Ok(())
    }

    pub fn get_i32(&self) -> Result<i32, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Int32)?;

        Ok(i32::from_le_bytes(get_byte_array_at(
            self.payload_scoped_buffer(),
            0,
        )?))
    }

    pub fn get_i64(&self) -> Result<i64, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Int64)?;

        Ok(i64::from_le_bytes(get_byte_array_at(
            self.payload_scoped_buffer(),
            0,
        )?))
    }

    pub fn get_u32(&self) -> Result<u32, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::UInt32)?;

        Ok(u32::from_le_bytes(get_byte_array_at(
            self.payload_scoped_buffer(),
            0,
        )?))
    }

    pub fn get_u64(&self) -> Result<u64, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::UInt64)?;

        Ok(u64::from_le_bytes(get_byte_array_at(
            self.payload_scoped_buffer(),
            0,
        )?))
    }

    pub fn get_double(&self) -> Result<f64, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Double)?;

        Ok(f64::from_le_bytes(get_byte_array_at(
            self.payload_scoped_buffer(),
            0,
        )?))
    }

    /// Returns a reference to the null-terminated string pointed to by the cursor.
    ///
    /// The returned reference is lifetime-bound to the current cursor.
    /// If the cursor is not an owning-cursor, a reference bound to the backing storage
    /// can be receieved by calling `get_storage_cstr`.
    pub fn get_cstr(&self) -> Result<&CStr, CursorError> {
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
    /// your document is non-conforming, use `get_cstr`.
    ///
    /// The returned reference is lifetime-bound to the current cursor.
    /// If the cursor is not an owning-cursor, a reference bound to the backing storage
    /// can be receieved by calling `get_storage_str`.
    pub fn get_str(&self) -> Result<&str, CursorError> {
        self.get_cstr()?
            .to_str()
            .map_err(|_| CursorError::Utf8Error)
    }

    /// Returns a reference to the payload of a binary node.
    ///
    /// The returned reference is lifetime-bound to the current cursor.
    /// If the cursor is not an owning-cursor, a reference bound to the backing storage
    /// can be receieved by calling `get_storage_binary`.
    pub fn get_binary(&self) -> Result<&[u8], CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Binary)?;

        Ok(self.payload_scoped_buffer())
    }

    /// Iterate over the children of this map node.
    /// Malformed children are silently dropped.
    pub fn iter_map<'a>(
        &'a self,
    ) -> Result<impl Iterator<Item = (&'a str, Self)> + 'a, CursorError> {
        Ok(self
            .raw_cursor
            .iter_map(self.range.clone(), self.scoped_buffer())?
            // Ignore
            .flat_map(|kv| kv.ok())
            .flat_map(|(key, range)| {
                Cursor::new_with_range(self.buffer.clone(), range)
                    .ok()
                    .map(|cursor| (key, cursor))
            }))
    }

    /// Iterate over the children of this map node, returning borrowed cursors.
    /// Malformed
    pub fn iter_map_borrowed<'a>(
        &'a self,
    ) -> Result<impl Iterator<Item = (&'a str, Cursor<&'a [u8]>)>, CursorError> {
        Ok(self
            .raw_cursor
            .iter_map(self.range.clone(), self.scoped_buffer())?
            .flat_map(|kv| kv.ok())
            .flat_map(|(key, range)| {
                Cursor::new_with_range(self.buffer.as_ref(), range)
                    .ok()
                    .map(|cursor| (key, cursor))
            }))
    }

    pub fn iter_array(&self) -> Result<impl Iterator<Item = Cursor<&[u8]>>, CursorError> {
        Ok(self
            .raw_cursor
            .iter_array(self.range.clone(), self.scoped_buffer())?
            .flat_map(|range| Cursor::new_with_range(self.buffer.as_ref(), range).ok()))
    }

    /// Returns a new cursor that borrows this one.
    /// This is useful for cases where a lot of cursor-juggling is expected, in case
    /// that the current cursor is reference-counted.    
    pub fn borrow(&self) -> Cursor<&[u8]> {
        Cursor {
            buffer: self.buffer.as_ref(),
            range: self.range.clone(),
            raw_cursor: self.raw_cursor.clone(),
        }
    }
}

impl<'data> Cursor<&'data [u8]> {
    /// Returns a reference to the null-terminated string pointed to by the cursor.
    ///
    /// This reference is lifetime-bound to the backing storage referenced by this cursor,
    /// and may outlive the cursor.
    pub fn get_storage_cstr(&self) -> Result<&'data CStr, CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::String)?;

        let mut range = self.range.clone();
        // Skip the first element as it is the element type
        range.start += 1;
        // NOTE: Can also fail if there's an embedded null character; might want to use
        // `from_bytes_until_nul` when stabilisied.
        // https://github.com/rust-lang/rust/issues/95027
        CStr::from_bytes_with_nul(&self.buffer.as_ref()[range])
            .map_err(|_| CursorError::UnterminatedString)
    }

    /// Try to parse the string as a UTF-8 string.
    /// SBSON spec requires strings to be valid UTF-8 sans-nul; if you suspect
    /// your document is non-conforming, use `get_storage_cstr`.
    ///
    /// This reference is lifetime-bound to the backing storage referenced by this cursor,
    /// and may outlive the cursor.
    pub fn get_storage_str(&self) -> Result<&'data str, CursorError> {
        self.get_storage_cstr()?
            .to_str()
            .map_err(|_| CursorError::Utf8Error)
    }

    /// Try to parse the string as a UTF-8 string.
    /// SBSON spec requires strings to be valid UTF-8 sans-nul; if you suspect
    /// your document is non-conforming, use `get_storage_cstr`.
    ///
    /// This reference is lifetime-bound to the backing storage referenced by this cursor,
    /// and may outlive the cursor.
    pub fn get_storage_binary(&self) -> Result<&'data [u8], CursorError> {
        self.raw_cursor
            .ensure_element_type(ElementTypeCode::Binary)?;

        let mut range = self.range.clone();
        // Skip the first element as it is the element type
        range.start += 1;
        Ok(&self.buffer.as_ref()[range])
    }
}
