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

use crate::ArcCursor;

use super::raw_cursor::{get_byte_array_at, RawCursor};
use super::{CursorError, ElementTypeCode};
use core::ffi::CStr;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CachedMapCursor {
    /// A reference to the entire top-level document
    pub buffer: Arc<[u8]>,
    /// A map from the children names to their range inside the buffer.
    pub children: HashMap<String, Range<usize>>,
    pub declared_children_count: usize,
}

impl CachedMapCursor {
    pub fn new(
        buffer: Arc<[u8]>,
        map_buffer: &[u8],
        raw_cursor: RawCursor,
        self_range: Range<usize>,
    ) -> Result<Self, CursorError> {
        let children: HashMap<_, _> = raw_cursor
                    .iter_map(self_range, map_buffer)?
                    .flat_map(|kv| kv.ok())
                    .map(|(key, range)| {
                        (key.to_string(), range)
                    })
                    .collect();
        Ok(CachedMapCursor {
            buffer,
            children,
            declared_children_count: raw_cursor.child_count as usize,
        })
    }

    /// Searches a map item by key, and return a cursor for that item.
    pub fn get_value_by_key(&self, key: &str) -> Result<ArcCursor, CursorError> {
        let range = self.children.get(key).ok_or(CursorError::KeyNotFound)?;
        ArcCursor::new_with_range(self.buffer.clone(), range.clone())
    }
    
}
