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

use crate::{ArcCursor, CursorError, BorrowedCursor};
use std::collections::HashMap;
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct CachedMapCursor {
    pub cursor: ArcCursor,

    /// A map from the children names to their range inside the buffer.
    pub children: HashMap<String, Range<usize>>,
}

impl CachedMapCursor {
    pub fn new(
        cursor: ArcCursor,
    ) -> Result<Self, CursorError> {
        let children: HashMap<_, _> = cursor.raw_cursor
                    .iter_map(cursor.range.clone(), cursor.scoped_buffer())?
                    .flat_map(|kv| kv.ok())
                    .map(|(key, range)| {
                        (key.to_string(), range)
                    })
                    .collect();
        Ok(CachedMapCursor {
            cursor,
            children,
        })
    }

    /// Searches a map item by key, and return a cursor for that item.
    pub fn get_value_by_key(&self, key: &str) -> Result<ArcCursor, CursorError> {
        let range = self.children.get(key).ok_or(CursorError::KeyNotFound)?;
        ArcCursor::new_with_range(self.cursor.buffer.clone(), range.clone())
    }

    pub fn get_value_by_index(&self, index: usize) -> Result<ArcCursor, CursorError> {
        // Fallback to using the underlying cursor since the hashmap doesn't know anything
        // about indicies.
        self.cursor.get_value_by_index(index)
    }

    pub fn iter_borrowed<'a>(&'a self) -> Result<impl Iterator<Item = (String, BorrowedCursor<'a>)>, CursorError> {
        self.cursor.iter_borrowed()
    }
}
