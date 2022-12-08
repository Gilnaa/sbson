extern crate core;

use core::ffi::CStr;
use std::ops::Range;

const ELEMENT_TYPE_SIZE: usize = 1;
const U32_SIZE_BYTES: usize = core::mem::size_of::<u32>();
const ARRAY_DESCRIPTOR_SIZE: usize = U32_SIZE_BYTES;
const MAP_DESCRIPTOR_SIZE: usize = 2 * U32_SIZE_BYTES;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ElementTypeCode {
    Double = 0x01,
    String = 0x02,
    Map = 0x03,
    Array = 0x04,
    Binary = 0x05,
    False = 0x08,
    True = 0x09,
    None = 0x0A,
    Int32 = 0x10,
    UInt32 = 0x11,
    Int64 = 0x12,
    UInt64 = 0x13,
}

impl TryFrom<u8> for ElementTypeCode {
    type Error = CursorError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            x if x == ElementTypeCode::Double as u8 => ElementTypeCode::Double,
            x if x == ElementTypeCode::String as u8 => ElementTypeCode::String,
            x if x == ElementTypeCode::Map as u8 => ElementTypeCode::Map,
            x if x == ElementTypeCode::Array as u8 => ElementTypeCode::Array,
            x if x == ElementTypeCode::Binary as u8 => ElementTypeCode::Binary,
            x if x == ElementTypeCode::False as u8 => ElementTypeCode::False,
            x if x == ElementTypeCode::True as u8 => ElementTypeCode::True,
            x if x == ElementTypeCode::None as u8 => ElementTypeCode::None,
            x if x == ElementTypeCode::Int32 as u8 => ElementTypeCode::Int32,
            x if x == ElementTypeCode::Int64 as u8 => ElementTypeCode::Int64,
            x => return Err(CursorError::InvalidElementType(x)),
        })
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CursorError {
    DocumentTooShort,

    /// The on-the-wire byte representing the element type is unknown.
    InvalidElementType(u8),

    /// The user have asked for one element type, but the cursor points to another.
    WrongElementType {
        actual: ElementTypeCode,
    },

    /// String is missing a null-terminator
    UnterminatedString,

    Utf8Error,

    BufferIndexOutOfBounds,
    ItemIndexOutOfBounds,
    KeyNotFound,
}

/// This cursor contains the functionality needed in order to traverse
/// the document, but does not own, nor borrows the data.
/// 
/// This is a private implementation detail.
#[derive(Debug, Clone)]
struct RawCursor {
    element_type: ElementTypeCode,
    child_count: u32,
}

impl RawCursor {
    /// Shorthand for validating that the cursor points to a particular SBSON node.
    fn ensure_element_type(&self, expected_type: ElementTypeCode) -> Result<(), CursorError> {
        if self.element_type != expected_type {
            return Err(CursorError::WrongElementType {
                actual: self.element_type,
            });
        }
        Ok(())
    }

    fn new<T: AsRef<[u8]> + ?Sized>(buffer: &T) -> Result<Self, CursorError> {
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
    fn get_value_by_index(&self, buffer: &[u8], index: usize) -> Result<(Range<usize>, RawCursor), CursorError> {
        // let (_element_type, buffer) = buffer.split_first().ok_or(CursorError::DocumentTooShort)?;

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
        let item_header_start =
            ELEMENT_TYPE_SIZE + U32_SIZE_BYTES + descriptor_size * index + value_offset_within_header;
        let item_offset_start = get_u32_at_offset(buffer, item_header_start)? as usize;
        let range = if index == self.child_count as usize - 1 {
            item_offset_start..buffer.len()
        } else {
            let next_item_header_start =
                ELEMENT_TYPE_SIZE + U32_SIZE_BYTES + descriptor_size * (index + 1) + value_offset_within_header;
            let next_item_offset_start =
                get_u32_at_offset(buffer, next_item_header_start)? as usize;
            item_offset_start..next_item_offset_start
        };

        let buffer = buffer.get(range.clone()).ok_or(CursorError::DocumentTooShort)?;
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
        // let (_element_type, buffer) = buffer.split_first().ok_or(CursorError::DocumentTooShort)?;

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

            // SAFETY: The slice size was checked in `get`, and is bound by:
            //   - `descriptor_end - descriptor_start` == `MAP_DESCRIPTOR_SIZE * self.child_count`
            let key_offset =
                get_u32_at_offset(descriptors, MAP_DESCRIPTOR_SIZE * mid).unwrap() as usize;

            // Since `from_bytes_until_nul` is unstable, we have to get the exact placement of the null-terminator.
            let null_terminator = buffer
                .iter()
                .skip(key_offset)
                .position(|&x| x == 0)
                .ok_or(CursorError::UnterminatedString)?;

            // SAFETY: `null_terminator` was found after `key_offset` in the buffer, so they're both in range.
            let current_key = &buffer[key_offset..key_offset + null_terminator];
            match current_key.cmp(key.as_bytes()) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => {
                    let value_offset =
                        get_u32_at_offset(descriptors, MAP_DESCRIPTOR_SIZE * mid + U32_SIZE_BYTES)
                            .unwrap() as usize;

                    let range = if mid == self.child_count as usize - 1 {
                        value_offset..buffer.len()
                    } else {
                        let next_value_offset = get_u32_at_offset(
                            descriptors,
                            MAP_DESCRIPTOR_SIZE * (mid + 1) + U32_SIZE_BYTES,
                        )
                        .unwrap() as usize;
                        value_offset..next_value_offset
                    };
                    let buffer = buffer.get(range.clone()).ok_or(CursorError::DocumentTooShort)?;
                    return RawCursor::new(buffer).map(|cursor| (mid, range, cursor));
                }
            }
        }

        Err(CursorError::KeyNotFound)
    }
}

/// A cursor into a SBSON object.
///
/// The cursor points to a single node in the document tree, and allows
/// creating sub-cursors when indexing into maps or arrays.
///
/// Leaves can be read using `parse_*` methods.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
    /// A buffer pointing to an SBSON element node, excluding the type specifier.
    ///
    /// ```txt
    ///    1B           4B
    /// ┌──────┬──────────────────┐
    /// │ \x10 │ \x02\x00\x00\x00 │
    /// └──────┴──────────────────┘
    ///     ▲            ▲
    ///     │            │
    ///     │         buffer
    ///  element_type
    /// ```
    buffer: &'a [u8],

    raw_cursor: RawCursor,
}

impl<'a> Cursor<'a> {
    /// Create a new SBSON cursor referencing a buffer.
    pub fn new<T: AsRef<[u8]> + ?Sized>(buffer: &'a T) -> Result<Self, CursorError> {
        let buffer = buffer.as_ref();
        let raw_cursor = RawCursor::new(buffer)?;

        Ok(Cursor {
            buffer,
            raw_cursor,
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
    pub fn get_value_by_index(&self, index: usize) -> Result<Cursor<'a>, CursorError> {
        let (range, raw_cursor) = self.raw_cursor.get_value_by_index(self.buffer, index)?;
        Ok(Cursor {
            buffer: &self.buffer[range],
            raw_cursor,
        })
    }

    /// Searches a map item by key, and return a cursor for that item.
    pub fn get_value_by_key(&self, key: &str) -> Result<Cursor<'a>, CursorError> {
        let (_index, cursor) = self.get_value_and_index_by_key(key)?;
        Ok(cursor)
    }

    /// Searches a map item by key, and return the item's index and cursor.
    /// The index can be used with `get_value_by_index`, or saved into a path-vector.
    pub fn get_value_and_index_by_key(
        &self,
        key: &str,
    ) -> Result<(usize, Cursor<'a>), CursorError> {
        let (index, range, raw_cursor) = self.raw_cursor.get_value_and_index_by_key(self.buffer, key)?;
        Ok((index, Cursor {
            buffer: &self.buffer[range],
            raw_cursor,
        }))
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
        self.raw_cursor.ensure_element_type(ElementTypeCode::Int32)?;

        Ok(i32::from_le_bytes(get_byte_array_at(self.buffer, 1)?))
    }

    pub fn parse_i64(&self) -> Result<i64, CursorError> {
        self.raw_cursor.ensure_element_type(ElementTypeCode::Int64)?;

        Ok(i64::from_le_bytes(get_byte_array_at(self.buffer, 1)?))
    }

    /// Returns a pointer to the null-terminated string pointed to by the cursor
    pub fn parse_cstr(&self) -> Result<&'a CStr, CursorError> {
        self.raw_cursor.ensure_element_type(ElementTypeCode::String)?;

        // NOTE: Can also fail if there's an embedded null character; might want to use
        // `from_bytes_until_nul` when stabilisied.
        // https://github.com/rust-lang/rust/issues/95027
        CStr::from_bytes_with_nul(&self.buffer[1..]).map_err(|_| CursorError::UnterminatedString)
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
        self.raw_cursor.ensure_element_type(ElementTypeCode::Binary)?;

        Ok(&self.buffer[1..])
    }
}

fn get_byte_array_at<const N: usize>(buffer: &[u8], offset: usize) -> Result<[u8; N], CursorError> {
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

fn get_u32_at_offset(buffer: &[u8], offset: usize) -> Result<u32, CursorError> {
    Ok(u32::from_le_bytes(get_byte_array_at(buffer, offset)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This buffer is the serialized representation of:
    /// ```python
    /// {
    ///     '3': b'beep boop',
    ///     'BLARG': [1, 2, True, False, None],
    ///     'FLORP': {'X': 255},
    ///     "help me i'm trapped in a format factory help me before they": '...'
    /// }
    /// ```
    const DOC: &[u8] = b"\x03\x04\x00\x00\x00%\x00\x00\x00o\x00\x00\x00\'\x00\x00\x00y\x00\x00\x00-\x00\x00\x00\xa7\x00\x00\x003\x00\x00\x00\xbf\x00\x00\x003\x00BLARG\x00FLORP\x00help me i\'m trapped in a format factory help me before they\x00\x05beep boop\x04\x05\x00\x00\x00\x19\x00\x00\x00\"\x00\x00\x00+\x00\x00\x00,\x00\x00\x00-\x00\x00\x00\x12\x01\x00\x00\x00\x00\x00\x00\x00\x12\x02\x00\x00\x00\x00\x00\x00\x00\t\x08\n\x03\x01\x00\x00\x00\r\x00\x00\x00\x0f\x00\x00\x00X\x00\x12\xff\x00\x00\x00\x00\x00\x00\x00\x02...\x00";

    #[test]
    fn it_works() {
        let cur = Cursor::new(DOC).unwrap();
        assert_eq!(cur.get_element_type(), ElementTypeCode::Map);
        assert_eq!(cur.get_children_count(), 4);

        // Should be the same because "3" is the first key, lexicographically.
        let three_by_name = cur.get_value_by_key("3".into()).unwrap();
        let three_by_index = cur.get_value_by_index(0).unwrap();
        assert_eq!(three_by_name.parse_binary(), Ok(&b"beep boop"[..]));
        assert_eq!(three_by_index.parse_binary(), Ok(&b"beep boop"[..]));

        // Query ".BLARG[0]"
        let blarg_0 = cur
            .get_value_by_key("BLARG")
            .unwrap()
            .get_value_by_index(0)
            .unwrap();
        assert_eq!(blarg_0.parse_i64(), Ok(1));

        // Query ".BLARG[1]", but drop the intermediary cursor
        // to make sure we can in theory always keep one cursor.
        let blarg_1 = {
            let b = cur.get_value_by_key("BLARG").unwrap();
            b.get_value_by_index(1).unwrap()
        };
        assert_eq!(blarg_1.parse_i64(), Ok(2));

        // Query ".FLORP.X"
        let florp_x = cur
            .get_value_by_key("FLORP")
            .unwrap()
            .get_value_by_key("X")
            .unwrap();
        assert_eq!(florp_x.parse_i64(), Ok(0xFF));

        let blarg = cur.get_value_by_key("BLARG").unwrap();
        assert_eq!(blarg.get_value_by_index(2).unwrap().parse_bool(), Ok(true));
        assert_eq!(blarg.get_value_by_index(3).unwrap().parse_bool(), Ok(false));
        assert_eq!(blarg.get_value_by_index(4).unwrap().parse_none(), Ok(()));

        // Query the last parameter
        assert_eq!(cur.get_value_by_index(3).unwrap().parse_str(), Ok("..."));
    }
}
