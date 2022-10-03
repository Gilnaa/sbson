extern crate core;

use core::ffi::CStr;

const U32_SIZE_BYTES: usize = core::mem::size_of::<u32>();

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
    Int64 = 0x12,
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
            _ => return Err(CursorError::InvalidElementType),
        })
    }
}

#[derive(Clone, Debug)]
pub enum CursorError {
    DocumentTooShort,

    /// The on-the-wire byte representing the element type is unknown.
    InvalidElementType,

    /// The user have asked for one element type, but the cursor points to another.
    WrongElementType {
        actual: ElementTypeCode,
    },

    /// String is missing a null-terminator
    UnterminatedString,

    Utf8Error,

    KeyOrIndexInvalid,
}

/// A cursor into a SBSON object.
///
/// The cursor points to a single node in the document tree, and allows
/// creating sub-cursors when indexing into maps or arrays.
///
/// Leaves can be read using `parse_*` methods.
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

    /// The element type of the node pointed to by the cursor.
    /// This is filled on construction, and thus always valid.
    element_type: ElementTypeCode,

    child_count: u32,
}

impl<'a> Cursor<'a> {
    /// Shorthand for validating that the cursor points to a particular SBSON node.
    fn ensure_element_type(&self, expected_type: ElementTypeCode) -> Result<(), CursorError> {
        if self.element_type != expected_type {
            return Err(CursorError::WrongElementType {
                actual: self.element_type,
            });
        }
        Ok(())
    }
}

impl<'a> Cursor<'a> {
    /// Create a new SBSON cursor referencing a buffer.
    pub fn new<T: AsRef<[u8]> + ?Sized>(buffer: &'a T) -> Result<Self, CursorError> {
        let buffer = buffer.as_ref();

        let (first, buffer) = buffer.split_first().ok_or(CursorError::DocumentTooShort)?;
        let element_type = ElementTypeCode::try_from(*first)?;

        let child_count = match element_type {
            ElementTypeCode::Map | ElementTypeCode::Array => {
                get_u32_at_offset(buffer, 0)?
            },
            _ => 0
        };
        // TODO: Make sure we have at least a valid amount of bytes for headers (array/map descriptors, etc.)
        //       Arrays are easy as the descriptor has a constant size, maps are harder. (should we alter spec?)

        Ok(Cursor {
            buffer,
            element_type,
            child_count,
        })
    }

    pub fn get_element_type(&self) -> ElementTypeCode {
        self.element_type
    }

    /// Determinte the amount of child-elements this cursor has.
    /// 
    /// This will always be 0 for non-container element types (i.e. not an array or a map).
    pub fn get_children_count(&self) -> usize {
        self.child_count as usize
    }

    /// Returns a subcursor by indexing into a specific array item.
    pub fn index_into_array(&self, index: usize) -> Result<Cursor<'a>, CursorError> {
        self.ensure_element_type(ElementTypeCode::Array)?;

        if index >= self.child_count as usize {
            return Err(CursorError::KeyOrIndexInvalid);
        }

        // Offset I+1 dwords into the array to skip the item-count and irrelevant headers.
        let item_header_start = U32_SIZE_BYTES + U32_SIZE_BYTES * index;
        let item_offset_start = get_u32_at_offset(self.buffer, item_header_start)? as usize;
        let buffer = if index == self.child_count as usize - 1 {
            self.buffer.get(item_offset_start..)
        } else {
            let next_item_header_start = U32_SIZE_BYTES + U32_SIZE_BYTES * (index + 1);
            let next_item_offset_start = get_u32_at_offset(self.buffer, next_item_header_start)? as usize;
            self.buffer.get(item_offset_start..next_item_offset_start)
        };

        let buffer = buffer.ok_or(CursorError::DocumentTooShort)?;
        Cursor::new(buffer)
    }

    /// Returns a subcursor by indexing into a specific array item.
    pub fn index_into_map(&self, key: &str) -> Result<Cursor<'a>, CursorError> {
        self.ensure_element_type(ElementTypeCode::Array)?;

        unimplemented!()
    }

    pub fn parse_bool(&self) -> Result<bool, CursorError> {
        match self.element_type {
            ElementTypeCode::True => Ok(true),
            ElementTypeCode::False => Ok(false),
            _ => Err(CursorError::WrongElementType {
                actual: self.element_type,
            }),
        }
    }

    pub fn parse_i32(&self) -> Result<i32, CursorError> {
        self.ensure_element_type(ElementTypeCode::Int32)?;

        Ok(i32::from_le_bytes(get_byte_array_at(self.buffer, 0)?))
    }

    pub fn parse_i64(&self) -> Result<i64, CursorError> {
        self.ensure_element_type(ElementTypeCode::Int64)?;

        Ok(i64::from_le_bytes(get_byte_array_at(self.buffer, 0)?))
    }

    /// Returns a pointer to the null-terminated string pointed to by the cursor
    pub fn parse_cstr(&self) -> Result<&'a CStr, CursorError> {
        self.ensure_element_type(ElementTypeCode::String)?;

        // NOTE: Can also fail if there's an embedded null character; might want to use
        // `from_bytes_until_nul` when stabilisied.
        CStr::from_bytes_with_nul(self.buffer).map_err(|_| CursorError::UnterminatedString)
    }

    /// Try to parse the string as a UTF-8 string.
    /// SBSON spec requires strings to be valid UTF-8 sans-nul; if you suspect
    /// your document is non-conforming, use `parse_cstr`.
    pub fn parse_str(&self) -> Result<&'a str, CursorError> {
        self.parse_cstr()?.to_str().map_err(|_| CursorError::Utf8Error)
    }

    pub fn parse_binary(&self) -> Result<&'a [u8], CursorError> {
        self.ensure_element_type(ElementTypeCode::Binary)?;

        Ok(self.buffer)
    }
}


fn get_byte_array_at<const N: usize>(buffer: &[u8], offset: usize) -> Result<[u8; N], CursorError> {
    // Unfortunate double-checking for length.
    // The second check (in try-into) can never be wrong, since `get` already returns a len-4 slice.
    //
    // Maybe we can get a try_split_array_ref in the future:
    // https://github.com/rust-lang/rust/issues/90091
    buffer
        .get(offset..(offset+N))
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

    #[test]
    fn it_works() {
        // This buffer is the serialized representation of:
        //  - {'3': 4, 'BLARG': [1, 2, 3], 'FLORP': {'1': 3}}
        let doc = b"\x03\x03\x00\x00\x00\x1e\x00\x00\x003\x00\'\x00\x00\x00BLARG\x00S\x00\x00\x00FLORP\x00\x12\x04\x00\x00\x00\x00\x00\x00\x00\x04\x03\x00\x00\x00\x10\x00\x00\x00\x19\x00\x00\x00\"\x00\x00\x00\x12\x01\x00\x00\x00\x00\x00\x00\x00\x12\x02\x00\x00\x00\x00\x00\x00\x00\x12\x03\x00\x00\x00\x00\x00\x00\x00\x03\x01\x00\x00\x00\n\x00\x00\x001\x00\x12\x03\x00\x00\x00\x00\x00\x00\x00";
        let f = Cursor::new(doc).unwrap();
        assert_eq!(f.get_element_type(), ElementTypeCode::Map);
        assert_eq!(f.get_children_count(), 3);
    }
}
