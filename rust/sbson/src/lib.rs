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

extern crate core;

mod raw_cursor;

mod cursor;
#[cfg(feature = "pyo3")]
mod pyo3;
pub use cursor::Cursor;
#[cfg(feature = "serde")]
mod serde;
#[cfg(feature = "std")]
pub mod serializer;

#[cfg(feature = "serde")]
pub use crate::serde::from_bytes;

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
    MapCHD = 0x20,
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
            x if x == ElementTypeCode::UInt32 as u8 => ElementTypeCode::UInt32,
            x if x == ElementTypeCode::UInt64 as u8 => ElementTypeCode::UInt64,
            x if x == ElementTypeCode::MapCHD as u8 => ElementTypeCode::MapCHD,
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

    EmbeddedOffsetOutOfBounds,
    ItemIndexOutOfBounds,
    KeyNotFound,
}

pub enum PathSegment<'a> {
    Key(&'a str),
    Index(usize),
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
    const DOC: &[u8] = include_bytes!("../../../test_vectors/sanity.sbson");
    const DOC_PHF: &[u8] = include_bytes!("../../../test_vectors/sanity_phf.sbson");

    fn test_impl_sanity<T: Clone + AsRef<[u8]>>(cursor: Cursor<T>) {
        assert_eq!(cursor.get_children_count(), 4);

        // Should be the same because "3" is the first key, lexicographically.
        let three_by_name = cursor.get_value_by_key("3".into()).unwrap();
        assert_eq!(three_by_name.get_binary(), Ok(&b"beep boop"[..]));

        // Query ".BLARG[0]"
        let blarg_0 = cursor
            .get_value_by_key("BLARG")
            .unwrap()
            .get_value_by_index(0)
            .unwrap();
        assert_eq!(blarg_0.get_i64(), Ok(1));

        // Query ".BLARG[1]", but drop the intermediary cursor
        // to make sure we can in theory always keep one cursor.
        let blarg_1 = {
            let b = cursor.get_value_by_key("BLARG").unwrap();
            b.get_value_by_index(1).unwrap()
        };
        assert_eq!(blarg_1.get_i64(), Ok(2));

        // Query ".FLORP.X"
        let florp_x = cursor
            .get_value_by_key("FLORP")
            .unwrap()
            .get_value_by_key("X")
            .unwrap();
        assert_eq!(florp_x.get_i64(), Ok(0xFF));

        let blarg = cursor.get_value_by_key("BLARG").unwrap();
        assert_eq!(blarg.get_value_by_index(2).unwrap().get_bool(), Ok(true));
        assert_eq!(blarg.get_value_by_index(3).unwrap().get_bool(), Ok(false));
        assert_eq!(blarg.get_value_by_index(4).unwrap().get_none(), Ok(()));

        // Query the last parameter
        assert_eq!(
            cursor
                .get_value_by_key("help me i'm trapped in a format factory help me before they")
                .unwrap()
                .get_str(),
            Ok("...")
        );
    }

    #[test]
    fn test_sanity_borrowed() {
        let cursor = Cursor::new(DOC).unwrap();
        assert_eq!(cursor.get_element_type(), ElementTypeCode::Map);
        test_impl_sanity(cursor);
    }

    #[test]
    fn test_sanity_borrowed_chd() {
        let cursor = Cursor::new(DOC_PHF).unwrap();
        assert_eq!(cursor.get_element_type(), ElementTypeCode::MapCHD);
        test_impl_sanity(cursor);
    }

    #[test]
    fn test_sanity_arc() {
        use std::sync::Arc;
        let cursor: Cursor<Arc<[u8]>> = Cursor::new(DOC.into()).unwrap();
        assert_eq!(cursor.get_element_type(), ElementTypeCode::Map);
        test_impl_sanity(cursor);
    }

    /// Make sure our hand-rolled Python implementation matches that of `phf_shared`. (External crate)
    /// ```python
    /// In [2]: phf.Hashes('florp_blarg', 0xaabbccdd)
    /// Out[2]: Hashes(g=3120106014, f1=1555086281, f2=999888330)
    /// ```
    #[test]
    fn phf_hash_matches() {
        let hashes = phf_shared::hash("florp_blarg", &0xaabbccdd);
        assert_eq!(hashes.g, 3120106014);
        assert_eq!(hashes.f1, 1555086281);
        assert_eq!(hashes.f2, 999888330);
    }
}
