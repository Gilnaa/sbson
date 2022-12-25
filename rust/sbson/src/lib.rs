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

mod borrowed_cursor;
mod raw_cursor;

#[cfg(feature = "std")]
mod arc_cursor;
#[cfg(feature = "std")]
mod cached_map_cursor;
#[cfg(feature = "pyo3")]
mod pyo3;

#[cfg(feature = "serde")]
mod serde;

#[cfg(feature = "serde")]
pub use crate::serde::from_bytes;
#[cfg(feature = "std")]
pub use arc_cursor::ArcCursor;
pub use borrowed_cursor::BorrowedCursor;
#[cfg(feature = "std")]
pub use cached_map_cursor::CachedMapCursor;

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

    #[test]
    fn it_works() {
        let cur = BorrowedCursor::new(DOC).unwrap();
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

    #[test]
    fn it_works_phf() {
        let cur = BorrowedCursor::new(DOC_PHF).unwrap();
        assert_eq!(cur.get_element_type(), ElementTypeCode::MapCHD);
        assert_eq!(cur.get_children_count(), 4);

        // Should be the same because "3" is the first key, lexicographically.
        let three_by_name = cur.get_value_by_key("3".into()).unwrap();
        // let three_by_index = cur.get_value_by_index(0).unwrap();
        assert_eq!(three_by_name.parse_binary(), Ok(&b"beep boop"[..]));
        // assert_eq!(three_by_index.parse_binary(), Ok(&b"beep boop"[..]));

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
        // assert_eq!(cur.get_value_by_index(3).unwrap().parse_str(), Ok("..."));
    }

    #[test]
    fn it_works_arc() {
        let cur = ArcCursor::new(DOC).unwrap();
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
