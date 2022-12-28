#![allow(unused_variables)]

use std::fmt::Debug;

use crate::{Cursor, CursorError, ElementTypeCode};
use serde::{
    de::{value::StrDeserializer, MapAccess, SeqAccess, Visitor},
    Deserialize,
};

type Result<T> = std::result::Result<T, CursorError>;

pub fn from_bytes<'a, T>(input: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_bytes(input)?;
    let value = T::deserialize(&mut deserializer)?;

    Ok(value)
}

pub struct Deserializer<'de> {
    cursor: Cursor<&'de [u8]>,
}

impl<'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Result<Self> {
        Ok(Self {
            cursor: Cursor::new(input)?,
        })
    }
}

impl std::fmt::Display for CursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <CursorError as Debug>::fmt(&self, f)
    }
}

impl serde::de::StdError for CursorError {}

impl serde::de::Error for CursorError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        todo!()
    }
}

impl<'de, 'a> serde::de::Deserializer<'de> for &'a mut Deserializer<'de> {
    // TODO: Maybe something a bit more serde-specific.
    type Error = CursorError;
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let f = match self.cursor.get_element_type() {
            crate::ElementTypeCode::Double => todo!(),
            crate::ElementTypeCode::String => visitor.visit_str(self.cursor.get_str()?)?,
            crate::ElementTypeCode::Map => todo!(),
            crate::ElementTypeCode::Array => todo!(),
            crate::ElementTypeCode::Binary => visitor.visit_bytes(self.cursor.get_binary()?)?,
            crate::ElementTypeCode::False => visitor.visit_bool(false)?,
            crate::ElementTypeCode::True => visitor.visit_bool(true)?,
            crate::ElementTypeCode::None => visitor.visit_none()?,
            crate::ElementTypeCode::Int32 => visitor.visit_i32(self.cursor.get_i32()?)?,
            crate::ElementTypeCode::UInt32 => todo!(),
            crate::ElementTypeCode::Int64 => visitor.visit_i64(self.cursor.get_i64()?)?,
            crate::ElementTypeCode::UInt64 => todo!(),
            crate::ElementTypeCode::MapCHD => todo!(),
        };
        Ok(f)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(self.cursor.get_bool()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.cursor.get_i32()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.cursor.get_i64()?)
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.cursor.get_storage_str()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.cursor.get_storage_str()?)
    }

    // The `Serializer` implementation on the previous page serialized byte
    // arrays as JSON arrays of bytes. Handle that representation here.
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bytes(self.cursor.get_binary()?)
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let element_type = self.cursor.get_element_type();
        if element_type != ElementTypeCode::Array {
            return Err(CursorError::WrongElementType {
                actual: element_type,
            });
        }
        visitor.visit_seq(ArrayIteator {
            de: &mut self,
            index: 0,
        })
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if len != self.cursor.get_children_count() {
            println!(
                "{} != {} ({:?})",
                len,
                self.cursor.get_children_count(),
                self.cursor.get_element_type()
            );
            //// HAAAA WRONG ERRORR HAHAHAHAHHA (TODO)
            Err(CursorError::ItemIndexOutOfBounds)
        } else {
            self.deserialize_seq(visitor)
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(MapIterator { de: self, index: 0 })
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(MapIterator { de: self, index: 0 })
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

struct ArrayIteator<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    index: usize,
}

impl<'de, 'a> SeqAccess<'de> for ArrayIteator<'a, 'de> {
    type Error = CursorError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if self.index >= self.de.cursor.get_children_count() {
            return Ok(None);
        }
        let index = self.index;
        self.index += 1;
        let cursor = self.de.cursor.get_value_by_index(index)?;
        seed.deserialize(&mut Deserializer { cursor }).map(Some)
    }
}

struct MapIterator<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    index: usize,
}

impl<'de, 'a> MapAccess<'de> for MapIterator<'a, 'de> {
    type Error = CursorError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: serde::de::DeserializeSeed<'de>,
        // K::Value= &'de str
    {
        if self.index >= self.de.cursor.get_children_count() {
            return Ok(None);
        }
        let key = self.de.cursor.get_key_by_index(self.index)?;
        seed.deserialize(StrDeserializer::new(key)).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        if self.index >= self.de.cursor.get_children_count() {
            return Err(CursorError::ItemIndexOutOfBounds);
        }
        let cursor = self.de.cursor.get_value_by_index(self.index)?;
        self.index += 1;
        seed.deserialize(&mut Deserializer { cursor })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_simple() {
        assert_eq!(Ok(false), from_bytes(b"\x08"));
        assert_eq!(Ok(true), from_bytes(b"\x09"));
        assert_eq!(Ok(0i32), from_bytes(b"\x10\x00\x00\x00\x00"));
        assert_eq!(
            Ok(0i64),
            from_bytes(b"\x12\x00\x00\x00\x00\x00\x00\x00\x00")
        );
        assert_eq!(Ok(1i32), from_bytes(b"\x10\x01\x00\x00\x00"));
        assert_eq!(
            Ok(1i64),
            from_bytes(b"\x12\x01\x00\x00\x00\x00\x00\x00\x00")
        );
    }

    #[test]
    fn test_serde_array() {
        let buf = [
            // Opcode
            0x04, // Item count
            0x02, 0x00, 0x00, 0x00, // Descriptors
            0x0d, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, // Item 0 (0u8)
            0x10, 0x00, 0x00, 0x00, 0x00, // Item 1 (16u8)
            0x10, 0x10, 0x00, 0x00, 0x00,
        ];
        assert_eq!(Ok([0i32, 16]), from_bytes::<[i32; 2]>(&buf));
    }

    #[test]
    fn test_serde_struct() {
        let buf = [
            0x03, 0x02, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x17,
            0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x61, 0x00, 0x62, 0x00, 0x12, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        #[derive(Deserialize, PartialEq, Eq, Debug)]
        struct Florp {
            a: i64,
            b: i64,
        }
        assert_eq!(Ok(Florp { a: 0, b: 1 }), from_bytes(&buf));
    }
}
