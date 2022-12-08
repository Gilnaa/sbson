
# Specification Version 1.0

SBSON (seekable BSON) is a binary format inspired by [BSON](https://bsonspec.org/), but is aimed to be seekable without deserialization.
The format itself is not that similar to BSON and is not supposed to be in any way compatible.

Some notable design decisions:
 - Strings are null terminated so they can be used in C as is, without copying.
 - Arrays and documents have a header block with relative offsets to the members, making it
   easier to index into them without parsing the whole thing.
 - Key-value pairs are sorted to ease binary-searching a particular key in large maps.

The following grammar specifies version 1.0 of the SBSON standard. The grammar is written using a pseudo-BNF syntax. Valid SBSON Sdata is represented by the document non-terminal.

## Basic Types

The following basic types are used as terminals in the rest of the grammar. Each type must be serialized in little-endian format.
```
byte 	1 byte (8-bits)
int32 	4 bytes (32-bit signed integer, two's complement)
uint32 	4 bytes (32-bit unsigned integer)
int64 	8 bytes (64-bit signed integer, two's complement)
uint64 	8 bytes (64-bit unsigned integer)
double 	8 bytes (64-bit IEEE 754-2008 binary floating point)
```

## Non-terminals

The following specifies the rest of the SBSON grammar. Note that quoted strings represent terminals, and should be interpreted with C semantics (e.g. "\x01" represents the byte 0000 0001). Also note that we use the * operator as shorthand for repetition (e.g. ("\x01"*2) is "\x01\x01"). When used as a unary operator, * means that the repetition can occur 0 or more times.

```
document    ::= 	element                             SBSON Document containing a top-level element.

map         ::= 	e_descriptor*N e_name*N element*N	For some value of N, a consecutive array of N descriptors, 
                    followed by a similar array of elements. The descriptors and elements MUST be sorted
					lexicographically by the name of the element. The elements MUST be ordered respective to the 
					descriptors.

e_descriptor ::=    uint32 uint32	The offset of the element's name into the map (Starting with the element_type byte), 
                    followed by the offset of its value.

array ::=           uint32 uint32*N element*N 	SBSON Array. Similar to a map with the name removed. The first uint32
                    is the total size of the array, in bytes. The following uint32 array is the offset of each respective 
					element relative to the array header.

element 	::= 	"\x01" double   64-bit binary floating point
              |     "\x02" cstring  A null-terminated UTF-8 string.
              |     "\x03" map      A key-value map
              |     "\x04" array    Array
              |     "\x05" binary   Binary data
              |     "\x08"          Boolean "false"
              |     "\x09"          Boolean "true"
              |     "\x0A"          Null value
              |     "\x10" int32    32-bit integer
              |     "\x12" int64    64-bit integer

e_name      ::=     cstring         Key name
cstring     ::=     (byte*) "\x00"  Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*) MUST
                    NOT contain '\x00', hence it is not fully UTF-8.
binary      ::=     uint32 (byte*) 	Binary - The int32 is the number of bytes in the (byte*).
