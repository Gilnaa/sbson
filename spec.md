
# Specification Version 1.0

SBSON (seekable BSON) is a binary format inspired by [BSON](https://bsonspec.org/), but is aimed to be seekable without deserialization.
The format itself is not that similar to BSON and is not supposed to be in any way compatible.

Some notable design decisions:
 - Strings are null terminated so they can be used in C as is, without copying.
 - Arrays and Maps have a header block with relative offsets to the members, making it
   easier to index into them without parsing the whole thing.
 - Map key data encoding limits a single key to 255 bytes, and the sum of all keys of a single map to ~2^24+255 (16MiB)

The following grammar specifies version 0.1 of the SBSON standard. The grammar is written using a pseudo-BNF syntax. Valid SBSON Sdata is represented by the document non-terminal.

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
                    followed by a similar array of elements. The descriptors and elements MUST be ordered as an Eytzinger
                    binary tree.
                    The keys are compared lexicographically.
                    The elements MUST be ordered respective to the descriptors.

e_name      ::=     uint8 uint24  For a key, the little-endian `(key_length << 24) | key_offset`

e_descriptor ::=    e_name uint32	The offset of the element's name into the map (Starting with the element_type byte), 
                    followed by the offset of its value.

array ::=           uint32 uint32*N element*N 	SBSON Array. Similar to a map with the name removed. The first uint32
                    is the total size of the array, in bytes. The following uint32 array is the offset of each respective 
					element relative to the array header.

element 	::= 	"\x01" double   64-bit binary floating point
              |     "\x02" cstring  A null-terminated UTF-8 string.
              |     "\x03" map      A collection of key-value in a binary tree, encoded as an Eytzinger tree.
              |     "\x04" array    Array
              |     "\x05" binary   Binary data
              |     "\x08"          Boolean "false"
              |     "\x09"          Boolean "true"
              |     "\x0A"          Null value
              |     "\x10" int32    32-bit integer
              |     "\x12" int64    64-bit integer
              |     "\x20" map_chd  An encoded CHD hashmap.

e_name      ::=     cstring         Key name
cstring     ::=     (byte*) "\x00"  Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*) MUST
                    NOT contain '\x00', hence it is not fully UTF-8.
binary      ::=     uint32 (byte*) 	Binary - The int32 is the number of bytes in the (byte*).
map_chd     ::=   uint32 uint32*2*((N+4)/5) e_descriptor*N e_name*N element*N   An encoded CHD
                    hashmap.
                    The first DWORD is the hash-seed used to generate the hashmap, followed by the displacement
                    values for each bucket, followed by the regular map composition.