#!/usr/bin/env python3
import json
import struct
import typing
from enum import IntEnum


class ElementType(IntEnum):
    DOUBLE = 0x01
    STRING = 0x02
    MAP = 0x03
    ARRAY = 0x04
    BINARY = 0x05
    FALSE = 0x08
    TRUE = 0x09
    NONE = 0x0A
    INT32 = 0x10
    INT64 = 0x12


def encode(obj) -> bytes:
    if isinstance(obj, dict):
        obj_type, payload = ElementType.MAP, encode_map(obj)
    elif isinstance(obj, str):
        obj_type, payload = ElementType.STRING, obj.encode('utf-8') + b'\x00'
    # Must come after dict&str since they're also iterable.
    elif isinstance(obj, typing.Iterable):
        obj_type, payload = ElementType.ARRAY, encode_array(obj)
    elif isinstance(obj, int):
        obj_type, payload = ElementType.INT64, struct.pack("<Q", obj)
    elif isinstance(obj, bool):
        if obj:
            obj_type, payload = ElementType.TRUE, b''
        else:
            obj_type, payload = ElementType.FALSE, b''
    elif obj is None:
        obj_type, payload = ElementType.NONE, b''
    else:
        raise TypeError(f"Unexpected type {type(obj)} for value {obj}")
    
    return struct.pack('B', int(obj_type)) + payload

def decode(view: memoryview):
    element_type = view[0]

    if element_type == ElementType.FALSE:
        return False
    elif element_type == ElementType.TRUE:
        return True
    elif element_type == ElementType.NONE:
        return None
    elif element_type == ElementType.INT32:
        return struct.unpack('<I', view[1:5])[0]
    elif element_type == ElementType.INT64:
        return struct.unpack('<Q', view[1:9])[0]
    elif element_type == ElementType.STRING:
        return str(view[1:], 'utf-8').rstrip('\x00')
    elif element_type == ElementType.ARRAY:
        return decode_array(view[1:])
    elif element_type == ElementType.MAP:
        return decode_map(view[1:])
    else:
        raise ValueError(f"Unknown element type {element_type}")


def encode_map(obj: typing.Dict[str, typing.Any]) -> bytes:
    assert isinstance(obj, dict)
    for field_name in obj.keys():
        assert isinstance(field_name, str)
        assert '\x00' not in field_name
    
    # TODO: Ensure this sorts lexicographically and anything smarter
    field_names = sorted(obj.keys())
    field_values = [
        encode(obj[field_name])
        for field_name in field_names
    ]
    field_names: typing.List[bytes] = [
        field_name.encode('utf-8') + b'\x00'
        for field_name in field_names
    ]

    # Size of the count field + the descriptors
    header_size = 4 + (8 * len(field_names))
    keys_size = sum(map(len, field_names))
    descriptors = b''
    keys = b''
    values = b''
    keys_offset = header_size
    values_offset = header_size + keys_size
    for name, value in zip(field_names, field_values):
        descriptors += struct.pack('<2I', keys_offset, values_offset)
        keys += name
        keys_offset += len(name)
        values += value
        values_offset += len(value)
    assert keys_offset == header_size + keys_size
    return struct.pack('<I', len(field_names)) + descriptors + keys + values


def decode_map(view: memoryview) -> dict:
    item_count, = struct.unpack_from('<I', view)
    if item_count == 0:
        return {}

    field_descriptors = []
    descriptor_view = view[4:]
    for _ in range(item_count):
        keys_offset, values_offset = struct.unpack_from("<2I", descriptor_view)
        descriptor_view = descriptor_view[8:]

        # Find null-terminator, why doesn't memoryview have `index`?
        for i, b, in enumerate(view[keys_offset:]):
            if b == 0:
                # Skipping the null-terminator from both ends
                name = str(view[keys_offset:keys_offset+i], 'utf-8')
                break
        else:
            raise ValueError("Field name is not terminated.")
        field_descriptors.append((values_offset, name))

    output = {}
    for (a_offset, a_name), (b_offset, _) in zip(field_descriptors[:-1], field_descriptors[1:]):
        output[a_name] = decode(view[a_offset:b_offset])

    if len(field_descriptors) > 0:
        last_field_offset, last_field_name = field_descriptors[-1]
        output[last_field_name] = decode(view[last_field_offset:])

    return output
        

def encode_array(itr: typing.Iterable) -> bytes:
    values = [encode(value) for value in itr]
    count = len(values)

    header_size = 4 + (4 * count)
    descriptors = b''
    payload = b''
    offset = header_size
    for value in values:
        descriptors += struct.pack('<I', offset)
        payload += value
        offset += len(value)
    return struct.pack('<I', len(values)) + descriptors + payload


def decode_array(view: memoryview) -> list:
    item_count, = struct.unpack_from('<I', view)
    if item_count == 0:
        return []
    
    item_offsets = struct.unpack_from(f'<{item_count}I', view[4:])
    items = []
    for a_offset, b_offset in zip(item_offsets[:-1], item_offsets[1:]):
        items.append(decode(view[a_offset:b_offset]))

    if len(item_offsets) > 0:
        last_item_offset = item_offsets[-1]
        items.append(decode(view[last_item_offset:]))
    
    return items


if __name__ == '__main__':
    original = {"3": 4, "BLARG": [1,2,3], 'FLORP': {"1":3}}
    b = encode(original)
    # b = encode({"florp": {'blarg': 3}})
    v = memoryview(b)
    o = decode(v)
    print(b)
    print(o)
    assert o == original