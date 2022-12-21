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
    UINT32 = 0x11
    INT64 = 0x12
    UINT64 = 0x13


def encode(obj) -> bytes:
    if isinstance(obj, dict):
        payload = encode_map(obj)
    elif isinstance(obj, str):
        payload = struct.pack("B", ElementType.STRING) + obj.encode('utf-8') + b'\x00'
    elif isinstance(obj, bytes):
        payload = struct.pack("B", ElementType.BINARY) + obj
    elif isinstance(obj, bool):
        if obj:
            payload = struct.pack("B", ElementType.TRUE)
        else:
            payload = struct.pack("B", ElementType.FALSE)
    elif isinstance(obj, int):
        if obj > 2**63-1:
            payload = struct.pack("<BQ", int(ElementType.UINT64), obj)
        elif obj > 2**32-1:
            payload = struct.pack("<BI", int(ElementType.UINT32), obj)
        elif obj < -(2**32-1):
            payload = struct.pack("<Bi", int(ElementType.INT32), obj)
        else:
            payload = struct.pack("<Bq", int(ElementType.INT64), obj)
    elif obj is None:
        payload = struct.pack("B", ElementType.NONE)
    # Must come after dict&str since they're also iterable.
    elif isinstance(obj, typing.Iterable):
        payload = encode_array(obj)
    else:
        raise TypeError(f"Unexpected type {type(obj)} for value {obj}")

    return payload

def decode(view: memoryview):
    element_type = view[0]

    if element_type == ElementType.FALSE:
        return False
    elif element_type == ElementType.TRUE:
        return True
    elif element_type == ElementType.NONE:
        return None
    elif element_type == ElementType.INT32:
        return struct.unpack_from('<I', view, offset=1)[0]
    elif element_type == ElementType.INT64:
        return struct.unpack_from('<Q', view, offset=1)[0]
    elif element_type == ElementType.STRING:
        return str(view[1:], 'utf-8').rstrip('\x00')
    elif element_type == ElementType.ARRAY:
        return decode_array(view)
    elif element_type == ElementType.MAP:
        return decode_map(view)
    elif element_type == ElementType.BINARY:
        return bytes(view[1:])
    else:
        raise ValueError(f"Unknown element type {element_type}")


def encode_map(obj: typing.Dict[str, typing.Any]) -> bytes:
    assert isinstance(obj, dict)
    for field_name in obj.keys():
        assert isinstance(field_name, str)
        assert '\x00' not in field_name
    
    # TODO: Ensure this sorts lexicographically and not anything smarter
    field_names = sorted(obj.keys())
    field_values = [
        encode(obj[field_name])
        for field_name in field_names
    ]
    field_names: typing.List[bytes] = [
        field_name.encode('utf-8') + b'\x00'
        for field_name in field_names
    ]

    # Size of the element type, count field, and the descriptors
    header_size = 1 + 4 + (8 * len(field_names))
    keys_size = sum(map(len, field_names))
    descriptors = b''
    keys = b''.join(field_names)
    values = b''.join(field_values)
    keys_offset = header_size
    values_offset = header_size + keys_size
    for name, value in zip(field_names, field_values):
        descriptors += struct.pack('<2I', keys_offset, values_offset)
        keys_offset += len(name)
        values_offset += len(value)
    assert keys_offset == header_size + keys_size
    return struct.pack('<BI', int(ElementType.MAP), len(field_names)) + descriptors + keys + values


def decode_map(view: memoryview) -> dict:
    _element_type, item_count, = struct.unpack_from('<BI', view)
    if item_count == 0:
        return {}

    field_descriptors = []
    descriptors = struct.unpack_from(f"<{2*item_count}I", view[5:])
    for idx in range(item_count):
        keys_offset, values_offset = descriptors[idx*2:(idx + 1)*2]
        if idx != item_count - 1:
            next_key_offset = descriptors[(idx + 1) * 2]
        else:
            # Use first value offset
            next_key_offset = descriptors[1]
        name = view[keys_offset:next_key_offset]
        if name[-1] != 0:
            raise ValueError(f"Field {name} is not terminated.")
        name = str(name, 'utf-8').strip('\0')
        field_descriptors.append((values_offset, name))
    field_descriptors.append((len(view), None))
    output = {}
    for (a_offset, a_name), (b_offset, _) in zip(field_descriptors[:-1], field_descriptors[1:]):
        v = view[a_offset:b_offset]
        o = decode(v)
        output[a_name] = o

    # if len(field_descriptors) > 0:
    #     last_field_offset, last_field_name = field_descriptors[-1]
    #     output[last_field_name] = decode(view[last_field_offset:])

    return output
        

def encode_array(itr: typing.Iterable) -> bytes:
    values = [encode(value) for value in itr]
    count = len(values)

    # type(1B), count(4B), offset array
    header_size = 1 + 4 + (4 * count)
    descriptors = b''
    payload = b''.join(values)
    offset = header_size
    for value in values:
        descriptors += struct.pack('<I', offset)
        offset += len(value)
    return struct.pack('<BI', int(ElementType.ARRAY), len(values)) + descriptors + payload


def decode_array(view: memoryview) -> list:
    _element_type, item_count, = struct.unpack_from('<BI', view)
    if item_count == 0:
        return []
    
    item_offsets = struct.unpack_from(f'<{item_count}I', view[5:])
    items = []
    for a_offset, b_offset in zip(item_offsets[:-1], item_offsets[1:]):
        v = view[a_offset:b_offset]
        o = decode(v)
        items.append(o)

    if len(item_offsets) > 0:
        last_item_offset = item_offsets[-1]
        items.append(decode(view[last_item_offset:]))
    
    return items

