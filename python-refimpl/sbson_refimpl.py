#!/usr/bin/env python3
import dataclasses
import itertools
import struct
import typing
from enum import IntEnum
import phf


PHF_THRESHOLD = 10_000

@dataclasses.dataclass(frozen=True)
class EncodeOptions:
    # Determine how many elements a dict should have to trigger CHD generation,
    # instead of the default binary tree.
    phf_threshold: int = PHF_THRESHOLD


DEFAULT_ENCODE_OPTIONS = EncodeOptions()


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
    MAP_PHF_CHD = 0x20


def encode(obj, options: EncodeOptions = DEFAULT_ENCODE_OPTIONS) -> bytes:
    if isinstance(obj, dict):
        payload = encode_map(obj, options=options)
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
        payload = encode_array(obj, options=options)
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
    elif element_type == ElementType.MAP_PHF_CHD:
        return decode_map_chd(view)
    elif element_type == ElementType.BINARY:
        return bytes(view[1:])
    else:
        raise ValueError(f"Unknown element type {element_type}")


def _encode_map_chd(obj: typing.Dict[str, typing.Any], options: EncodeOptions) -> bytes:
    map = phf.try_build_map(obj, 0x500)
    field_values = [
        encode(value, options=options)
        for _field_name, value in map.key_values
    ]
    field_names: typing.List[bytes] = [
        field_name.encode('utf-8') + b'\x00'
        for field_name, _value in map.key_values
    ]
    keys = b''.join(field_names)
    values = b''.join(field_values)

    assert map is not None
    # Headers. Size is always `1 + 4 + 4 + 8 * bucket_count`
    # Bucket count = `(len(map) + 4) / 5`
    header = struct.pack("<BII", int(ElementType.MAP_PHF_CHD), len(obj), map.seed) + \
             struct.pack(f"<{len(map.disps) * 2}I", *itertools.chain(*map.disps))
    
    # Descriptors
    descriptors = b''
    descriptors_len = len(obj) * 8

    keys_offset = len(header) + descriptors_len
    values_offset = keys_offset + len(keys)
    for name, value in zip(field_names, field_values):
        key_data = ((len(name) - 1) << 24) | keys_offset
        descriptors += struct.pack('<2I', key_data, values_offset)
        keys_offset += len(name)
        values_offset += len(value)
    
    return header + descriptors + keys + values


def _sort_eytzinger(kvs: typing.List[str]):
    new_arr = [None] * len(kvs)
    # TODO: Ensure this sorts lexicographically and not anything smarter
    kvs.sort()
    i = 0

    def _inner(k=1):
        nonlocal i
        if k <= len(kvs):
            _inner(2 * k)
            new_arr[k - 1] = kvs[i]
            i += 1
            _inner(2 * k + 1)

    _inner()
    return new_arr


def encode_map(obj: typing.Dict[str, typing.Any], options: EncodeOptions) -> bytes:
    assert isinstance(obj, dict)
    for field_name in obj.keys():
        assert isinstance(field_name, str)
        assert '\x00' not in field_name
    
    if len(obj) >= options.phf_threshold:
        print(f"Encoding a PHF with {len(obj)} items (>= {options.phf_threshold})")
        return _encode_map_chd(obj, options=options)

    element_type = ElementType.MAP
    field_names = _sort_eytzinger(list(obj.keys()))
    field_values = [
        encode(obj[field_name], options=options)
        for field_name in field_names
    ]
    field_names: typing.List[bytes] = [
        field_name.encode('utf-8') + b'\x00'
        for field_name in field_names
    ]
    keys = b''.join(field_names)
    values = b''.join(field_values)

    # Size of the element type, count field, and the descriptors
    header_size = 1 + 4 + (8 * len(field_names))
    descriptors = b''
    keys_offset = header_size
    values_offset = header_size + len(keys)
    for name, value in zip(field_names, field_values):
        # Subtract 1 to account for the null-terminator we added earlier
        key_data = keys_offset | ((len(name) - 1) << 24)
        descriptors += struct.pack('<2I', key_data, values_offset)
        keys_offset += len(name)
        values_offset += len(value)
    assert keys_offset == header_size + len(keys)
    return struct.pack('<BI', int(element_type), len(field_names)) + descriptors + keys + values


def decode_map_chd(view: memoryview) -> dict:
    """
    Parses a CHD into a dict, discarding any CHD metadata.
    """
    _element_type, item_count, _seed, = struct.unpack_from("<BII", view)
    bucket_count = (item_count + phf.LAMBDA - 1) // phf.LAMBDA
    descriptor_offset = struct.calcsize(f"<BII{bucket_count}Q")

    field_descriptors = []
    descriptors = struct.unpack_from(f"<{2*item_count}I", view, descriptor_offset)
    for idx in range(item_count):
        keys_data, values_offset = descriptors[idx*2:(idx + 1)*2]
        key_offset = keys_data & 0x00FFFFFF
        key_length = keys_data >> 24
        name = view[key_offset:key_offset + key_length + 1]
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
    return output


def decode_map(view: memoryview) -> dict:
    _element_type, item_count, = struct.unpack_from('<BI', view)
    if item_count == 0:
        return {}

    field_descriptors = []
    descriptors = struct.unpack_from(f"<{2*item_count}I", view[5:])
    for idx in range(item_count):
        keys_data, values_offset = descriptors[idx*2:(idx + 1)*2]
        key_offset = keys_data & 0x00FFFFFF
        key_length = keys_data >> 24
        name = view[key_offset:key_offset + key_length + 1]
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

    return output
        

def encode_array(itr: typing.Iterable, options: EncodeOptions) -> bytes:
    values = [encode(value, options=options) for value in itr]
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

