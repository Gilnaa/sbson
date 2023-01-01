#!/usr/bin/env python3
import sys
import json
import sbson_refimpl


def main():
    if len(sys.argv) != 3:
        print('Convert a JSON document to an SBSON document')
        print('Usage: encode.py <input_file> <output_file>')
        sys.exit(1)
    _progname, input_path, output_path = sys.argv
    with open(input_path, 'r') as f:
        data = json.load(f)
    data = sbson_refimpl.encode(data, options=sbson_refimpl.EncodeOptions(phf_threshold=512))
    with open(output_path, 'wb') as f:
        f.write(data)



if __name__ == '__main__':
    main()

