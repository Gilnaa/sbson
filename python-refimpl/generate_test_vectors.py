from sbson_refimpl import *

def generate_test_vectors():
    sanity = {
        '3': b"beep boop",
        'BLARG': [1, 2, True, False, None],
        'FLORP': {'X': 0xFF},
        "help me i'm trapped in a format factory help me before they": "..."
    }
    goto = {
        "top": {
            f"item_{i:04}": {
                "something": [100] * 100
            } for i in range(8000)
        }
    }

    vectors = {
        "sanity": sanity,
        "goto": goto,
    }
    for name, vector in vectors.items():
        print(f"Encoding {name}")
        sbson = encode(vector)
        print(f"Decoding {name}")
        assert decode(sbson) == vector
        print(f"Saving {name}.sbson")
        with open(f"../test_vectors/{name}.sbson", "wb") as f:
            f.write(sbson)


if __name__ == '__main__':
    generate_test_vectors()
