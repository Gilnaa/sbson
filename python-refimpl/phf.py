from dataclasses import dataclass
import siphasher
from typing import Any, List, Tuple, Dict

LAMBDA = 5


@dataclass
class Hashes:
    # Hash used to choose bucket
    g: int
    # Hashes used to go from bucket to index
    f1: int
    f2: int

    def __init__(self, key: str, seed: int):
        key = key.encode('utf-8')
        hasher = siphasher.SipHash13_128(0, seed)
        hasher.update(key)
        digest = hasher.intdigest_structured()
        self.f1 = digest.low & 0xFFFFFFFF
        self.f2 = digest.high & 0xFFFFFFFF
        self.g = digest.low >> 32


@dataclass
class Bucket:
    index: int
    keys: List[int]


@dataclass
class HashState:
    seed: int
    # A list of (d1,d2) displacement pairs, one per bucket
    disps: List[Tuple[int, int]]
    # A list mapping the output of the hash function to the original ordered set of keys.
    hash_output_to_original_index: List[int]


@dataclass
class HashMap:
    seed: int
    disps: List[Tuple[int, int]]
    key_values: List[Tuple[str, Any]]


def displace(f1: int, f2: int, d1: int, d2: int) -> int:
    """
    (d2 + f1 * d1 + f2) mod 2**32
    """
    a = (f1 * d1) & 0xFFFFFFFF
    b = (a + d2) & 0xFFFFFFFF
    c = (b + f2) & 0xFFFFFFFF
    return c


def _try_build_generation(bucket: Bucket,
                          hashes: List[Hashes],
                          d1: int,
                          d2: int,
                          table_len: int,
                          try_map: List[int],
                          map: List[int],
                          generation: int) -> List[Tuple[int, int]]:
    values_to_add = []
    for key in bucket.keys:
        h = hashes[key]
        idx = displace(h.f1, h.f2, d1, d2) % table_len
        if map[idx] is not None or try_map[idx] == generation:
            return None
        try_map[idx] = generation
        values_to_add.append((idx, key))
    return values_to_add


def try_build_hash_state(keys: List[str], seed: int):
    table_len = len(keys)
    buckets_len = (table_len + LAMBDA - 1) // LAMBDA
    hashes = [Hashes(key, seed) for key in keys]
    buckets = [Bucket(index=index, keys=[]) for index in range(buckets_len)]

    for i, h in enumerate(hashes):
        buckets[h.g % buckets_len].keys.append(i)
    buckets.sort(key=lambda bucket: len(bucket.keys), reverse=True)

    values_to_add = None
    try_map = [0] * table_len
    map = [None] * table_len
    generation = 0
    disps = [None] * buckets_len

    for bucket in buckets:
        found_something_for_bucket = False
        for d1 in range(min(64*1024 - 1, table_len)):
            values_to_add = None
            for d2 in range(min(64*1024 - 1, table_len)):
                generation += 1

                values_to_add = _try_build_generation(bucket, hashes, d1, d2, table_len, try_map, map, generation)
                if values_to_add is None:
                    continue
                
                disps[bucket.index] = (d1, d2)
                for (idx, key) in values_to_add:
                    map[idx] = key
                found_something_for_bucket = True
                break

            # We found a d1,d2 combination, so don't continue to try more
            if found_something_for_bucket:
                break
        # We've exhausted all d1,d2 combination and found nothing: give up
        if not found_something_for_bucket:
            return None
    
    return HashState(seed=seed, disps=disps, hash_output_to_original_index=map)


def try_build_map(d: dict, seed: int):
    keys = list(d.keys())
    hash_state = try_build_hash_state(keys, seed)
    if hash_state is None:
        return None
    key_values = [
        (keys[orig_idx], d[keys[orig_idx]])
        for orig_idx in hash_state.hash_output_to_original_index
    ]
    return HashMap(hash_state.seed, hash_state.disps, key_values)

def contains_key(hash_state: HashState, keys: List[str], key) -> bool:
    hashes = Hashes(key, hash_state.seed)
    bucket = hash_state.disps[hashes.g % len(hash_state.disps)]
    idx = displace(hashes.f1, hashes.f2, bucket[0], bucket[1]) % len(keys)
    return keys[hash_state.hash_output_to_original_index[idx]] == key


def map_get(hash_map: HashMap, key):
    hashes = Hashes(key, hash_map.seed)
    bucket = hash_map.disps[hashes.g % len(hash_map.disps)]
    idx = displace(hashes.f1, hashes.f2, bucket[0], bucket[1]) % len(hash_map.key_values)
    if hash_map.key_values[idx][0] != key:
        return None
    return hash_map.key_values[idx][1]


def main():
    keys = {f'a{i}': 0 for i in range(100_000)}
    m = None
    for i in range(1000):
        # m = prof.runcall(try_build_map, keys, 0x500 + i)
        m = try_build_map(keys, 0x500 + i)
        if m is not None:
            break
        print("Retrying")
    
    if m is None:
        print("Failed")
        return

    print(f"Seed={m.seed:x}")
    for key in keys:
        kv = map_get(m, key)
        if kv is None:
            print(f"Missing {key}")
    for key in ["florp", "blarg"]:
        print(key, map_get(m, key))
    import IPython; IPython.embed(colors='Linux')

if __name__ == '__main__':
    main()