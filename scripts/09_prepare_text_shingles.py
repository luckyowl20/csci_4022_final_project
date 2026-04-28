from __future__ import annotations

import argparse
import hashlib
import json

from pipeline_utils import require, tokenize

config = __import__("00_config")


MAX_HASH = (1 << 64) - 1


def shingles_for(text: str, size: int) -> set[str]:
    words = tokenize(text)
    if len(words) < size:
        return set(words)
    return {" ".join(words[i : i + size]) for i in range(len(words) - size + 1)}


def hash64(seed: int, value: str) -> int:
    digest = hashlib.blake2b(f"{seed}:{value}".encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def minhash_signature(shingles: set[str], permutations: int) -> list[int]:
    if not shingles:
        return [MAX_HASH] * permutations
    return [min(hash64(seed, shingle) for shingle in shingles) for seed in range(permutations)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build word shingles and deterministic MinHash signatures.")
    parser.add_argument("--shingle-size", type=int, default=config.SHINGLE_SIZE)
    parser.add_argument("--permutations", type=int, default=config.MINHASH_PERMUTATIONS)
    args = parser.parse_args()

    pd = require("pandas")
    groups = pd.read_parquet(config.processed_path("experiment_groups.parquet"))
    articles = pd.read_parquet(config.processed_path("articles_clean.parquet"), columns=["page_id", "clean_text"])
    selected = groups.merge(articles, on="page_id", how="inner")

    shingle_rows = []
    signature_rows = []
    for row in selected.itertuples(index=False):
        shingles = shingles_for(row.clean_text, args.shingle_size)
        shingle_json = json.dumps(sorted(shingles), ensure_ascii=False)
        signature = minhash_signature(shingles, args.permutations)
        shingle_rows.append(
            {
                "page_id": row.page_id,
                "group_name": row.group_name,
                "shingles": shingle_json,
                "num_shingles": len(shingles),
            }
        )
        signature_rows.append(
            {
                "page_id": row.page_id,
                "group_name": row.group_name,
                "signature": json.dumps(signature),
            }
        )

    pd.DataFrame(shingle_rows).to_parquet(config.processed_path("shingles.parquet"), index=False)
    pd.DataFrame(signature_rows).to_parquet(config.processed_path("minhash_signatures.parquet"), index=False)
    print(f"Wrote shingles and MinHash signatures for {len(shingle_rows):,} selected pages")


if __name__ == "__main__":
    main()

