from __future__ import annotations

import argparse
import bz2
import re
import xml.etree.ElementTree as ET

from pipeline_utils import require, tokenize

config = __import__("00_config")


COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
REF_RE = re.compile(r"<ref[^>/]*>.*?</ref>|<ref[^/]*/>", re.DOTALL | re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
FILE_LINK_RE = re.compile(
    r"\[\[(?:File|Image|Category):[^\]]*\]\]",
    re.IGNORECASE,
)
LINK_RE = re.compile(r"\[\[([^\]|#]+)(?:#[^\]|]*)?(?:\|([^\]]+))?\]\]")
BRACKET_LINK_RE = re.compile(r"\[https?://[^\s\]]+\s*([^\]]*)\]")
TABLE_RE = re.compile(r"\{\|.*?\|\}", re.DOTALL)
HTML_ENTITY_RE = re.compile(r"&(?:nbsp|amp|lt|gt|quot|apos);", re.IGNORECASE)
HEADING_RE = re.compile(r"={2,}\s*(.*?)\s*={2,}")
LIST_PREFIX_RE = re.compile(r"(?m)^[*#;:]+")
MARKUP_RE = re.compile(r"'{2,}|__\w+__|<nowiki>|</nowiki>", re.IGNORECASE)


def strip_balanced(text: str, start: str, end: str) -> str:
    output = []
    depth = 0
    i = 0
    while i < len(text):
        if text.startswith(start, i):
            depth += 1
            i += len(start)
            continue
        if depth and text.startswith(end, i):
            depth -= 1
            i += len(end)
            continue
        if depth == 0:
            output.append(text[i])
        i += 1
    return "".join(output)


def replace_internal_link(match: re.Match) -> str:
    target, label = match.groups()
    if ":" in target:
        prefix = target.split(":", 1)[0].lower()
        if prefix not in {"w", "wikipedia"}:
            return " "
    return label or target


def clean_wikitext(text: str) -> str:
    text = COMMENT_RE.sub(" ", text)
    text = REF_RE.sub(" ", text)
    text = TABLE_RE.sub(" ", text)
    text = strip_balanced(text, "{{", "}}")
    text = strip_balanced(text, "{|", "|}")
    text = FILE_LINK_RE.sub(" ", text)
    text = LINK_RE.sub(replace_internal_link, text)
    text = BRACKET_LINK_RE.sub(r"\1", text)
    text = HEADING_RE.sub(r"\1", text)
    text = LIST_PREFIX_RE.sub(" ", text)
    text = TAG_RE.sub(" ", text)
    text = HTML_ENTITY_RE.sub(" ", text)
    text = MARKUP_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def page_records(xml_path, limit: int | None):
    namespace = ""
    count = 0
    with bz2.open(xml_path, "rb") as handle:
        context = ET.iterparse(handle, events=("start", "end"))
        for event, elem in context:
            if event == "start" and elem.tag.endswith("mediawiki"):
                namespace = elem.tag.split("}")[0].strip("{") if elem.tag.startswith("{") else ""
            if event != "end" or not elem.tag.endswith("page"):
                continue
            prefix = f"{{{namespace}}}" if namespace else ""
            page_id = elem.findtext(f"{prefix}id")
            title = elem.findtext(f"{prefix}title")
            ns = elem.findtext(f"{prefix}ns")
            redirect = elem.find(f"{prefix}redirect") is not None
            revision = elem.find(f"{prefix}revision")
            text = revision.findtext(f"{prefix}text") if revision is not None else ""
            if page_id and title and ns == "0" and not redirect and text:
                clean_text = clean_wikitext(text)
                words = tokenize(clean_text)
                yield {
                    "page_id": int(page_id),
                    "title": title.replace(" ", "_"),
                    "raw_wikitext": text,
                    "clean_text": clean_text,
                    "word_count": len(words),
                }
                count += 1
                if limit and count >= limit:
                    break
            elem.clear()


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract article text from pages-articles XML dump.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=10_000)
    args = parser.parse_args()

    pd = require("pandas")
    pa = require("pyarrow")
    pq = require("pyarrow.parquet")

    xml_path = config.raw_path("enwiki-latest-pages-articles-multistream.xml.bz2")
    output = config.processed_path("articles_raw.parquet")
    writer = None
    total = 0
    batch = []
    try:
        for record in page_records(xml_path, args.limit):
            batch.append(record)
            if len(batch) >= args.chunk_size:
                frame = pd.DataFrame.from_records(batch)
                table = pa.Table.from_pandas(frame, preserve_index=False)
                writer = writer or pq.ParquetWriter(output, table.schema)
                writer.write_table(table)
                total += len(batch)
                print(f"articles: wrote {total:,} rows")
                batch.clear()
        if batch:
            frame = pd.DataFrame.from_records(batch)
            table = pa.Table.from_pandas(frame, preserve_index=False)
            writer = writer or pq.ParquetWriter(output, table.schema)
            writer.write_table(table)
            total += len(batch)
    finally:
        if writer is not None:
            writer.close()
    print(f"Wrote {total:,} extracted articles to {output}")


if __name__ == "__main__":
    main()
