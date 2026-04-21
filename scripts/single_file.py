# this is an example of loading and iterating over a single file

import json
import logging.handlers
import sys
from datetime import datetime, timezone
from pathlib import Path

import zstandard

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    """Reads a chunk of data from the reader and attempts to decode it. If a UnicodeDecodeError occurs, it will read another chunk and try again until it can decode the data or it has read more than the max_window_size."""
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError as e:
        if bytes_read > max_window_size:
            msg = f"Unable to decode frame after reading {bytes_read:,} bytes"
            raise UnicodeError(msg) from e
        msg = f"Decoding error with {bytes_read:,} bytes, reading another chunk"
        log.info(msg)
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
    """Reads lines from a zst compressed ndjson file."""
    with Path(file_name).open("rb") as file_handle:
        buffer = ""
        reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
        while True:
            chunk = read_and_decode(reader, 2**27, (2**29) * 2)

            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line, file_handle.tell()

            buffer = lines[-1]

        reader.close()


if __name__ == "__main__":
    file_path = sys.argv[1]
    file_size = Path(file_path).stat().st_size
    file_lines = 0
    file_bytes_processed = 0
    created = None
    field = "subreddit"
    value = "wallstreetbets"
    bad_lines = 0
    for line, file_bytes_processed in read_lines_zst(file_path):
        try:
            obj = json.loads(line)
            created = datetime.fromtimestamp(int(obj["created_utc"]), tz=timezone.utc)
            temp = obj[field] == value
        except (KeyError, json.JSONDecodeError):
            bad_lines += 1
        file_lines += 1
        if file_lines % 100000 == 0:
            msg = f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%"
            log.info(msg)

    msg = f"Complete : {file_lines:,} : {bad_lines:,}"
    log.info(msg)

