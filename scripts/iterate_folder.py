# this is an example of iterating over all zst files in a single folder,
# decompressing them and reading the created_utc field to make sure the files
# are intact. It has no output other than the number of lines

import json
import logging.handlers
import os
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


input_folder = sys.argv[1]
input_files = []
total_size = 0
for subdir, _dirs, files in os.walk(input_folder):
    for filename in files:
        input_path = Path(subdir) / filename
        if input_path.suffix == ".zst":
            file_size = input_path.stat().st_size
            total_size += file_size
            input_files.append([input_path, file_size])
msg = f"Processing {len(input_files)} files of {(total_size / (2**30)):.2f} gigabytes"
log.info(msg)

total_lines = 0
total_bytes_processed = 0
for input_file in input_files:
    file_lines = 0
    file_bytes_processed = 0
    created = None
    for line, file_bytes_processed in read_lines_zst(input_file[0]):
        obj = json.loads(line)
        created = datetime.fromtimestamp(int(obj["created_utc"]), tz=timezone.utc)
        file_lines += 1
        if file_lines == 1:
            msg = f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,} : 0% : {(total_bytes_processed / total_size) * 100:.0f}%"
            log.info(msg)
        if file_lines % 100000 == 0:
            msg = f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,} : {(file_bytes_processed / input_file[1]) * 100:.0f}% : {(total_bytes_processed / total_size) * 100:.0f}%"
            log.info(msg)
    total_lines += file_lines
    total_bytes_processed += input_file[1]
    msg = f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {total_lines:,} : 100% : {(total_bytes_processed / total_size) * 100:.0f}%"
    log.info(msg)
msg = f"Total: {total_lines}"
log.info(msg)
