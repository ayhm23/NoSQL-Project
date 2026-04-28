#!/usr/bin/env python3
"""
mapreduce/q2_reducer.py
=======================
Q2 Reducer — Top 20 Requested Resources

Input  (stdin) : sorted lines of {resource_path}\t{bytes}|{host}
Output (stdout): {resource_path}\t{request_count}\t{total_bytes}\t{distinct_hosts}

Emits ALL resources with aggregated stats.
The pipeline (mapreduce_pipeline.py) takes the top 20 by request_count
after reading the reducer output — this is standard practice when a global
top-N cannot be determined within a single reducer.
"""

import sys


def emit(resource_path: str, request_count: int, total_bytes: int, distinct_hosts: set):
    print(f"{resource_path}\t{request_count}\t{total_bytes}\t{len(distinct_hosts)}")


def main():
    current_key    = None
    request_count  = 0
    total_bytes    = 0
    distinct_hosts = set()

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        key, value = parts

        value_parts = value.split('|', 1)
        if len(value_parts) != 2:
            continue

        try:
            bytes_val = int(value_parts[0])
        except ValueError:
            continue
        host = value_parts[1]

        if key != current_key:
            if current_key is not None:
                emit(current_key, request_count, total_bytes, distinct_hosts)
            current_key    = key
            request_count  = 0
            total_bytes    = 0
            distinct_hosts = set()

        request_count  += 1
        total_bytes    += bytes_val
        distinct_hosts.add(host)

    # Flush the final group
    if current_key is not None:
        emit(current_key, request_count, total_bytes, distinct_hosts)


if __name__ == "__main__":
    main()