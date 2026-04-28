#!/usr/bin/env python3
"""
mapreduce/q1_reducer.py
=======================
Q1 Reducer — Daily Traffic Summary

Input  (stdin) : sorted lines of {log_date}|{status_code}\t{bytes}
Output (stdout): {log_date}\t{status_code}\t{request_count}\t{total_bytes}

Hadoop guarantees input is sorted by key, so all records for a given
(log_date, status_code) pair arrive consecutively.
"""

import sys


def emit(key: str, request_count: int, total_bytes: int):
    log_date, status_code = key.split("|", 1)
    print(f"{log_date}\t{status_code}\t{request_count}\t{total_bytes}")


def main():
    current_key   = None
    request_count = 0
    total_bytes   = 0

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        key, value = parts

        try:
            bytes_val = int(value)
        except ValueError:
            continue

        if key != current_key:
            if current_key is not None:
                emit(current_key, request_count, total_bytes)
            current_key   = key
            request_count = 0
            total_bytes   = 0

        request_count += 1
        total_bytes   += bytes_val

    # Flush the final group
    if current_key is not None:
        emit(current_key, request_count, total_bytes)


if __name__ == "__main__":
    main()