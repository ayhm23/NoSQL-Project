#!/usr/bin/env python3
"""
mapreduce/q3_reducer.py
=======================
Q3 Reducer — Hourly Error Analysis

Input  (stdin) : sorted lines of {log_date}|{log_hour}\t{status_code}|{host}
Output (stdout): {log_date}\t{log_hour}\t{error_count}\t{total_requests}\t{error_rate}\t{distinct_error_hosts}

For each (log_date, log_hour) bucket:
  - total_requests       = all records in that bucket
  - error_count          = records with status_code in range 400–599
  - error_rate           = error_count / total_requests
  - distinct_error_hosts = unique hosts that generated error requests
"""

import sys


def emit(key: str, error_count: int, total_requests: int, distinct_error_hosts: set):
    log_date, log_hour = key.split("|", 1)
    error_rate = round(error_count / total_requests, 6) if total_requests > 0 else 0.0
    print(f"{log_date}\t{log_hour}\t{error_count}\t{total_requests}\t{error_rate}\t{len(distinct_error_hosts)}")


def main():
    current_key          = None
    error_count          = 0
    total_requests       = 0
    distinct_error_hosts = set()

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
            status_code = int(value_parts[0])
        except ValueError:
            continue
        host = value_parts[1]

        if key != current_key:
            if current_key is not None:
                emit(current_key, error_count, total_requests, distinct_error_hosts)
            current_key          = key
            error_count          = 0
            total_requests       = 0
            distinct_error_hosts = set()

        total_requests += 1
        if 400 <= status_code <= 599:
            error_count += 1
            distinct_error_hosts.add(host)

    # Flush the final group
    if current_key is not None:
        emit(current_key, error_count, total_requests, distinct_error_hosts)


if __name__ == "__main__":
    main()