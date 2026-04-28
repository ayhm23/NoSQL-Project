#!/usr/bin/env python3
"""
mapreduce/q3_mapper.py
======================
Q3 Mapper — Hourly Error Analysis

Input  (stdin) : raw NASA log lines
Output (stdout): {log_date}|{log_hour}\t{status_code}|{host}

Emits ALL records (not just errors) so the reducer can compute
both total_requests and error_count for each (date, hour) bucket.
The reducer applies the 400–599 error filter itself.
"""

import sys
import os

sys.path.insert(0, '.')
from log_parser import parse_line


def main():
    for raw_line in sys.stdin:
        record, malformed = parse_line(raw_line.rstrip('\n'))

        if malformed or record is None:
            sys.stderr.write("reporter:counter:NasaETL,MalformedRecords,1\n")
            continue

        key   = f"{record.log_date}|{record.log_hour}"
        value = f"{record.status_code}|{record.host}"
        print(f"{key}\t{value}")


if __name__ == "__main__":
    main()