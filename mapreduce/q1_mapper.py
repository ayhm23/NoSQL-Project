#!/usr/bin/env python3
"""
mapreduce/q1_mapper.py
======================
Q1 Mapper — Daily Traffic Summary

Input  (stdin) : raw NASA log lines
Output (stdout): {log_date}|{status_code}\t{bytes_transferred}

One line emitted per valid parsed record.
Malformed records are counted via Hadoop reporter counters (visible in job logs).
"""

import sys
import os

# When shipped via -files, log_parser.py lands in the task working directory
sys.path.insert(0, '.')
from log_parser import parse_line


def main():
    for raw_line in sys.stdin:
        record, malformed = parse_line(raw_line.rstrip('\n'))

        if malformed or record is None:
            # Report to Hadoop job counters — visible in yarn logs
            sys.stderr.write("reporter:counter:NasaETL,MalformedRecords,1\n")
            continue

        key   = f"{record.log_date}|{record.status_code}"
        value = str(record.bytes_transferred)
        print(f"{key}\t{value}")


if __name__ == "__main__":
    main()