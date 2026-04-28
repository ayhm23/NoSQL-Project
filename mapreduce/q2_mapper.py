#!/usr/bin/env python3
"""
mapreduce/q2_mapper.py
======================
Q2 Mapper — Top 20 Requested Resources

Input  (stdin) : raw NASA log lines
Output (stdout): {resource_path}\t{bytes_transferred}|{host}

One line emitted per valid parsed record.
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

        key   = record.resource_path
        value = f"{record.bytes_transferred}|{record.host}"
        print(f"{key}\t{value}")


if __name__ == "__main__":
    main()