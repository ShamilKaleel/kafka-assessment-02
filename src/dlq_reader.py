#!/usr/bin/env python3
"""Prints everything currently in the Dead Letter Queue, with the reason each
message ended up there."""

from src.config import DLQ_TOPIC
from src.dlq import read_dlq


def main():
    messages = read_dlq()
    for m in messages:
        print(f"DLQ offset {m.offset}  key={m.key}", flush=True)
        print(f"  reason : {m.reason}", flush=True)
        print(f"  from   : {m.origin}", flush=True)
        print(f"  order  : {m.order if m.order is not None else f'undecodable bytes: {m.raw!r}'}", flush=True)
    print(f"\n{len(messages)} message(s) in {DLQ_TOPIC}", flush=True)


if __name__ == "__main__":
    main()
