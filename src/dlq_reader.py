#!/usr/bin/env python3
"""Prints everything currently in the Dead Letter Queue, with the reason each
message ended up there."""

from src import console
from src.config import DLQ_TOPIC
from src.dlq import read_dlq


def main():
    console.dlq_table(read_dlq(), DLQ_TOPIC)


if __name__ == "__main__":
    main()
