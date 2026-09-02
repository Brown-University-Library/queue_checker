#!/usr/bin/env python

"""
Runs the queue-checker doctests.
"""

import argparse
import doctest
from unittest.mock import Mock, patch

import queue_check


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments.
    Called by: run_tests()
    """
    parser = argparse.ArgumentParser(description='Runs the doctests embedded in queue_check.py.')
    parser.add_argument('--verbose', action='store_true', help='Shows each doctest example and result.')
    args = parser.parse_args()
    return args


def run_tests() -> int:
    """
    Runs the doctests and returns a shell exit status.
    Called by: dunder-main.
    """
    args = parse_args()
    failed_queue = Mock()
    failed_queue.jobs = []
    with patch.object(queue_check, 'get_failed_queue', return_value=failed_queue):
        test_results = doctest.testmod(queue_check, verbose=args.verbose)
    exit_status = 1 if test_results.failed else 0
    return exit_status


if __name__ == '__main__':
    raise SystemExit(run_tests())
