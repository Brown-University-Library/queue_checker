#!/usr/bin/env python

"""
Runs the queue-checker doctests.
"""

import argparse
import doctest
from unittest.mock import Mock, patch

import queue_check
from lib import check_reports, email_delivery, failed_job_reports, queue_data, queue_evaluation

DOCTEST_MODULES = (
    queue_check,
    queue_data,
    queue_evaluation,
    check_reports,
    failed_job_reports,
    email_delivery,
)


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments.
    Called by: run_tests()
    """
    parser = argparse.ArgumentParser(description='Runs the doctests embedded in the queue-checker modules.')
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
    failed_count = 0
    with patch.object(queue_evaluation, 'get_failed_queue', return_value=failed_queue):
        for doctest_module in DOCTEST_MODULES:
            test_results = doctest.testmod(doctest_module, verbose=args.verbose)
            failed_count += test_results.failed
    exit_status = 1 if failed_count else 0
    return exit_status


if __name__ == '__main__':
    raise SystemExit(run_tests())
