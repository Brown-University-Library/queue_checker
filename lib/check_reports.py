"""
Builds queue, worker, and overall check sections for alert emails.
"""

from lib.queue_evaluation import compare_queue_data, compare_worker_data
from lib.report_formatting import format_check_status, format_report_header


def build_check_summary(previous_failure_count: int, expectations_dct: dict, evaluation_dct: dict, data_dct: dict) -> str:
    """
    Builds the short check summary shown near the beginning of an alert.
    Called by: email_delivery.build_email_message().
    """
    queue_details = compare_queue_data(expectations_dct, data_dct)
    worker_details = compare_worker_data(expectations_dct, data_dct)
    failure_change = data_dct['failed_count'] - previous_failure_count
    summary_lines = [
        format_report_header('CHECK SUMMARY'),
        '',
        f'- Failed jobs: {format_check_status(evaluation_dct["failure_queue_check"])}',
        f'  Previous: {previous_failure_count}',
        f'  Current: {data_dct["failed_count"]}',
        f'  Change: {failure_change}',
        '',
        f'- Queues: {format_check_status(evaluation_dct["queue_check"])}',
        f'  Missing: {len(queue_details["missing"])} of {len(queue_details["expected"])} expected queues',
        '',
        f'- Workers: {format_check_status(evaluation_dct["worker_check"])}',
        f'  Unable to check because the queue was not found: {len(worker_details["unavailable"])}',
        f'  Wrong worker counts among found queues: {len(worker_details["mismatched"])}',
    ]
    summary = '\n'.join(summary_lines)
    return summary


def build_queue_check_report(expectations_dct: dict, evaluation_dct: dict, data_dct: dict) -> str:
    """
    Builds a complete plain-text comparison of expected and found queues.
    Called by: email_delivery.build_email_message().

    >>> report = build_queue_check_report(
    ...     {'expected_queues': ['q1', 'q2']},
    ...     {'queue_check': 'FAIL'},
    ...     {'queues': ['q1', 'extra']},
    ... )
    >>> 'Missing expected queues:\\n- q2' in report
    True
    >>> 'Additional found queues:\\n- extra' in report
    True

    """
    details = compare_queue_data(expectations_dct, data_dct)
    lines = [
        format_report_header(f'QUEUE CHECK: {format_check_status(evaluation_dct["queue_check"])}'),
        '',
        f'Expected queues: {len(details["expected"])}',
        f'Expected queues found: {len(details["found_expected"])}',
        f'Missing expected queues: {len(details["missing"])}',
        f'Additional found queues: {len(details["additional"])}',
        '',
        'Missing expected queues:',
    ]
    if details['missing']:
        lines.extend([f'- {queue}' for queue in details['missing']])
    else:
        lines.append('- None')
    lines.extend(['', 'Expected queues found:'])
    if details['found_expected']:
        lines.extend([f'- {queue}' for queue in details['found_expected']])
    else:
        lines.append('- None')
    lines.extend(['', 'Additional found queues:'])
    if details['additional']:
        lines.extend([f'- {queue}' for queue in details['additional']])
    else:
        lines.append('- None')
    report = '\n'.join(lines)
    return report


def append_worker_names(lines: list, workers: list) -> None:
    """
    Appends worker identifiers to a list of report lines.
    Called by: build_worker_check_report().
    """
    if len(workers) == 1:
        lines.append(f'  Worker: {workers[0]}')
    elif len(workers) > 1:
        lines.append('  Workers:')
        lines.extend([f'  - {worker}' for worker in workers])


def build_worker_check_report(expectations_dct: dict, evaluation_dct: dict, data_dct: dict) -> str:
    """
    Builds a complete plain-text comparison of expected and found workers.
    Called by: email_delivery.build_email_message().

    >>> report = build_worker_check_report(
    ...     {'expected_workers': [{'queue': 'missing', 'worker_count': 1}]},
    ...     {'worker_check': 'FAIL'},
    ...     {'workers_by_queue': {}},
    ... )
    >>> 'Unable to check because the queue was not found:\\n- missing\\n  Expected workers: 1' in report
    True
    >>> mismatch_report = build_worker_check_report(
    ...     {'expected_workers': [
    ...         {'queue': 'wrong', 'worker_count': 1},
    ...         {'queue': 'matching', 'worker_count': 1},
    ...     ]},
    ...     {'worker_check': 'FAIL'},
    ...     {'workers_by_queue': {
    ...         'wrong': ['server.1', 'server.2'],
    ...         'matching': ['server.3'],
    ...     }},
    ... )
    >>> 'Worker-count mismatches:\\n- wrong\\n  Expected: 1\\n  Found: 2' in mismatch_report
    True
    >>> 'Worker counts that matched:\\n- matching\\n  Expected: 1\\n  Found: 1' in mismatch_report
    True

    """
    details = compare_worker_data(expectations_dct, data_dct)
    expected_count = len(expectations_dct['expected_workers'])
    lines = [
        format_report_header(f'WORKER CHECK: {format_check_status(evaluation_dct["worker_check"])}'),
        '',
        f'Expected worker entries: {expected_count}',
        f'Worker counts that matched: {len(details["matched"])}',
        f'Wrong worker counts: {len(details["mismatched"])}',
        f'Unable to check: {len(details["unavailable"])}',
        '',
        'Unable to check because the queue was not found:',
    ]
    if details['unavailable']:
        for detail in details['unavailable']:
            lines.extend(
                [
                    f'- {detail["queue"]}',
                    f'  Expected workers: {detail["expected_count"]}',
                ]
            )
    else:
        lines.append('- None')
    lines.extend(['', 'Worker-count mismatches:'])
    if details['mismatched']:
        for detail in details['mismatched']:
            lines.extend(
                [
                    f'- {detail["queue"]}',
                    f'  Expected: {detail["expected_count"]}',
                    f'  Found: {detail["found_count"]}',
                ]
            )
            append_worker_names(lines, detail['workers'])
    else:
        lines.append('- None')
    lines.extend(['', 'Worker counts that matched:'])
    if details['matched']:
        for detail in details['matched']:
            lines.extend(
                [
                    f'- {detail["queue"]}',
                    f'  Expected: {detail["expected_count"]}',
                    f'  Found: {detail["found_count"]}',
                ]
            )
            append_worker_names(lines, detail['workers'])
    else:
        lines.append('- None')
    report = '\n'.join(lines)
    return report
