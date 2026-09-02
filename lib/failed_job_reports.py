"""
Formats selected failed jobs and builds the failed-job alert section.
"""

import datetime
import re
from typing import Optional

from lib.report_formatting import format_check_status, format_datetime, format_report_header

MAX_FAILED_JOBS_TO_SHOW = 3
TRACEBACK_FRAME_PATTERN = re.compile(r'^\s*File "([^"]+)", line (\d+), in (.+)$')


def extract_exception_details(exc_info: Optional[str]) -> dict:  # noqa: FA100 -- keep Python 3.8-compatible union syntax
    """
    Extracts the final exception and deepest traceback frames from RQ exception text.
    Called by: format_failed_job().

    >>> exc_info = (
    ...     'Traceback (most recent call last):\\n'
    ...     '  File "/app/tasks.py", line 10, in process\\n'
    ...     '    load_record()\\n'
    ...     '  File "/app/records.py", line 20, in load_record\\n'
    ...     '    raise ValueError("missing")\\n'
    ...     'ValueError: missing\\n'
    ... )
    >>> details = extract_exception_details(exc_info)
    >>> details['exception']
    'ValueError: missing'
    >>> details['frames'][0]
    {'file': '/app/tasks.py', 'line': '10', 'function': 'process', 'code': 'load_record()'}
    >>> details['frames'][1]
    {'file': '/app/records.py', 'line': '20', 'function': 'load_record', 'code': 'raise ValueError("missing")'}

    """
    if not exc_info:
        details = {'exception': 'Unavailable', 'frames': [], 'recent_lines': []}
        return details
    source_lines = exc_info.splitlines()
    nonblank_lines = [line.strip() for line in source_lines if line.strip()]
    exception_message = nonblank_lines[-1] if nonblank_lines else 'Unavailable'
    frames = []
    for index, line in enumerate(source_lines):
        match = TRACEBACK_FRAME_PATTERN.match(line)
        if match is None:
            continue
        code = ''
        if index + 1 < len(source_lines):
            next_line = source_lines[index + 1].strip()
            if next_line and TRACEBACK_FRAME_PATTERN.match(source_lines[index + 1]) is None:
                code = next_line
        frames.append(
            {
                'file': match.group(1),
                'line': match.group(2),
                'function': match.group(3),
                'code': code,
            }
        )
    recent_lines = []
    if not frames and len(nonblank_lines) > 1:
        recent_lines = nonblank_lines[-7:-1]
    details = {'exception': exception_message[:500], 'frames': frames[-2:], 'recent_lines': recent_lines}
    return details


def sort_failed_jobs_newest_first(failed_jobs: list) -> list:
    """
    Sorts failed jobs by failure time, falling back to their queue order.
    Called by: build_failure_queue_check_report().
    """
    sortable_jobs = []
    for position, job in enumerate(failed_jobs):
        ended_at = getattr(job, 'ended_at', None)
        if ended_at is None:
            sort_time = float('-inf')
        else:
            if ended_at.tzinfo is None:
                ended_at = ended_at.replace(tzinfo=datetime.timezone.utc)
            sort_time = ended_at.timestamp()
        sortable_jobs.append((sort_time, position, job))
    sortable_jobs.sort(key=lambda item: (item[0], item[1]), reverse=True)
    sorted_jobs = [item[2] for item in sortable_jobs]
    return sorted_jobs


def format_failed_job(job, display_number: int, displayed_count: int) -> str:
    """
    Formats one selected failed job for a plain-text email.
    Called by: build_failure_queue_check_report().
    """
    job_id = getattr(job, 'id', None) or 'Unavailable'
    queue = getattr(job, 'origin', None) or 'Unavailable'
    try:
        function = getattr(job, 'func_name', None) or 'Unavailable'
    except Exception:  # noqa: BLE001 -- malformed or incompatible serialized job data can raise multiple exception types
        function = 'Unavailable'
    exception_details = extract_exception_details(getattr(job, 'exc_info', None))
    newest_label = ' - NEWEST' if display_number == 1 else ''
    lines = [
        f'FAILED JOB {display_number} OF {displayed_count}{newest_label}',
        '',
        f'Job ID: {job_id}',
        f'Queue: {queue}',
        f'Function: {function}',
        f'Enqueued: {format_datetime(getattr(job, "enqueued_at", None))}',
        f'Started: {format_datetime(getattr(job, "started_at", None))}',
        f'Failed: {format_datetime(getattr(job, "ended_at", None))}',
        '',
        'Exception:',
        exception_details['exception'],
        '',
        'Recent traceback:',
    ]
    if exception_details['frames']:
        for frame in exception_details['frames']:
            lines.extend(
                [
                    f'- File: {frame["file"]}',
                    f'  Line: {frame["line"]}',
                    f'  Function: {frame["function"]}',
                ]
            )
            if frame['code']:
                lines.append(f'  Code: {frame["code"][:300]}')
    elif exception_details['recent_lines']:
        lines.extend([f'- {line[:300]}' for line in exception_details['recent_lines']])
    else:
        lines.append('- Unavailable')
    result = '\n'.join(lines)
    return result


def build_failure_queue_check_report(
    new_failures: list, previous_failure_count: int, expectations_dct: dict, evaluation_dct: dict, data_dct: dict
) -> str:
    """
    Builds the failed-job count comparison and limited selected-job details.
    Called by: email_delivery.build_email_message().

    >>> failed_job_class = type('FailedJob', (), {})
    >>> failed_jobs = []
    >>> for hour in range(4):
    ...     job = failed_job_class()
    ...     job.id = f'job-{hour}'
    ...     job.origin = 'example'
    ...     job.func_name = 'example.run'
    ...     job.enqueued_at = None
    ...     job.started_at = None
    ...     job.ended_at = datetime.datetime(2026, 1, 1, hour, tzinfo=datetime.timezone.utc)
    ...     job.exc_info = 'ValueError: example'
    ...     failed_jobs.append(job)
    >>> report = build_failure_queue_check_report(
    ...     failed_jobs,
    ...     10,
    ...     {'surge_failure_limit': 0},
    ...     {'failure_queue_check': 'FAIL'},
    ...     {'failed_count': 14},
    ... )
    >>> report.count('FAILED JOB')
    3
    >>> report.index('Job ID: job-3') < report.index('Job ID: job-2')
    True
    >>> 'Job ID: job-0' in report
    False

    """
    current_failure_count = data_dct['failed_count']
    failure_change = current_failure_count - previous_failure_count
    lines = [
        format_report_header(f'FAILED-JOB CHECK: {format_check_status(evaluation_dct["failure_queue_check"])}'),
        '',
        f'Previous failed-job count: {previous_failure_count}',
        f'Current failed-job count: {current_failure_count}',
        f'Change: {failure_change}',
        f'Allowed increase: {expectations_dct["surge_failure_limit"]}',
    ]
    if evaluation_dct['failure_queue_check'] == 'FAIL':
        sorted_failures = sort_failed_jobs_newest_first(list(new_failures))
        failures_to_show = sorted_failures[:MAX_FAILED_JOBS_TO_SHOW]
        available_count = len(sorted_failures)
        displayed_count = len(failures_to_show)
        lines.extend(['', f'Selected failed-job details available: {available_count}'])
        if displayed_count:
            if available_count == 1:
                lines.append('Showing the newest selected failed job.')
            else:
                lines.append(f'Showing the newest {displayed_count} of {available_count} selected failed jobs.')
            hidden_count = available_count - displayed_count
            if hidden_count:
                hidden_label = 'job is' if hidden_count == 1 else 'jobs are'
                lines.append(f'{hidden_count} additional selected {hidden_label} not shown.')
            for display_number, job in enumerate(failures_to_show, start=1):
                lines.extend(['', format_failed_job(job, display_number, displayed_count)])
        else:
            lines.extend(['', 'No selected failed-job details were available.'])
    report = '\n'.join(lines)
    return report
