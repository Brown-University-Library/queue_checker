"""
This code checks:
- that expected queues exist
- that expected workers are running.
- that the failed-queue count is as expected.

Usage:
% cd /path/to/queue_checker/
% source ../env/bin/activate                # for access to rqinfo
% source ../venv_settings/env_settings.sh   # for access to settings
% python ./queue_check.py

Tests can be run via substituting for the above line:
% python -m doctest ./queue_check.py
(which will show no output if all tests pass) ...or...
% python -m doctest -v ./queue_check.py
"""

import datetime
import json
import logging
import os
import pprint
import re
import smtplib
import socket
import subprocess
from email.mime.text import MIMEText
from pathlib import Path
from string import Template
from typing import Optional

from dotenv import load_dotenv
from redis import Redis
from rq import get_failed_queue

DOTENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=DOTENV_PATH, override=True)

MAX_FAILED_JOBS_TO_SHOW = 3
TRACEBACK_FRAME_PATTERN = re.compile(r'^\s*File "([^"]+)", line (\d+), in (.+)$')


class QueueCheckerError(Exception):
    """
    Indicates that the queue checker cannot complete an operation.
    """


ENV_LOG_LEVEL = os.environ.get('QCHKR__LOG_LEVEL','INFO')
level_dct = { 'DEBUG': logging.DEBUG, 'INFO': logging.INFO, }
logging.basicConfig(  # no file-logging for now
    level=level_dct[ENV_LOG_LEVEL],
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
log = logging.getLogger( '__name__' )


expectations: dict = json.loads( os.environ.get('QCHKR__EXPECTATIONS_JSON', '{}' ) )
log.debug( f'expectations, ``{pprint.pformat(expectations)}``' )    

## main controller --------------------------------------------------


def run_code():
    """
    Controller.
    Called by: dunder-main.
    """
    # previous_rqinfo_data = load_previous_rqinfo_data()
    # assert type(previous_rqinfo_data) == dict
    ## run `rqinfo` -------------------------------------------------
    output  = get_rqinfo()
    assert type(output) == str
    ## parse `rqinfo` output ----------------------------------------
    data_dct = parse_rqinfo( output )
    assert type(data_dct) == dict
    ## load previous `rqinfo` data ----------------------------------
    previous_rqinfo_data = load_previous_rqinfo_data( data_dct )
    assert type(previous_rqinfo_data) == dict
    ## save current `rqinfo` data -----------------------------------
    save_rqinfo_data( data_dct )
    ## evaluate `rqinfo` output -------------------------------------
    last_failed_count = previous_rqinfo_data['failed_count']
    evaluation_dct, new_failures = evaluate_qdata( last_failed_count, expectations, data_dct )
    assert type(evaluation_dct) == dict
    if evaluation_dct == {'queue_check': 'ok', 'worker_check': 'ok', 'failure_queue_check': 'ok'}:
        pass
    ## send email if necessary ---------------------------------------
    else:
        previous_failure_count = previous_rqinfo_data['failed_count']
        msg: str = build_email_message( new_failures, previous_failure_count, expectations, evaluation_dct, data_dct )
        send_email( message=msg )
    log.info( f'evaluation_dct, ``{pprint.pformat(evaluation_dct)}``' )


## helper functions called by run_code() ----------------------------


def load_previous_rqinfo_data( current_rqinfo_data ):
    """
    Loads previous rqinfo data from file.
    Called by: run_code().
    On failure, saves current data to file, and returns current-data.
        - This enables a smooth first run of the script.
    """
    try:
        with open( '../previous_rqinfo_data/previous_rqinfo_data.json', 'r' ) as f:
            previous_rqinfo_data = json.loads( f.read() )
        assert type(previous_rqinfo_data) == dict
        log.debug( f' previous_rqinfo_data, loaded from file, ``{pprint.pformat(previous_rqinfo_data)}``' )
    ## Any load, parse, or validation failure uses current data so the first run can continue.
    except Exception as e:  # noqa: BLE001
        log.warning( f'exception loading previous data; err, ``{e}``; will save existing data.' )
        save_rqinfo_data( current_rqinfo_data )
        previous_rqinfo_data = current_rqinfo_data
        log.debug( f' previous_rqinfo_data, from _current_ data, ``{pprint.pformat(previous_rqinfo_data)}``' )
    return previous_rqinfo_data


# def load_previous_rqinfo_data():
#     """
#     Loads previous rqinfo data from file."""
#     with open( '../previous_rqinfo_data/previous_rqinfo_data.json', 'r' ) as f:
#         previous_rqinfo_data = json.loads( f.read() )
#     assert type(previous_rqinfo_data) == dict
#     log.debug( f' previous_rqinfo_data, ``{pprint.pformat(previous_rqinfo_data)}``' )
#     return previous_rqinfo_data


def get_rqinfo() -> str:
    """
    Runs `rqinfo`, returns output.
    Called by: run_code().
    - `--by-queue` returns the normal queue output, but shows workers associated with each queue.
    - `--raw` doesn't return the summary line or the job-bar, just the basic data.
    """
    result = subprocess.run(['rqinfo', '--by-queue', '--raw'], stdout=subprocess.PIPE, check=False)
    output = result.stdout.decode()
    assert type(output) == str
    log.debug( f'output, ``{output}``' )
    return output


def parse_rqinfo( rq_output ):
    """ 
    Parses rqinfo output into a dict.
    Called by: run_code().
    Doctest usage (w/env sourced): `% python -m doctest ./queue_check.py`

    Example:
    >>> result = parse_rqinfo(
    ...     'queue q_1 0\\n'
    ...     'queue q_2 0\\n'
    ...     'queue failed 333\\n'
    ...     'q_1: server.968 (idle), server.952 (idle)\\n'
    ...     'q_2: server.952 (idle)\\n'
    ...     'failed: –\\n'
    ... )
    >>> result
    {'failed_count': 333, 'queues': ['q_1', 'q_2', 'failed'], 'workers_by_queue': {'q_1': ['server.968', 'server.952'], 'q_2': ['server.952'], 'failed': []}}
    >>> pprint.pprint( result )
    {'failed_count': 333,
     'queues': ['q_1', 'q_2', 'failed'],
     'workers_by_queue': {'failed': [],
                          'q_1': ['server.968', 'server.952'],
                          'q_2': ['server.952']}}
    """
    lines = rq_output.split('\n')
    log.debug( f'lines, ``{lines}``' )
    output = {'failed_count': 0, 'queues': [], 'workers_by_queue': {}}
    for line in lines:
        log.debug( f'processing line, ``{line}``' )
        line = line.strip()
        if line == '':
            log.debug( 'blank line; continuing' )
            continue
        if line.startswith('queue'):    # Line format: queue <queue_name> <count>
            ( _, queue_name, count ) = line.split()
            output['queues'].append(queue_name)
            if queue_name == 'failed':
                output['failed_count'] = int(count)
        else:                           # Line format: <queue_name>: <worker.123 (idle), worker.124 (idle)> ...or...
                                        #                    failed: –
            ( queue_name, worker_data ) = line.split(':')
            worker_data = worker_data.strip()
            worker_names = []
            if worker_data != '–':      # Split by comma and get the worker name from each part
                worker_names = [part.split()[0] for part in worker_data.split(',')]
            output['workers_by_queue'][queue_name] = worker_names
    log.debug( f'output, ``{pprint.pformat(output)}``' )
    return output
    # end def parse_rqinfo()


def compare_queue_data( expectations_dct: dict, data_dct: dict ) -> dict:
    """
    Compares expected queues with queues found by rqinfo.
    Called by: evaluate_qdata(), build_check_summary(), and build_queue_check_report().

    >>> details = compare_queue_data(
    ...     {'expected_queues': ['q1', 'q2', 'q3']},
    ...     {'queues': ['q1', 'q3', 'extra']},
    ... )
    >>> details
    {'expected': ['q1', 'q2', 'q3'], 'found_expected': ['q1', 'q3'], 'missing': ['q2'], 'additional': ['extra']}

    """
    expected_queues = list( expectations_dct['expected_queues'] )
    found_queues = list( data_dct['queues'] )
    found_expected = [queue for queue in expected_queues if queue in found_queues]
    missing_queues = [queue for queue in expected_queues if queue not in found_queues]
    additional_queues = [queue for queue in found_queues if queue not in expected_queues]
    details = {
        'expected': expected_queues,
        'found_expected': found_expected,
        'missing': missing_queues,
        'additional': additional_queues,
    }
    return details


def compare_worker_data( expectations_dct: dict, data_dct: dict ) -> dict:
    """
    Compares expected worker counts with worker data found by rqinfo.
    Called by: evaluate_qdata(), build_check_summary(), and build_worker_check_report().

    >>> details = compare_worker_data(
    ...     {'expected_workers': [
    ...         {'queue': 'missing', 'worker_count': 1},
    ...         {'queue': 'wrong', 'worker_count': 1},
    ...         {'queue': 'matching', 'worker_count': 1},
    ...     ]},
    ...     {'workers_by_queue': {
    ...         'wrong': ['server.1', 'server.2'],
    ...         'matching': ['server.3'],
    ...     }},
    ... )
    >>> [detail['queue'] for detail in details['unavailable']]
    ['missing']
    >>> [(detail['queue'], detail['expected_count'], detail['found_count']) for detail in details['mismatched']]
    [('wrong', 1, 2)]
    >>> [detail['queue'] for detail in details['matched']]
    ['matching']

    """
    workers_by_queue = data_dct['workers_by_queue']
    unavailable = []
    mismatched = []
    matched = []
    for expectation in expectations_dct['expected_workers']:
        queue = expectation['queue']
        expected_count = expectation['worker_count']
        if queue not in workers_by_queue:
            unavailable.append({
                'queue': queue,
                'expected_count': expected_count,
                'found_count': None,
                'workers': [],
            })
            continue
        workers = list( workers_by_queue[queue] )
        detail = {
            'queue': queue,
            'expected_count': expected_count,
            'found_count': len(workers),
            'workers': workers,
        }
        if detail['found_count'] == expected_count:
            matched.append( detail )
        else:
            mismatched.append( detail )
    details = {'unavailable': unavailable, 'mismatched': mismatched, 'matched': matched}
    return details


def save_rqinfo_data( data_dct ):
    """
    Saves rqinfo data to file.
    Called by: run_code() and load_previous_rqinfo_data().
    """
    assert type(data_dct) == dict
    jsn = json.dumps( data_dct, sort_keys=True, indent=2 )
    ## assume unicorns exist ------------------------------------------
    file_path = '../previous_rqinfo_data/previous_rqinfo_data.json'
    try:
        with open( file_path, 'w' ) as f:
            f.write( jsn )
    ## only acknowledge unhappiness if necessary ----------------------
    except FileNotFoundError:
        os.makedirs( os.path.dirname(file_path), exist_ok=True )
        with open( file_path, 'w' ) as f:
            f.write( jsn )
    except Exception as e:
        log.exception( 'problem saving rqinfo data; traceback follows' )
        raise QueueCheckerError( f'problem saving rqinfo data; error, ``{e!r}``' ) from e
    log.debug( 'rqinfo data saved' )


def evaluate_qdata( previous_failed_count, expectations, data_dct ):
    """ 
    Evaluates rqinfo output against expectation-data.
    Called by: run_code().

    Example -- all ok:
    >>> previous_failed_count = 10
    >>> expectations_data = {'expected_queues': ['q1', 'q2'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}], 'surge_failure_limit': 10}
    >>> rqinfo_data = {'failed_count': 15, 'queues': ['q1', 'q2', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'q2': ['server.234'], 'failed': []}}
    >>> result, new_failures = evaluate_qdata( previous_failed_count, expectations_data, rqinfo_data )
    >>> result
    {'queue_check': 'ok', 'worker_check': 'ok', 'failure_queue_check': 'ok'}
    
    Example -- problem:
    >>> previous_failed_count = 10
    >>> expectations_data = {'expected_queues': ['q1', 'q2', 'q3'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}, {'queue': 'q2', 'worker_count': 1}], 'surge_failure_limit': 10}
    >>> rqinfo_data = {'failed_count': 30, 'queues': ['q1', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'failed': []}}
    >>> result, new_failures = evaluate_qdata( previous_failed_count, expectations_data, rqinfo_data )
    >>> result
    {'queue_check': 'FAIL', 'worker_check': 'FAIL', 'failure_queue_check': 'FAIL'}
    """
    assert type( previous_failed_count ) == int
    assert type( expectations ) == dict
    assert type( data_dct ) == dict
    checks_result = {'queue_check': 'init', 'worker_check': 'init', 'failure_queue_check': 'init'}
    ## queue check --------------------------------------------------
    queue_details = compare_queue_data( expectations, data_dct )
    checks_result['queue_check'] = 'FAIL' if queue_details['missing'] else 'ok'
    log.debug( f'missing queue count, ``{len(queue_details["missing"])}``' )
    log.debug( f'after queue-check, checks_result, ``{checks_result}``' )
    ## worker check --------------------------------------------------
    worker_details = compare_worker_data( expectations, data_dct )
    worker_problem_count = len(worker_details['unavailable']) + len(worker_details['mismatched'])
    checks_result['worker_check'] = 'FAIL' if worker_problem_count else 'ok'
    log.debug( f'worker problem count, ``{worker_problem_count}``' )
    log.debug( f'after worker-check, checks_result, ``{checks_result}``' )
    ## failure-count check ------------------------------------------
    failure_increase = data_dct['failed_count'] - previous_failed_count
    log.debug( f'failure_increase, ``{failure_increase}``' )
    surge_failure_limit = expectations['surge_failure_limit']
    log.debug( f'surge_failure_limit, ``{surge_failure_limit}``' )
    if failure_increase > surge_failure_limit:
        log.debug( 'failure-increase exceeded expectation-settings-limit' )
        checks_result['failure_queue_check'] = 'FAIL'
        new_failures = get_failed_queue(connection=Redis('localhost')).jobs[-failure_increase:]
    else:
        checks_result['failure_queue_check'] = 'ok'
        new_failures=[]
    log.debug( f'checks_result, ``{checks_result}``' )
    return checks_result, new_failures
    # end def evaluate_qdata()


def format_datetime( value ) -> str:
    """
    Formats an aware datetime in local time and treats an RQ naive datetime as UTC.
    Called by: build_email_message() and format_failed_job().
    """
    if value is None:
        result = 'Unavailable'
    else:
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        result = value.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
    return result


def format_check_status( status: str ) -> str:
    """
    Formats an internal check result for an operator-facing report.
    Called by: build_check_summary(), build_queue_check_report(), build_worker_check_report(), and
    build_failure_queue_check_report().
    """
    result = 'FAILED' if status == 'FAIL' else status.upper()
    return result


def build_check_summary( previous_failure_count: int, expectations_dct: dict, evaluation_dct: dict, data_dct: dict ) -> str:
    """
    Builds the short check summary shown near the beginning of an alert.
    Called by: build_email_message().
    """
    queue_details = compare_queue_data( expectations_dct, data_dct )
    worker_details = compare_worker_data( expectations_dct, data_dct )
    failure_change = data_dct['failed_count'] - previous_failure_count
    summary_lines = [
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
    summary = '\n'.join( summary_lines )
    return summary


def build_queue_check_report( expectations_dct: dict, evaluation_dct: dict, data_dct: dict ) -> str:
    """
    Builds a complete plain-text comparison of expected and found queues.
    Called by: build_email_message().

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
    details = compare_queue_data( expectations_dct, data_dct )
    lines = [
        f'QUEUE CHECK: {format_check_status(evaluation_dct["queue_check"])}',
        '',
        f'Expected queues: {len(details["expected"])}',
        f'Expected queues found: {len(details["found_expected"])}',
        f'Missing expected queues: {len(details["missing"])}',
        f'Additional found queues: {len(details["additional"])}',
        '',
        'Missing expected queues:',
    ]
    if details['missing']:
        lines.extend( [f'- {queue}' for queue in details['missing']] )
    else:
        lines.append( '- None' )
    lines.extend( ['', 'Expected queues found:'] )
    if details['found_expected']:
        lines.extend( [f'- {queue}' for queue in details['found_expected']] )
    else:
        lines.append( '- None' )
    lines.extend( ['', 'Additional found queues:'] )
    if details['additional']:
        lines.extend( [f'- {queue}' for queue in details['additional']] )
    else:
        lines.append( '- None' )
    report = '\n'.join( lines )
    return report


def append_worker_names( lines: list, workers: list ) -> None:
    """
    Appends worker identifiers to a list of report lines.
    Called by: build_worker_check_report().
    """
    if len(workers) == 1:
        lines.append( f'  Worker: {workers[0]}' )
    elif len(workers) > 1:
        lines.append( '  Workers:' )
        lines.extend( [f'  - {worker}' for worker in workers] )


def build_worker_check_report( expectations_dct: dict, evaluation_dct: dict, data_dct: dict ) -> str:
    """
    Builds a complete plain-text comparison of expected and found workers.
    Called by: build_email_message().

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
    details = compare_worker_data( expectations_dct, data_dct )
    expected_count = len(expectations_dct['expected_workers'])
    lines = [
        f'WORKER CHECK: {format_check_status(evaluation_dct["worker_check"])}',
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
            lines.extend([
                f'- {detail["queue"]}',
                f'  Expected workers: {detail["expected_count"]}',
            ])
    else:
        lines.append( '- None' )
    lines.extend( ['', 'Worker-count mismatches:'] )
    if details['mismatched']:
        for detail in details['mismatched']:
            lines.extend([
                f'- {detail["queue"]}',
                f'  Expected: {detail["expected_count"]}',
                f'  Found: {detail["found_count"]}',
            ])
            append_worker_names( lines, detail['workers'] )
    else:
        lines.append( '- None' )
    lines.extend( ['', 'Worker counts that matched:'] )
    if details['matched']:
        for detail in details['matched']:
            lines.extend([
                f'- {detail["queue"]}',
                f'  Expected: {detail["expected_count"]}',
                f'  Found: {detail["found_count"]}',
            ])
            append_worker_names( lines, detail['workers'] )
    else:
        lines.append( '- None' )
    report = '\n'.join( lines )
    return report


def extract_exception_details( exc_info: Optional[str] ) -> dict:  # noqa: FA100 -- keep Python 3.8-compatible union syntax
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
        match = TRACEBACK_FRAME_PATTERN.match( line )
        if match is None:
            continue
        code = ''
        if index + 1 < len(source_lines):
            next_line = source_lines[index + 1].strip()
            if next_line and TRACEBACK_FRAME_PATTERN.match(source_lines[index + 1]) is None:
                code = next_line
        frames.append({
            'file': match.group(1),
            'line': match.group(2),
            'function': match.group(3),
            'code': code,
        })
    recent_lines = []
    if not frames and len(nonblank_lines) > 1:
        recent_lines = nonblank_lines[-7:-1]
    details = {'exception': exception_message[:500], 'frames': frames[-2:], 'recent_lines': recent_lines}
    return details


def sort_failed_jobs_newest_first( failed_jobs: list ) -> list:
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
        sortable_jobs.append( (sort_time, position, job) )
    sortable_jobs.sort(key=lambda item: (item[0], item[1]), reverse=True)
    sorted_jobs = [item[2] for item in sortable_jobs]
    return sorted_jobs


def format_failed_job( job, display_number: int, displayed_count: int ) -> str:
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
    exception_details = extract_exception_details( getattr(job, 'exc_info', None) )
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
            lines.extend([
                f'- File: {frame["file"]}',
                f'  Line: {frame["line"]}',
                f'  Function: {frame["function"]}',
            ])
            if frame['code']:
                lines.append( f'  Code: {frame["code"][:300]}' )
    elif exception_details['recent_lines']:
        lines.extend( [f'- {line[:300]}' for line in exception_details['recent_lines']] )
    else:
        lines.append( '- Unavailable' )
    result = '\n'.join( lines )
    return result


def build_failure_queue_check_report(
        new_failures: list,
        previous_failure_count: int,
        expectations_dct: dict,
        evaluation_dct: dict,
        data_dct: dict ) -> str:
    """
    Builds the failed-job count comparison and limited selected-job details.
    Called by: build_email_message().

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
        f'FAILED-JOB CHECK: {format_check_status(evaluation_dct["failure_queue_check"])}',
        '',
        f'Previous failed-job count: {previous_failure_count}',
        f'Current failed-job count: {current_failure_count}',
        f'Change: {failure_change}',
        f'Allowed increase: {expectations_dct["surge_failure_limit"]}',
    ]
    if evaluation_dct['failure_queue_check'] == 'FAIL':
        sorted_failures = sort_failed_jobs_newest_first( list(new_failures) )
        failures_to_show = sorted_failures[:MAX_FAILED_JOBS_TO_SHOW]
        available_count = len(sorted_failures)
        displayed_count = len(failures_to_show)
        lines.extend( ['', f'Selected failed-job details available: {available_count}'] )
        if displayed_count:
            if available_count == 1:
                lines.append( 'Showing the newest selected failed job.' )
            else:
                lines.append( f'Showing the newest {displayed_count} of {available_count} selected failed jobs.' )
            hidden_count = available_count - displayed_count
            if hidden_count:
                hidden_label = 'job is' if hidden_count == 1 else 'jobs are'
                lines.append( f'{hidden_count} additional selected {hidden_label} not shown.' )
            for display_number, job in enumerate(failures_to_show, start=1):
                lines.extend( ['', format_failed_job(job, display_number, displayed_count)] )
        else:
            lines.extend( ['', 'No selected failed-job details were available.'] )
    report = '\n'.join( lines )
    return report


def build_email_message( new_failures, previous_failure_count, expectations_dct, evaluation_dct, data_dct ):
    """
    Assembles a plain-text email message with labeled check comparisons.
    Called by: run_code().

    >>> message = build_email_message(
    ...     [],
    ...     4,
    ...     {
    ...         'expected_queues': ['q1'],
    ...         'expected_workers': [{'queue': 'q1', 'worker_count': 1}],
    ...         'surge_failure_limit': 0,
    ...     },
    ...     {'queue_check': 'FAIL', 'worker_check': 'FAIL', 'failure_queue_check': 'ok'},
    ...     {'failed_count': 4, 'queues': [], 'workers_by_queue': {}},
    ... )
    >>> message.startswith('QUEUE CHECKER ALERT\\n')
    True
    >>> 'Overall result: 2 of 3 checks failed' in message
    True
    >>> 'Unable to check because the queue was not found:' in message
    True

    """
    assert type(evaluation_dct) == dict
    assert type(data_dct) == dict
    failed_check_count = sum(1 for result in evaluation_dct.values() if result == 'FAIL')
    overall_result = f'{failed_check_count} of {len(evaluation_dct)} checks failed'
    timestamp = format_datetime( datetime.datetime.now().astimezone() )
    with open('email_template.txt', 'r') as f:
        src = Template(f.read())
        result = src.safe_substitute({
            'server': socket.gethostname().upper(),
            'timestamp': timestamp,
            'overall_result': overall_result,
            'check_summary': build_check_summary(previous_failure_count, expectations_dct, evaluation_dct, data_dct),
            'failure_queue_report': build_failure_queue_check_report(
                new_failures, previous_failure_count, expectations_dct, evaluation_dct, data_dct),
            'queue_report': build_queue_check_report(expectations_dct, evaluation_dct, data_dct),
            'worker_report': build_worker_check_report(expectations_dct, evaluation_dct, data_dct),
        })
        msg = result.rstrip() + '\n'
    log.debug( f'email message character count, ``{len(msg)}``' )
    return msg


def send_email( message ):
    """
    Sends mail; generates exception which cron-job should email to crontab owner on sendmail failure.
    Called by: run_code().
    """
    assert type(message) == str, type(message)
    log.debug( f'message, ``{message}``' )
    EMAIL_HOST = os.environ['QCHKR__EMAIL_HOST']
    EMAIL_PORT = int( os.environ['QCHKR__EMAIL_HOST_PORT'] )  
    EMAIL_FROM = os.environ['QCHKR__EMAIL_FROM']
    EMAIL_RECIPIENTS = json.loads( os.environ['QCHKR__EMAIL_RECIPIENTS_JSON'] )
    HOST = socket.gethostname()
    try:
        s = smtplib.SMTP( EMAIL_HOST, EMAIL_PORT )
        body = message
        eml = MIMEText( f'{body}' )
        eml['Subject'] = f'queue-checker alert from ``{HOST.upper()}``'
        eml['From'] = EMAIL_FROM
        eml['To'] = ';'.join( EMAIL_RECIPIENTS )
        s.sendmail( EMAIL_FROM, EMAIL_RECIPIENTS, eml.as_string())
    except Exception as e:
        err = repr( e )
        log.exception( f'Problem sending queue-checker mail, ``{err}``' )
        raise QueueCheckerError( err ) from e


## dunder-main ------------------------------------------------------

if __name__ == '__main__':
    run_code()
