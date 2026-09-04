"""
Collects, parses, loads, and saves queue data.
"""

import json
import logging
import os
import pprint
import subprocess

from redis import Redis
from rq import get_failed_queue

from lib.errors import QueueCheckerError

log = logging.getLogger(__name__)


def load_previous_rqinfo_data(current_rqinfo_data: dict) -> dict:
    """
    Loads previous queue data, using current data when no valid prior file exists.
    Called by: queue_check.run_code().
    """
    try:
        with open('../previous_rqinfo_data/previous_rqinfo_data.json', 'r') as file_handle:
            previous_rqinfo_data = json.loads(file_handle.read())
        assert type(previous_rqinfo_data) == dict
        assert type(previous_rqinfo_data['failed_count']) == int
        log.debug(f'previous_rqinfo_data, loaded from file, ``{pprint.pformat(previous_rqinfo_data)}``')
    ## Any load, parse, or validation failure uses current data so the first run can continue.
    except Exception as error:  # noqa: BLE001
        log.warning(f'exception loading previous data; err, ``{error}``; will save existing data.')
        save_rqinfo_data(current_rqinfo_data)
        previous_rqinfo_data = current_rqinfo_data
        log.debug(f'previous_rqinfo_data, from current data, ``{pprint.pformat(previous_rqinfo_data)}``')
    return previous_rqinfo_data


def summarize_command_error(error_output: str) -> str:
    """
    Converts command error output into a short single-line description.
    Called by: run_rqinfo().
    """
    detail = ' '.join(error_output.split())
    if not detail:
        detail = 'No error output was returned.'
    result = detail[:1000]
    return result


def run_rqinfo(arguments: list, description: str) -> str:
    """
    Runs one rqinfo inspection and rejects command failures or empty output.

    >>> from unittest.mock import Mock, patch
    >>> failed_result = Mock(returncode=1, stdout=b'', stderr=b'Connection refused')
    >>> with patch('lib.queue_data.subprocess.run', return_value=failed_result):
    ...     run_rqinfo(['--only-workers', '--raw'], 'worker information')
    Traceback (most recent call last):
      ...
    lib.errors.QueueCheckerError: Unable to collect worker information; rqinfo exited 1: Connection refused
    >>> empty_result = Mock(returncode=0, stdout=b'', stderr=b'')
    >>> with patch('lib.queue_data.subprocess.run', return_value=empty_result):
    ...     run_rqinfo(['--by-queue', '--raw'], 'queue information')
    Traceback (most recent call last):
      ...
    lib.errors.QueueCheckerError: Unable to collect queue information; rqinfo returned empty output.

    Called by: get_queue_rqinfo() and get_worker_rqinfo().
    """
    command = ['rqinfo'] + arguments
    try:
        result = subprocess.run(command, capture_output=True, check=False)
    except OSError as error:
        raise QueueCheckerError(f'Unable to run rqinfo for {description}; {type(error).__name__}: {error}') from error
    output = result.stdout.decode(errors='replace')
    error_output = result.stderr.decode(errors='replace')
    if result.returncode != 0:
        detail = summarize_command_error(error_output)
        raise QueueCheckerError(f'Unable to collect {description}; rqinfo exited {result.returncode}: {detail}')
    if not output.strip():
        raise QueueCheckerError(f'Unable to collect {description}; rqinfo returned empty output.')
    log.debug(f'{description}, ``{output}``')
    return output


def get_queue_rqinfo() -> str:
    """
    Collects registered queues, job counts, and the failed-job count.
    Called by: collect_rq_data().
    """
    output = run_rqinfo(['--by-queue', '--raw'], 'queue information')
    return output


def get_worker_rqinfo() -> str:
    """
    Collects workers independently of the registered-queue list.
    Called by: collect_rq_data().
    """
    output = run_rqinfo(['--only-workers', '--raw'], 'worker information')
    return output


def parse_queue_rqinfo(rq_output: str) -> dict:
    """
    Parses and validates the queue-organized raw rqinfo output.

    >>> result = parse_queue_rqinfo(
    ...     'queue q_1 2\\n'
    ...     'queue failed 3\\n'
    ...     'q_1: server.1 (idle)\\n'
    ...     'failed: –\\n'
    ... )
    >>> result
    {'failed_count': 3, 'queues': ['q_1', 'failed'], 'job_counts_by_queue': {'q_1': 2, 'failed': 3}}
    >>> parse_queue_rqinfo('not rqinfo output')
    Traceback (most recent call last):
      ...
    lib.errors.QueueCheckerError: Malformed queue information line: 'not rqinfo output'

    Called by: collect_rq_data().
    """
    queue_counts = {}
    queue_worker_lines = []
    for raw_line in rq_output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith('queue '):
            parts = line.split()
            if len(parts) != 3:
                raise QueueCheckerError(f'Malformed queue information line: {line!r}')
            queue_name = parts[1]
            try:
                job_count = int(parts[2])
            except ValueError as error:
                raise QueueCheckerError(f'Malformed queue job count line: {line!r}') from error
            if job_count < 0 or queue_name in queue_counts:
                raise QueueCheckerError(f'Malformed queue information line: {line!r}')
            queue_counts[queue_name] = job_count
            continue
        queue_name, separator, worker_text = line.partition(':')
        if not separator or not queue_name.strip() or not worker_text.strip():
            raise QueueCheckerError(f'Malformed queue information line: {line!r}')
        queue_name = queue_name.strip()
        if queue_name in queue_worker_lines:
            raise QueueCheckerError(f'Duplicate queue worker-information line: {queue_name!r}')
        queue_worker_lines.append(queue_name)
    if not queue_counts:
        raise QueueCheckerError('Malformed queue information: no queue-count lines were found.')
    if set(queue_counts) != set(queue_worker_lines):
        raise QueueCheckerError('Malformed queue information: queue-count and queue-worker names do not match.')
    data = {
        'failed_count': queue_counts.get('failed', 0),
        'queues': list(queue_counts),
        'job_counts_by_queue': queue_counts,
    }
    return data


def parse_worker_rqinfo(rq_output: str) -> dict:
    """
    Parses workers and reverses their declared queues into a queue-to-worker map.

    >>> result = parse_worker_rqinfo(
    ...     'worker server.1 idle high,medium,low\\n'
    ...     'worker server.2 busy high,medium,low\\n'
    ... )
    >>> result['workers_by_queue']
    {'high': ['server.1', 'server.2'], 'medium': ['server.1', 'server.2'], 'low': ['server.1', 'server.2']}
    >>> parse_worker_rqinfo('worker server.1 ?\\n')
    Traceback (most recent call last):
      ...
    lib.errors.QueueCheckerError: Worker information is incomplete; records without state or queue metadata: server.1

    Called by: collect_rq_data().
    """
    workers = []
    workers_by_queue = {}
    worker_names = set()
    incomplete_workers = []
    for raw_line in rq_output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=3)
        if len(parts) < 2 or parts[0] != 'worker':
            raise QueueCheckerError(f'Malformed worker information line: {line!r}')
        worker_name = parts[1]
        if worker_name in worker_names:
            raise QueueCheckerError(f'Duplicate worker information line: {worker_name!r}')
        worker_names.add(worker_name)
        if len(parts) != 4 or parts[2] == '?' or not parts[3].strip():
            incomplete_workers.append(worker_name)
            continue
        state = parts[2]
        queue_names = [queue_name.strip() for queue_name in parts[3].split(',')]
        if not queue_names or any(not queue_name for queue_name in queue_names) or len(queue_names) != len(set(queue_names)):
            raise QueueCheckerError(f'Malformed worker queue list for {worker_name!r}: {parts[3]!r}')
        workers.append({'name': worker_name, 'state': state, 'queues': queue_names})
        for queue_name in queue_names:
            workers_by_queue.setdefault(queue_name, []).append(worker_name)
    if incomplete_workers:
        names = ', '.join(incomplete_workers)
        raise QueueCheckerError(f'Worker information is incomplete; records without state or queue metadata: {names}')
    if not workers:
        raise QueueCheckerError('Malformed worker information: no complete worker lines were found.')
    data = {'workers': workers, 'workers_by_queue': workers_by_queue}
    return data


def collect_rq_data() -> dict:
    """
    Collects and combines independent queue and worker inspection results.
    Called by: queue_check.run_code().
    """
    queue_output = get_queue_rqinfo()
    worker_output = get_worker_rqinfo()
    queue_data = parse_queue_rqinfo(queue_output)
    worker_data = parse_worker_rqinfo(worker_output)
    data = dict(queue_data)
    data.update(worker_data)
    return data


def get_failed_job_details(failure_increase: int) -> dict:
    """
    Loads selected failed jobs without hiding a successful failed-count check when job details cannot be loaded.

    >>> get_failed_job_details(0)
    {'requested': False, 'jobs': [], 'error': None}

    Called by: queue_check.run_code().
    """
    details = {'requested': False, 'jobs': [], 'error': None}
    if failure_increase > 0:
        details['requested'] = True
        try:
            details['jobs'] = get_failed_queue(connection=Redis('localhost')).jobs[-failure_increase:]
        except Exception as error:
            log.exception('problem loading selected failed-job details; traceback follows')
            details['error'] = f'{type(error).__name__}: {error}'
    return details


def save_rqinfo_data(data_dct: dict) -> None:
    """
    Saves successfully collected queue data for the next failed-count comparison.
    Called by: queue_check.run_code() and load_previous_rqinfo_data().
    """
    assert type(data_dct) == dict
    json_text = json.dumps(data_dct, sort_keys=True, indent=2)
    file_path = '../previous_rqinfo_data/previous_rqinfo_data.json'
    try:
        with open(file_path, 'w') as file_handle:
            file_handle.write(json_text)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as file_handle:
            file_handle.write(json_text)
    except Exception as error:
        log.exception('problem saving rqinfo data; traceback follows')
        raise QueueCheckerError(f'problem saving rqinfo data; error, ``{error!r}``') from error
    log.debug('rqinfo data saved')
