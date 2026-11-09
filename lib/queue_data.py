"""
Collects, parses, loads, and saves queue data.
"""

import base64
import binascii
import json
import logging
import os
import pprint
import subprocess
import zlib
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional

from redis import Redis
from redis.exceptions import RedisError
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


def get_failed_registry_queues(expectations: dict) -> list:
    """
    Reads the optional list of queues whose RQ 1.x failed-job registries are counted.

    >>> get_failed_registry_queues({'failed_registry_queues_example': 1})
    []
    >>> get_failed_registry_queues({'failed_registry_queues': ['create_jp2', 'pdf_to_set']})
    ['create_jp2', 'pdf_to_set']
    >>> get_failed_registry_queues({'failed_registry_queues': []})
    []
    >>> get_failed_registry_queues({'failed_registry_queues': None})
    []
    >>> get_failed_registry_queues({'failed_registry_queues': ''})
    Traceback (most recent call last):
      ...
    lib.errors.QueueCheckerError: Expectation failed_registry_queues must be a list of non-empty queue names.
    >>> get_failed_registry_queues({'failed_registry_queues': 'create_jp2'})
    Traceback (most recent call last):
      ...
    lib.errors.QueueCheckerError: Expectation failed_registry_queues must be a list of non-empty queue names.

    Called by: collect_rq_data().
    """
    queues = expectations.get('failed_registry_queues')
    if queues is None:
        queues = []
    if not isinstance(queues, list) or not all(isinstance(queue, str) and queue for queue in queues):
        raise QueueCheckerError('Expectation failed_registry_queues must be a list of non-empty queue names.')
    result = list(dict.fromkeys(queues))
    return result


def get_failed_registry_counts(queues: list) -> dict:
    """
    Counts the jobs in each queue's RQ 1.x failed-job registry, which is the sorted set ``rq:failed:<queue>``.

    >>> from unittest.mock import Mock, patch
    >>> connection = Mock()
    >>> connection.zcard.side_effect = [2, 0]
    >>> with patch('lib.queue_data.Redis', return_value=connection):
    ...     get_failed_registry_counts(['create_jp2', 'pdf_to_set'])
    {'create_jp2': 2, 'pdf_to_set': 0}
    >>> get_failed_registry_counts([])
    {}
    >>> connection.zcard.side_effect = RedisError('boom')
    >>> with patch('lib.queue_data.Redis', return_value=connection):
    ...     get_failed_registry_counts(['create_jp2'])
    Traceback (most recent call last):
      ...
    lib.errors.QueueCheckerError: Unable to collect failed-job registry counts; RedisError: boom

    Called by: collect_rq_data().
    """
    counts = {}
    if queues:
        try:
            connection = Redis('localhost')
            for queue in queues:
                counts[queue] = int(connection.zcard(f'rq:failed:{queue}'))
        except (RedisError, TypeError, ValueError) as error:
            raise QueueCheckerError(
                f'Unable to collect failed-job registry counts; {type(error).__name__}: {error}'
            ) from error
    log.debug(f'failed_registry_counts, ``{counts}``')
    return counts


def collect_rq_data(expectations: Optional[dict] = None) -> dict:  # noqa: FA100 -- keep Python 3.8-compatible union syntax
    """
    Collects and combines independent queue, worker, and failed-registry inspection results.
    Called by: queue_check.run_code().
    """
    registry_queues = get_failed_registry_queues(expectations or {})
    queue_output = get_queue_rqinfo()
    worker_output = get_worker_rqinfo()
    queue_data = parse_queue_rqinfo(queue_output)
    worker_data = parse_worker_rqinfo(worker_output)
    data = dict(queue_data)
    data.update(worker_data)
    data['failed_registry_counts'] = get_failed_registry_counts(registry_queues)
    return data


def decode_exc_info(raw_value: Optional[bytes]) -> Optional[str]:  # noqa: FA100 -- keep Python 3.8-compatible union syntax
    """
    Decodes an RQ job's ``exc_info``, which RQ 1.x zlib-compresses, falling back to the raw value.

    >>> decode_exc_info(zlib.compress(b'ValueError: compressed'))
    'ValueError: compressed'
    >>> decode_exc_info(b'ValueError: plain')
    'ValueError: plain'
    >>> decode_exc_info(None) is None
    True

    Called by: load_registry_job().
    """
    result = None
    if raw_value:
        try:
            result = zlib.decompress(raw_value).decode(errors='replace')
        except zlib.error:
            result = raw_value.decode(errors='replace')
    return result


def parse_rq_timestamp(raw_value: Optional[bytes]) -> Optional[datetime]:  # noqa: FA100 -- keep Python 3.8-compatible union syntax
    """
    Parses an RQ 1.x ISO timestamp from a job hash into an aware UTC datetime.

    >>> parse_rq_timestamp(b'2026-01-02T03:04:05.123456Z')
    datetime.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone.utc)
    >>> parse_rq_timestamp(b'2026-01-02T03:04:05Z')
    datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    >>> parse_rq_timestamp(b'garbage') is None
    True

    Called by: load_registry_job().
    """
    result = None
    if raw_value:
        text = raw_value.decode(errors='replace')
        for time_format in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
            try:
                result = datetime.strptime(text, time_format).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
    return result


def load_result_exc_info(connection, job_id: str) -> Optional[str]:  # noqa: FA100 -- keep Python 3.8-compatible union syntax
    """
    Reads exception text from the newest failed entry in RQ 1.x's ``rq:results:<job_id>`` stream.
    On Redis server 5+, RQ 1.12+ stores ``exc_info`` there (base64 of zlib) instead of in the job hash.

    >>> from unittest.mock import Mock
    >>> connection = Mock()
    >>> encoded = base64.b64encode(zlib.compress(b'ValueError: streamed'))
    >>> connection.xrevrange.return_value = [(b'1-0', {b'type': b'2', b'exc_string': encoded})]
    >>> load_result_exc_info(connection, 'abc')
    'ValueError: streamed'
    >>> connection.xrevrange.assert_called_once_with('rq:results:abc', count=10)
    >>> connection.xrevrange.return_value = [(b'1-0', {b'type': b'1'})]
    >>> load_result_exc_info(connection, 'abc') is None
    True

    Called by: load_registry_job().
    """
    result = None
    for _entry_id, payload in connection.xrevrange(f'rq:results:{job_id}', count=10):
        if payload.get(b'type') == b'2' and payload.get(b'exc_string'):
            try:
                result = zlib.decompress(base64.b64decode(payload[b'exc_string'])).decode(errors='replace')
            except (binascii.Error, zlib.error):
                result = None
            break
    return result


def load_registry_job(connection, job_id: str, queue: str) -> SimpleNamespace:
    """
    Loads one failed registry job from its raw hash, avoiding rq 0.13 decoding of RQ 1.x data.

    >>> from unittest.mock import Mock
    >>> connection = Mock()
    >>> connection.hgetall.return_value = {
    ...     b'origin': b'create_jp2',
    ...     b'description': b'tasks.make_jp2(1)',
    ...     b'enqueued_at': b'2026-01-02T03:04:05.000000Z',
    ...     b'ended_at': b'2026-01-02T03:04:06.000000Z',
    ...     b'exc_info': zlib.compress(b'ValueError: bad'),
    ... }
    >>> job = load_registry_job(connection, 'abc', 'create_jp2')
    >>> (job.id, job.origin, job.description, job.exc_info)
    ('abc', 'create_jp2', 'tasks.make_jp2(1)', 'ValueError: bad')
    >>> connection.hgetall.assert_called_once_with('rq:job:abc')

    Called by: get_registry_failed_jobs().
    """
    fields = connection.hgetall(f'rq:job:{job_id}') or {}
    origin = fields.get(b'origin')
    description = fields.get(b'description')
    job = SimpleNamespace(
        id=job_id,
        origin=origin.decode(errors='replace') if origin else queue,
        description=description.decode(errors='replace') if description else None,
        enqueued_at=parse_rq_timestamp(fields.get(b'enqueued_at')),
        started_at=parse_rq_timestamp(fields.get(b'started_at')),
        ended_at=parse_rq_timestamp(fields.get(b'ended_at')),
        exc_info=decode_exc_info(fields.get(b'exc_info')) or load_result_exc_info(connection, job_id),
    )
    return job


def get_registry_failed_jobs(connection, queue: str, count: int) -> list:
    """
    Loads the newest failed jobs from a queue's registry; the score is the expiry time, so highest is newest.

    >>> from unittest.mock import Mock
    >>> connection = Mock()
    >>> connection.zrevrange.return_value = [b'job-2']
    >>> connection.hgetall.return_value = {b'origin': b'create_jp2'}
    >>> connection.xrevrange.return_value = []
    >>> [job.id for job in get_registry_failed_jobs(connection, 'create_jp2', 1)]
    ['job-2']
    >>> connection.zrevrange.assert_called_once_with('rq:failed:create_jp2', 0, 0)

    Called by: get_failed_job_details().
    """
    job_ids = connection.zrevrange(f'rq:failed:{queue}', 0, count - 1)
    jobs = [load_registry_job(connection, job_id.decode(errors='replace'), queue) for job_id in job_ids]
    return jobs


def get_failed_job_details(failure_increase: int, registry_increases: Optional[dict] = None) -> dict:  # noqa: FA100 -- keep Python 3.8-compatible union syntax
    """
    Loads selected failed jobs without hiding a successful failed-count check when job details cannot be loaded.
    The shared failed queue supplies `failure_increase` jobs; each failed registry supplies its own increase.

    >>> get_failed_job_details(0)
    {'requested': False, 'jobs': [], 'error': None}
    >>> get_failed_job_details(0, {'create_jp2': 0})
    {'requested': False, 'jobs': [], 'error': None}

    Called by: queue_check.run_code().
    """
    details = {'requested': False, 'jobs': [], 'error': None}
    registry_increases = registry_increases or {}
    positive_registry_increases = {queue: count for queue, count in registry_increases.items() if count > 0}
    if failure_increase > 0 or positive_registry_increases:
        details['requested'] = True
        try:
            connection = Redis('localhost')
            if failure_increase > 0:
                details['jobs'] = list(get_failed_queue(connection=connection).jobs[-failure_increase:])
            for queue, count in positive_registry_increases.items():
                details['jobs'].extend(get_registry_failed_jobs(connection, queue, count))
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
