"""
Collects, parses, loads, and saves queue data.
"""

import json
import logging
import os
import pprint
import subprocess

from lib.errors import QueueCheckerError

log = logging.getLogger(__name__)


def load_previous_rqinfo_data(current_rqinfo_data: dict) -> dict:
    """
    Loads previous rqinfo data from file.
    Called by: queue_check.run_code().
    On failure, saves current data to file, and returns current-data.
        - This enables a smooth first run of the script.
    """
    try:
        with open('../previous_rqinfo_data/previous_rqinfo_data.json', 'r') as file_handle:
            previous_rqinfo_data = json.loads(file_handle.read())
        assert type(previous_rqinfo_data) == dict
        log.debug(f'previous_rqinfo_data, loaded from file, ``{pprint.pformat(previous_rqinfo_data)}``')
    ## Any load, parse, or validation failure uses current data so the first run can continue.
    except Exception as error:  # noqa: BLE001
        log.warning(f'exception loading previous data; err, ``{error}``; will save existing data.')
        save_rqinfo_data(current_rqinfo_data)
        previous_rqinfo_data = current_rqinfo_data
        log.debug(f'previous_rqinfo_data, from current data, ``{pprint.pformat(previous_rqinfo_data)}``')
    return previous_rqinfo_data


def get_rqinfo() -> str:
    """
    Runs `rqinfo`, returns output.
    Called by: queue_check.run_code().
    - `--by-queue` returns the normal queue output, but shows workers associated with each queue.
    - `--raw` doesn't return the summary line or the job-bar, just the basic data.
    """
    result = subprocess.run(['rqinfo', '--by-queue', '--raw'], stdout=subprocess.PIPE, check=False)
    output = result.stdout.decode()
    assert type(output) == str
    log.debug(f'output, ``{output}``')
    return output


def parse_rqinfo(rq_output: str) -> dict:
    """
    Parses rqinfo output into a dict.
    Called by: queue_check.run_code().

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
    >>> pprint.pprint(result)
    {'failed_count': 333,
     'queues': ['q_1', 'q_2', 'failed'],
     'workers_by_queue': {'failed': [],
                          'q_1': ['server.968', 'server.952'],
                          'q_2': ['server.952']}}
    """
    lines = rq_output.split('\n')
    log.debug(f'lines, ``{lines}``')
    output = {'failed_count': 0, 'queues': [], 'workers_by_queue': {}}
    for line in lines:
        log.debug(f'processing line, ``{line}``')
        line = line.strip()
        if line == '':
            log.debug('blank line; continuing')
            continue
        if line.startswith('queue'):  # Line format: queue <queue_name> <count>
            (_, queue_name, count) = line.split()
            output['queues'].append(queue_name)
            if queue_name == 'failed':
                output['failed_count'] = int(count)
        else:  # Line format: <queue_name>: <worker.123 (idle), worker.124 (idle)> ...or...
            #                    failed: –
            (queue_name, worker_data) = line.split(':')
            worker_data = worker_data.strip()
            worker_names = []
            if worker_data != '–':  # Split by comma and get the worker name from each part
                worker_names = [part.split()[0] for part in worker_data.split(',')]
            output['workers_by_queue'][queue_name] = worker_names
    log.debug(f'output, ``{pprint.pformat(output)}``')
    return output


def save_rqinfo_data(data_dct: dict) -> None:
    """
    Saves rqinfo data to file.
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
