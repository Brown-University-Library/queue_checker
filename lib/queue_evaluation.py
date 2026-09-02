"""
Compares observed queue data with expectations and evaluates the checks.
"""

import logging

from redis import Redis
from rq import get_failed_queue

log = logging.getLogger(__name__)


def compare_queue_data(expectations_dct: dict, data_dct: dict) -> dict:
    """
    Compares expected queues with queues found by rqinfo.
    Called by: evaluate_qdata(), check_reports.build_check_summary(), and check_reports.build_queue_check_report().

    >>> details = compare_queue_data(
    ...     {'expected_queues': ['q1', 'q2', 'q3']},
    ...     {'queues': ['q1', 'q3', 'extra']},
    ... )
    >>> details
    {'expected': ['q1', 'q2', 'q3'], 'found_expected': ['q1', 'q3'], 'missing': ['q2'], 'additional': ['extra']}
    """
    expected_queues = list(expectations_dct['expected_queues'])
    found_queues = list(data_dct['queues'])
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


def compare_worker_data(expectations_dct: dict, data_dct: dict) -> dict:
    """
    Compares expected worker counts with worker data found by rqinfo.
    Called by: evaluate_qdata(), check_reports.build_check_summary(), and check_reports.build_worker_check_report().

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
            unavailable.append(
                {
                    'queue': queue,
                    'expected_count': expected_count,
                    'found_count': None,
                    'workers': [],
                }
            )
            continue
        workers = list(workers_by_queue[queue])
        detail = {
            'queue': queue,
            'expected_count': expected_count,
            'found_count': len(workers),
            'workers': workers,
        }
        if detail['found_count'] == expected_count:
            matched.append(detail)
        else:
            mismatched.append(detail)
    details = {'unavailable': unavailable, 'mismatched': mismatched, 'matched': matched}
    return details


def evaluate_qdata(previous_failed_count: int, expectations: dict, data_dct: dict) -> tuple:
    """
    Evaluates rqinfo output against expectation data.
    Called by: queue_check.run_code().

    Example -- all ok:
    >>> previous_failed_count = 10
    >>> expectations_data = {'expected_queues': ['q1', 'q2'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}], 'surge_failure_limit': 10}
    >>> rqinfo_data = {'failed_count': 15, 'queues': ['q1', 'q2', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'q2': ['server.234'], 'failed': []}}
    >>> result, new_failures = evaluate_qdata(previous_failed_count, expectations_data, rqinfo_data)
    >>> result
    {'queue_check': 'ok', 'worker_check': 'ok', 'failure_queue_check': 'ok'}

    Example -- problem:
    >>> previous_failed_count = 10
    >>> expectations_data = {'expected_queues': ['q1', 'q2', 'q3'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}, {'queue': 'q2', 'worker_count': 1}], 'surge_failure_limit': 10}
    >>> rqinfo_data = {'failed_count': 30, 'queues': ['q1', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'failed': []}}
    >>> result, new_failures = evaluate_qdata(previous_failed_count, expectations_data, rqinfo_data)
    >>> result
    {'queue_check': 'FAIL', 'worker_check': 'FAIL', 'failure_queue_check': 'FAIL'}
    """
    assert type(previous_failed_count) == int
    assert type(expectations) == dict
    assert type(data_dct) == dict
    checks_result = {'queue_check': 'init', 'worker_check': 'init', 'failure_queue_check': 'init'}

    queue_details = compare_queue_data(expectations, data_dct)
    checks_result['queue_check'] = 'FAIL' if queue_details['missing'] else 'ok'
    log.debug(f'missing queue count, ``{len(queue_details["missing"])}``')
    log.debug(f'after queue-check, checks_result, ``{checks_result}``')

    worker_details = compare_worker_data(expectations, data_dct)
    worker_problem_count = len(worker_details['unavailable']) + len(worker_details['mismatched'])
    checks_result['worker_check'] = 'FAIL' if worker_problem_count else 'ok'
    log.debug(f'worker problem count, ``{worker_problem_count}``')
    log.debug(f'after worker-check, checks_result, ``{checks_result}``')

    failure_increase = data_dct['failed_count'] - previous_failed_count
    log.debug(f'failure_increase, ``{failure_increase}``')
    surge_failure_limit = expectations['surge_failure_limit']
    log.debug(f'surge_failure_limit, ``{surge_failure_limit}``')
    if failure_increase > surge_failure_limit:
        log.debug('failure-increase exceeded expectation-settings-limit')
        checks_result['failure_queue_check'] = 'FAIL'
        new_failures = get_failed_queue(connection=Redis('localhost')).jobs[-failure_increase:]
    else:
        checks_result['failure_queue_check'] = 'ok'
        new_failures = []
    log.debug(f'checks_result, ``{checks_result}``')
    return checks_result, new_failures
