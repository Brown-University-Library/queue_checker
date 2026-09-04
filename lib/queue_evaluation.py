"""
Compares observed queue data with expectations and evaluates alerting checks.
"""

import logging

log = logging.getLogger(__name__)


def compare_queue_data(expectations_dct: dict, data_dct: dict) -> dict:
    """
    Compares expected names with queues registered by prior enqueue activity.

    >>> details = compare_queue_data(
    ...     {'expected_queues': ['q1', 'q2', 'q3']},
    ...     {'queues': ['q1', 'q3', 'extra']},
    ... )
    >>> details
    {'expected': ['q1', 'q2', 'q3'], 'registered_expected': ['q1', 'q3'], 'not_registered': ['q2'], 'additional': ['extra']}

    Called by: check_reports.build_check_summary() and check_reports.build_queue_registration_report().
    """
    expected_queues = list(expectations_dct['expected_queues'])
    registered_queues = list(data_dct['queues'])
    registered_expected = [queue for queue in expected_queues if queue in registered_queues]
    not_registered = [queue for queue in expected_queues if queue not in registered_queues]
    additional_queues = [queue for queue in registered_queues if queue not in expected_queues]
    details = {
        'expected': expected_queues,
        'registered_expected': registered_expected,
        'not_registered': not_registered,
        'additional': additional_queues,
    }
    return details


def compare_worker_data(expectations_dct: dict, data_dct: dict) -> dict:
    """
    Compares expected counts with independently collected worker subscriptions.

    An expected but unregistered queue is still evaluated from the worker inventory:

    >>> details = compare_worker_data(
    ...     {'expected_workers': [
    ...         {'queue': 'never_used', 'worker_count': 1},
    ...         {'queue': 'zero_expected', 'worker_count': 0},
    ...         {'queue': 'wrong', 'worker_count': 1},
    ...     ]},
    ...     {
    ...         'queues': ['wrong'],
    ...         'workers_by_queue': {
    ...             'never_used': ['server.1'],
    ...             'wrong': ['server.2', 'server.3'],
    ...         },
    ...     },
    ... )
    >>> [(item['queue'], item['found_count']) for item in details['matched']]
    [('never_used', 1), ('zero_expected', 0)]
    >>> [(item['queue'], item['found_count']) for item in details['mismatched']]
    [('wrong', 2)]
    >>> details['matched'][0]['registered']
    False

    Called by: evaluate_qdata(), check_reports.build_check_summary(), and
    check_reports.build_worker_subscription_report().
    """
    workers_by_queue = data_dct['workers_by_queue']
    registered_queues = set(data_dct['queues'])
    observations = []
    mismatched = []
    matched = []
    for expectation in expectations_dct['expected_workers']:
        queue = expectation['queue']
        expected_count = expectation['worker_count']
        workers = list(workers_by_queue.get(queue, []))
        detail = {
            'queue': queue,
            'expected_count': expected_count,
            'found_count': len(workers),
            'workers': workers,
            'registered': queue in registered_queues,
        }
        observations.append(detail)
        if detail['found_count'] == expected_count:
            matched.append(detail)
        else:
            mismatched.append(detail)
    details = {'observations': observations, 'mismatched': mismatched, 'matched': matched}
    return details


def find_unserved_queues(data_dct: dict) -> list:
    """
    Finds ordinary queues with waiting jobs and no worker subscriptions.

    >>> find_unserved_queues(
    ...     {
    ...         'job_counts_by_queue': {'waiting': 2, 'served': 1, 'failed': 9},
    ...         'workers_by_queue': {'served': ['server.1']},
    ...     }
    ... )
    [{'queue': 'waiting', 'job_count': 2}]

    Called by: evaluate_qdata(), check_reports.build_check_summary(),
    check_reports.build_worker_subscription_report(), and check_reports.build_alert_reasons().
    """
    unserved_queues = []
    workers_by_queue = data_dct['workers_by_queue']
    for queue_name, job_count in data_dct['job_counts_by_queue'].items():
        if queue_name == 'failed' or job_count <= 0:
            continue
        if not workers_by_queue.get(queue_name):
            unserved_queues.append({'queue': queue_name, 'job_count': job_count})
    return unserved_queues


def evaluate_qdata(previous_failed_count: int, expectations: dict, data_dct: dict) -> dict:
    """
    Evaluates the two alerting checks after complete RQ data collection.

    Queue registration does not affect the result:

    >>> expectations_data = {
    ...     'expected_queues': ['never_used', 'failed'],
    ...     'expected_workers': [
    ...         {'queue': 'never_used', 'worker_count': 1},
    ...         {'queue': 'failed', 'worker_count': 0},
    ...     ],
    ...     'surge_failure_limit': 0,
    ... }
    >>> rqinfo_data = {
    ...     'failed_count': 4,
    ...     'queues': ['failed'],
    ...     'job_counts_by_queue': {'failed': 4},
    ...     'workers_by_queue': {'never_used': ['server.1']},
    ... }
    >>> evaluate_qdata(4, expectations_data, rqinfo_data)
    {'worker_check': 'ok', 'failure_queue_check': 'ok'}

    Missing subscriptions and an excessive failed-job increase both alert:

    >>> rqinfo_data['workers_by_queue'] = {}
    >>> rqinfo_data['failed_count'] = 5
    >>> rqinfo_data['job_counts_by_queue']['failed'] = 5
    >>> evaluate_qdata(4, expectations_data, rqinfo_data)
    {'worker_check': 'FAIL', 'failure_queue_check': 'FAIL'}

    Waiting work without a listener alerts even when configured counts match:

    >>> rqinfo_data = {
    ...     'failed_count': 4,
    ...     'queues': ['orphaned', 'failed'],
    ...     'job_counts_by_queue': {'orphaned': 2, 'failed': 4},
    ...     'workers_by_queue': {'never_used': ['server.1']},
    ... }
    >>> evaluate_qdata(4, expectations_data, rqinfo_data)
    {'worker_check': 'FAIL', 'failure_queue_check': 'ok'}

    Called by: queue_check.run_code().
    """
    assert type(previous_failed_count) == int
    assert type(expectations) == dict
    assert type(data_dct) == dict
    worker_details = compare_worker_data(expectations, data_dct)
    unserved_queues = find_unserved_queues(data_dct)
    worker_problem_count = len(worker_details['mismatched']) + len(unserved_queues)
    worker_result = 'FAIL' if worker_problem_count else 'ok'
    log.debug(f'worker problem count, ``{worker_problem_count}``')

    failure_increase = data_dct['failed_count'] - previous_failed_count
    log.debug(f'failure_increase, ``{failure_increase}``')
    surge_failure_limit = expectations['surge_failure_limit']
    log.debug(f'surge_failure_limit, ``{surge_failure_limit}``')
    failure_queue_result = 'FAIL' if failure_increase > surge_failure_limit else 'ok'

    checks_result = {
        'worker_check': worker_result,
        'failure_queue_check': failure_queue_result,
    }
    log.debug(f'checks_result, ``{checks_result}``')
    return checks_result
