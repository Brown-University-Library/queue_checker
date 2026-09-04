"""
Builds queue-registration, worker-subscription, collection, and summary sections.
"""

from lib.queue_evaluation import compare_queue_data, compare_worker_data, find_unserved_queues
from lib.report_formatting import format_check_status, format_report_header


def build_check_summary(
    previous_failure_count: int,
    expectations_dct: dict,
    evaluation_dct: dict,
    data_dct: dict,
    failed_job_details: dict,
) -> str:
    """
    Builds the short comparison shown near the beginning of an alert.
    Called by: email_delivery.build_email_message().
    """
    queue_details = compare_queue_data(expectations_dct, data_dct)
    worker_details = compare_worker_data(expectations_dct, data_dct)
    unserved_queues = find_unserved_queues(data_dct)
    failure_change = data_dct['failed_count'] - previous_failure_count
    collection_status = 'PARTIAL' if failed_job_details['error'] else 'OK'
    summary_lines = [
        format_report_header('CHECK SUMMARY'),
        '',
        f'- Failed jobs: {format_check_status(evaluation_dct["failure_queue_check"])}',
        f'  Expectation: Increase must not exceed {expectations_dct["surge_failure_limit"]}',
        f'  Previous: {previous_failure_count}',
        f'  Current: {data_dct["failed_count"]}',
        f'  Change: {failure_change}',
        '',
        f'- Worker subscriptions: {format_check_status(evaluation_dct["worker_check"])}',
        f'  Expected queue entries: {len(expectations_dct["expected_workers"])}',
        f'  Matching worker counts: {len(worker_details["matched"])}',
        f'  Incorrect worker counts: {len(worker_details["mismatched"])}',
        f'  Queues with waiting jobs and no subscribers: {len(unserved_queues)}',
        '',
        '- Queue registration: INFORMATIONAL',
        f'  Expected queue names: {len(queue_details["expected"])}',
        f'  Registered expected names: {len(queue_details["registered_expected"])}',
        f'  Expected names not registered yet: {len(queue_details["not_registered"])}',
        f'  Additional registered names: {len(queue_details["additional"])}',
        '',
        f'- Data collection: {collection_status}',
    ]
    summary = '\n'.join(summary_lines)
    return summary


def append_name_list(lines: list, heading: str, names: list) -> None:
    """
    Appends a heading and a simple bullet list, including an explicit None entry.
    Called by: build_queue_registration_report().
    """
    lines.extend(['', heading])
    if names:
        lines.extend([f'- {name}' for name in names])
    else:
        lines.append('- None')


def build_queue_registration_report(expectations_dct: dict, data_dct: dict) -> str:
    """
    Builds the informational comparison of expected and registered queue names.

    >>> report = build_queue_registration_report(
    ...     {'expected_queues': ['q1', 'never_used']},
    ...     {'queues': ['q1', 'extra']},
    ... )
    >>> 'Expected names not registered yet:\\n- never_used' in report
    True
    >>> 'Additional registered names:\\n- extra' in report
    True
    >>> 'Queue registration did not cause this alert.' in report
    True

    Called by: email_delivery.build_email_message().
    """
    details = compare_queue_data(expectations_dct, data_dct)
    lines = [
        format_report_header('QUEUE REGISTRATION: INFORMATIONAL'),
    ]
    append_name_list(lines, 'Expected queue names:', details['expected'])
    append_name_list(lines, 'Expected names not registered yet:', details['not_registered'])
    append_name_list(lines, 'Registered expected names:', details['registered_expected'])
    append_name_list(lines, 'Additional registered names:', details['additional'])
    lines.extend(
        [
            '',
            'An expected queue may be unregistered simply because it has not received a job yet.',
            'Queue registration did not cause this alert.',
        ]
    )
    report = '\n'.join(lines)
    return report


def append_worker_names(lines: list, workers: list) -> None:
    """
    Appends worker identifiers to a detailed worker-expectation block.
    Called by: build_worker_subscription_report().
    """
    lines.append('  Workers found:')
    if workers:
        lines.extend([f'  - {worker}' for worker in workers])
    else:
        lines.append('  - None')


def append_worker_detail(lines: list, detail: dict) -> None:
    """
    Appends one labeled worker expectation and observation.
    Called by: build_worker_subscription_report().
    """
    registered_text = 'Yes' if detail['registered'] else 'No'
    lines.extend(
        [
            f'- Queue: {detail["queue"]}',
            f'  Expected active workers: {detail["expected_count"]}',
            f'  Found active workers: {detail["found_count"]}',
            f'  Registered by a prior enqueue: {registered_text}',
        ]
    )
    append_worker_names(lines, detail['workers'])


def build_worker_subscription_report(expectations_dct: dict, evaluation_dct: dict, data_dct: dict) -> str:
    """
    Builds the complete active-worker subscription comparison.

    >>> report = build_worker_subscription_report(
    ...     {
    ...         'expected_workers': [
    ...             {'queue': 'missing', 'worker_count': 1},
    ...             {'queue': 'matching', 'worker_count': 1},
    ...         ],
    ...     },
    ...     {'worker_check': 'FAIL'},
    ...     {
    ...         'queues': ['matching'],
    ...         'job_counts_by_queue': {'matching': 0},
    ...         'workers_by_queue': {'matching': ['server.1']},
    ...     },
    ... )
    >>> 'Queue: missing\\n  Expected active workers: 1\\n  Found active workers: 0' in report
    True
    >>> 'Worker counts that matched:\\n- Queue: matching' in report
    True

    Called by: email_delivery.build_email_message().
    """
    details = compare_worker_data(expectations_dct, data_dct)
    unserved_queues = find_unserved_queues(data_dct)
    lines = [
        format_report_header(f'WORKER SUBSCRIPTION CHECK: {format_check_status(evaluation_dct["worker_check"])}'),
        '',
        'Worker subscription expectations and observations:',
    ]
    for detail in details['observations']:
        lines.extend(
            [
                f'- Queue: {detail["queue"]}',
                f'  Expected active workers: {detail["expected_count"]}',
                f'  Found active workers: {detail["found_count"]}',
            ]
        )
    lines.extend(['', 'Worker-count mismatches:'])
    if details['mismatched']:
        for detail in details['mismatched']:
            append_worker_detail(lines, detail)
    else:
        lines.append('- None')
    lines.extend(['', 'Queues with waiting jobs and no active subscribers:'])
    if unserved_queues:
        for detail in unserved_queues:
            lines.extend(
                [
                    f'- Queue: {detail["queue"]}',
                    f'  Waiting jobs: {detail["job_count"]}',
                ]
            )
    else:
        lines.append('- None')
    lines.extend(['', 'Worker counts that matched:'])
    if details['matched']:
        for detail in details['matched']:
            append_worker_detail(lines, detail)
    else:
        lines.append('- None')
    report = '\n'.join(lines)
    return report


def build_data_collection_report(failed_job_details: dict) -> str:
    """
    Reports successful core inspection and any failed-job detail limitation.
    Called by: email_delivery.build_email_message().
    """
    collection_status = 'PARTIAL' if failed_job_details['error'] else 'OK'
    if failed_job_details['error']:
        detail_status = 'No'
    elif failed_job_details['requested']:
        detail_status = 'Yes'
    else:
        detail_status = 'Not requested'
    lines = [
        format_report_header(f'DATA COLLECTION: {collection_status}'),
        '',
        '- Queue information collected: Yes',
        '- Worker information collected: Yes',
        '- Failed-job count collected: Yes',
        f'- Selected failed-job details collected: {detail_status}',
    ]
    if failed_job_details['error']:
        lines.append(f'- Failed-job detail error: {failed_job_details["error"]}')
    report = '\n'.join(lines)
    return report


def build_suggested_verification(expectations_dct: dict) -> str:
    """
    Builds copyable read-only commands for checking the alert directly.
    Called by: email_delivery.build_email_message().
    """
    expected_queue_arguments = ' '.join(expectations_dct['expected_queues'])
    lines = [
        format_report_header('SUGGESTED VERIFICATION'),
        '',
        '1. Inspect every active worker and its declared queues:',
        '   uv run --no-sync rqinfo --only-workers --raw',
        '',
        '2. Inspect the expected queues explicitly:',
        f'   uv run --no-sync rqinfo --by-queue --raw {expected_queue_arguments}',
    ]
    report = '\n'.join(lines)
    return report


def build_alert_reasons(evaluation_dct: dict, data_dct: dict, expectations_dct: dict, failed_job_details: dict) -> str:
    """
    Builds a concise list of the conditions responsible for the alert.
    Called by: email_delivery.build_email_message().
    """
    worker_details = compare_worker_data(expectations_dct, data_dct)
    unserved_queues = find_unserved_queues(data_dct)
    reasons = []
    if evaluation_dct['failure_queue_check'] == 'FAIL':
        reasons.append('The failed-job increase exceeded the configured allowance.')
    mismatch_count = len(worker_details['mismatched'])
    if mismatch_count:
        label = 'queue had' if mismatch_count == 1 else 'queues had'
        reasons.append(f'{mismatch_count} {label} incorrect active-worker subscription counts.')
    unserved_count = len(unserved_queues)
    if unserved_count:
        label = 'queue had' if unserved_count == 1 else 'queues had'
        reasons.append(f'{unserved_count} ordinary {label} waiting jobs and no active subscribers.')
    if failed_job_details['error']:
        reasons.append('Selected failed-job details could not be loaded.')
    lines = [format_report_header('ALERT REASONS'), '']
    lines.extend([f'- {reason}' for reason in reasons])
    report = '\n'.join(lines)
    return report
