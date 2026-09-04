"""
Builds and sends queue-checker alert emails.
"""

import datetime
import json
import logging
import os
import smtplib
import socket
from email.mime.text import MIMEText
from string import Template

from lib.check_reports import (
    build_alert_reasons,
    build_check_summary,
    build_data_collection_report,
    build_queue_registration_report,
    build_suggested_verification,
    build_worker_subscription_report,
)
from lib.errors import QueueCheckerError
from lib.failed_job_reports import build_failure_queue_check_report
from lib.report_formatting import format_datetime, format_report_header

log = logging.getLogger(__name__)


def build_email_message(
    failed_job_details: dict,
    previous_failure_count: int,
    expectations_dct: dict,
    evaluation_dct: dict,
    data_dct: dict,
) -> str:
    """
    Assembles a complete plain-text alert with two alerting checks and queue-registration information.

    >>> message = build_email_message(
    ...     {'requested': False, 'jobs': [], 'error': None},
    ...     4,
    ...     {
    ...         'expected_queues': ['q1', 'never_used'],
    ...         'expected_workers': [
    ...             {'queue': 'q1', 'worker_count': 1},
    ...             {'queue': 'never_used', 'worker_count': 1},
    ...         ],
    ...         'surge_failure_limit': 0,
    ...     },
    ...     {'worker_check': 'FAIL', 'failure_queue_check': 'ok'},
    ...     {
    ...         'failed_count': 4,
    ...         'queues': ['q1'],
    ...         'job_counts_by_queue': {'q1': 0},
    ...         'workers_by_queue': {'q1': ['server.1']},
    ...     },
    ... )
    >>> message.startswith('QUEUE CHECKER ALERT\\n')
    True
    >>> 'Overall result: 1 of 2 alerting checks failed' in message
    True
    >>> 'QUEUE REGISTRATION: INFORMATIONAL' in message
    True
    >>> 'Expected names not registered yet:\\n- never_used' in message
    True

    Called by: queue_check.run_code().
    """
    assert type(evaluation_dct) == dict
    assert type(data_dct) == dict
    failed_check_count = sum(1 for result in evaluation_dct.values() if result == 'FAIL')
    overall_result = f'{failed_check_count} of {len(evaluation_dct)} alerting checks failed'
    timestamp = format_datetime(datetime.datetime.now().astimezone())
    with open('email_template.txt', 'r') as file_handle:
        template = Template(file_handle.read())
        result = template.safe_substitute(
            {
                'server': socket.gethostname().upper(),
                'timestamp': timestamp,
                'overall_result': overall_result,
                'check_summary': build_check_summary(
                    previous_failure_count,
                    expectations_dct,
                    evaluation_dct,
                    data_dct,
                    failed_job_details,
                ),
                'failure_queue_report': build_failure_queue_check_report(
                    failed_job_details,
                    previous_failure_count,
                    expectations_dct,
                    evaluation_dct,
                    data_dct,
                ),
                'worker_report': build_worker_subscription_report(expectations_dct, evaluation_dct, data_dct),
                'queue_registration_report': build_queue_registration_report(expectations_dct, data_dct),
                'data_collection_report': build_data_collection_report(failed_job_details),
                'suggested_verification': build_suggested_verification(expectations_dct),
                'alert_reasons': build_alert_reasons(
                    evaluation_dct,
                    data_dct,
                    expectations_dct,
                    failed_job_details,
                ),
            }
        )
        message = result.rstrip() + '\n'
    log.debug(f'email message character count, ``{len(message)}``')
    return message


def build_collection_error_message(error_message: str) -> str:
    """
    Builds an alert that clearly withholds queue and worker conclusions when RQ inspection is incomplete.

    >>> message = build_collection_error_message('Worker information is incomplete; example-worker')
    >>> message.startswith('QUEUE CHECKER ERROR\\n')
    True
    >>> 'No queue or worker health conclusions were inferred.' in message
    True
    >>> 'Worker information is incomplete; example-worker' in message
    True

    Called by: queue_check.run_code().
    """
    timestamp = format_datetime(datetime.datetime.now().astimezone())
    host = socket.gethostname().upper()
    error_detail = ' '.join(error_message.split())[:1000]
    lines = [
        'QUEUE CHECKER ERROR',
        '',
        f'Server: {host}',
        f'Checked: {timestamp}',
        'Overall result: Unable to inspect RQ safely',
        '',
        format_report_header('DATA COLLECTION: FAILED'),
        '',
        '- RQ inspection completed: No',
        f'- Error: {error_detail}',
        '',
        'No queue or worker health conclusions were inferred.',
        'The prior successful data file was not replaced.',
        '',
        format_report_header('SUGGESTED VERIFICATION'),
        '',
        '1. Inspect every active worker and its declared queues:',
        '   uv run --no-sync rqinfo --only-workers --raw',
        '',
        '2. Inspect registered queues and job counts:',
        '   uv run --no-sync rqinfo --by-queue --raw',
        '',
        '[END]',
    ]
    message = '\n'.join(lines) + '\n'
    return message


def send_email(message: str, collection_error: bool = False) -> None:
    """
    Sends an alert and raises an error that the scheduled caller can report when delivery fails.
    Called by: queue_check.deliver_alert().
    """
    assert type(message) == str, type(message)
    log.debug(f'message, ``{message}``')
    email_host = os.environ['QCHKR__EMAIL_HOST']
    email_port = int(os.environ['QCHKR__EMAIL_HOST_PORT'])
    email_from = os.environ['QCHKR__EMAIL_FROM']
    email_recipients = json.loads(os.environ['QCHKR__EMAIL_RECIPIENTS_JSON'])
    host = socket.gethostname().upper()
    if collection_error:
        subject = f'Queue checker error: unable to inspect RQ on {host}'
    else:
        subject = f'Queue checker alert from {host}'
    try:
        smtp_connection = smtplib.SMTP(email_host, email_port)
        email_message = MIMEText(message)
        email_message['Subject'] = subject
        email_message['From'] = email_from
        email_message['To'] = ';'.join(email_recipients)
        smtp_connection.sendmail(email_from, email_recipients, email_message.as_string())
    except Exception as error:
        error_repr = repr(error)
        log.exception(f'Problem sending queue-checker mail, ``{error_repr}``')
        raise QueueCheckerError(error_repr) from error
