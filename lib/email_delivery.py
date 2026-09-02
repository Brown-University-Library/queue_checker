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
    build_check_summary,
    build_queue_check_report,
    build_worker_check_report,
)
from lib.errors import QueueCheckerError
from lib.failed_job_reports import build_failure_queue_check_report
from lib.report_formatting import format_datetime

log = logging.getLogger(__name__)


def build_email_message(new_failures, previous_failure_count, expectations_dct, evaluation_dct, data_dct) -> str:
    """
    Assembles a plain-text email message with labeled check comparisons.
    Called by: queue_check.run_code().

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
    timestamp = format_datetime(datetime.datetime.now().astimezone())
    with open('email_template.txt', 'r') as file_handle:
        template = Template(file_handle.read())
        result = template.safe_substitute(
            {
                'server': socket.gethostname().upper(),
                'timestamp': timestamp,
                'overall_result': overall_result,
                'check_summary': build_check_summary(previous_failure_count, expectations_dct, evaluation_dct, data_dct),
                'failure_queue_report': build_failure_queue_check_report(
                    new_failures, previous_failure_count, expectations_dct, evaluation_dct, data_dct
                ),
                'queue_report': build_queue_check_report(expectations_dct, evaluation_dct, data_dct),
                'worker_report': build_worker_check_report(expectations_dct, evaluation_dct, data_dct),
            }
        )
        message = result.rstrip() + '\n'
    log.debug(f'email message character count, ``{len(message)}``')
    return message


def send_email(message: str) -> None:
    """
    Sends mail and raises an error that the cron job can report when delivery fails.
    Called by: queue_check.run_code().
    """
    assert type(message) == str, type(message)
    log.debug(f'message, ``{message}``')
    email_host = os.environ['QCHKR__EMAIL_HOST']
    email_port = int(os.environ['QCHKR__EMAIL_HOST_PORT'])
    email_from = os.environ['QCHKR__EMAIL_FROM']
    email_recipients = json.loads(os.environ['QCHKR__EMAIL_RECIPIENTS_JSON'])
    host = socket.gethostname()
    try:
        smtp_connection = smtplib.SMTP(email_host, email_port)
        email_message = MIMEText(message)
        email_message['Subject'] = f'queue-checker alert from ``{host.upper()}``'
        email_message['From'] = email_from
        email_message['To'] = ';'.join(email_recipients)
        smtp_connection.sendmail(email_from, email_recipients, email_message.as_string())
    except Exception as error:
        error_repr = repr(error)
        log.exception(f'Problem sending queue-checker mail, ``{error_repr}``')
        raise QueueCheckerError(error_repr) from error
