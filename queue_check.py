"""
Coordinates the queue checks and delivers an alert or inspection error when attention is required.

Run from the repository root with: `uv run ./queue_check.py`
"""

import argparse
import logging
import pprint

from lib import email_delivery, queue_data, queue_evaluation, settings
from lib.errors import QueueCheckerError

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments.
    Called by: dunder-main.
    """
    parser = argparse.ArgumentParser(description='Checks expected RQ worker subscriptions and failed-job activity.')
    parser.add_argument(
        '--no-email',
        action='store_true',
        help='Prints the complete alert to stdout instead of sending email.',
    )
    args = parser.parse_args()
    return args


def deliver_alert(message: str, no_email: bool, collection_error: bool = False) -> None:
    """
    Prints an alert to stdout or sends it by email.

    >>> deliver_alert('Example alert\\n', no_email=True)
    Example alert

    Called by: run_code().
    """
    if no_email:
        print(message, end='')
    else:
        email_delivery.send_email(message=message, collection_error=collection_error)


def run_code(no_email: bool = False) -> None:
    """
    Runs the queue checks and delivers an alert when a check fails.
    Called by: dunder-main.
    """
    collection_error = None
    try:
        data_dct = queue_data.collect_rq_data()
        assert type(data_dct) == dict
    except QueueCheckerError as error:
        collection_error = str(error)

    if collection_error:
        message = email_delivery.build_collection_error_message(collection_error)
        deliver_alert(message, no_email, collection_error=True)
        log.error(f'RQ data collection failed, ``{collection_error}``')
    else:
        previous_rqinfo_data = queue_data.load_previous_rqinfo_data(data_dct)
        assert type(previous_rqinfo_data) == dict
        queue_data.save_rqinfo_data(data_dct)

        previous_failure_count = previous_rqinfo_data['failed_count']
        evaluation_dct = queue_evaluation.evaluate_qdata(
            previous_failure_count,
            settings.expectations,
            data_dct,
        )
        assert type(evaluation_dct) == dict

        failed_job_details = {'requested': False, 'jobs': [], 'error': None}
        if evaluation_dct['failure_queue_check'] == 'FAIL':
            failure_increase = data_dct['failed_count'] - previous_failure_count
            failed_job_details = queue_data.get_failed_job_details(failure_increase)

        all_checks_ok = {
            'worker_check': 'ok',
            'failure_queue_check': 'ok',
        }
        if evaluation_dct != all_checks_ok:
            message = email_delivery.build_email_message(
                failed_job_details,
                previous_failure_count,
                settings.expectations,
                evaluation_dct,
                data_dct,
            )
            deliver_alert(message, no_email)

        log.info(f'evaluation_dct, ``{pprint.pformat(evaluation_dct)}``')


if __name__ == '__main__':
    arguments = parse_args()
    run_code(no_email=arguments.no_email)
