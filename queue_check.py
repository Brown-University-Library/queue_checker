"""
Coordinates the queue checks and delivers an alert when a check fails.

Run from the repository root with: `uv run ./queue_check.py`
"""

import argparse
import logging
import pprint

from lib import email_delivery, queue_data, queue_evaluation, settings

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments.
    Called by: dunder-main.
    """
    parser = argparse.ArgumentParser(description='Checks expected RQ queues and workers.')
    parser.add_argument(
        '--no-email',
        action='store_true',
        help='Prints the complete alert to stdout instead of sending email.',
    )
    args = parser.parse_args()
    return args


def deliver_alert(message: str, no_email: bool) -> None:
    """
    Prints an alert to stdout or sends it by email.

    >>> deliver_alert('Example alert\\n', no_email=True)
    Example alert

    Called by: run_code().
    """
    if no_email:
        print(message, end='')
    else:
        email_delivery.send_email(message=message)


def run_code(no_email: bool = False) -> None:
    """
    Runs the queue checks and delivers an alert when a check fails.
    Called by: dunder-main.
    """
    output = queue_data.get_rqinfo()
    assert type(output) == str

    data_dct = queue_data.parse_rqinfo(output)
    assert type(data_dct) == dict

    previous_rqinfo_data = queue_data.load_previous_rqinfo_data(data_dct)
    assert type(previous_rqinfo_data) == dict
    queue_data.save_rqinfo_data(data_dct)

    previous_failure_count = previous_rqinfo_data['failed_count']
    evaluation_dct, new_failures = queue_evaluation.evaluate_qdata(
        previous_failure_count,
        settings.expectations,
        data_dct,
    )
    assert type(evaluation_dct) == dict

    all_checks_ok = {
        'queue_check': 'ok',
        'worker_check': 'ok',
        'failure_queue_check': 'ok',
    }
    if evaluation_dct != all_checks_ok:
        message = email_delivery.build_email_message(
            new_failures,
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
