"""
Coordinates the queue checks and sends an alert when a check fails.

Run from the repository root with: `uv run ./queue_check.py`
"""

import logging
import pprint

from lib import email_delivery, queue_data, queue_evaluation, settings

log = logging.getLogger(__name__)


def run_code() -> None:
    """
    Runs the queue checks and sends an alert when a check fails.
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
        email_delivery.send_email(message=message)

    log.info(f'evaluation_dct, ``{pprint.pformat(evaluation_dct)}``')


if __name__ == '__main__':
    run_code()
