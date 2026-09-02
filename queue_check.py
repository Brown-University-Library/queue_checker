"""
Coordinates the queue checks and sends an alert when a check fails.

Run from the repository root with: `uv run ./queue_check.py`
"""

import logging
import pprint

from lib import queue_check_helpers

log = logging.getLogger(__name__)


def run_code() -> None:
    """
    Runs the queue checks and sends an alert when a check fails.
    Called by: dunder-main.
    """
    output = queue_check_helpers.get_rqinfo()
    assert type(output) == str

    data_dct = queue_check_helpers.parse_rqinfo(output)
    assert type(data_dct) == dict

    previous_rqinfo_data = queue_check_helpers.load_previous_rqinfo_data(data_dct)
    assert type(previous_rqinfo_data) == dict
    queue_check_helpers.save_rqinfo_data(data_dct)

    previous_failure_count = previous_rqinfo_data['failed_count']
    evaluation_dct, new_failures = queue_check_helpers.evaluate_qdata(
        previous_failure_count,
        queue_check_helpers.expectations,
        data_dct,
    )
    assert type(evaluation_dct) == dict

    all_checks_ok = {
        'queue_check': 'ok',
        'worker_check': 'ok',
        'failure_queue_check': 'ok',
    }
    if evaluation_dct != all_checks_ok:
        message = queue_check_helpers.build_email_message(
            new_failures,
            previous_failure_count,
            queue_check_helpers.expectations,
            evaluation_dct,
            data_dct,
        )
        queue_check_helpers.send_email(message=message)

    log.info(f'evaluation_dct, ``{pprint.pformat(evaluation_dct)}``')


if __name__ == '__main__':
    run_code()
