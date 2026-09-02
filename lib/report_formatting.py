"""
Provides formatting shared by queue-checker alert reports.
"""

import datetime


def format_datetime(value) -> str:
    """
    Formats an aware datetime in local time and treats an RQ naive datetime as UTC.
    Called by: email_delivery.build_email_message() and failed_job_reports.format_failed_job().
    """
    if value is None:
        result = 'Unavailable'
    else:
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        result = value.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
    return result


def format_check_status(status: str) -> str:
    """
    Formats an internal check result for an operator-facing report.
    Called by: check_reports.build_check_summary(), check_reports.build_queue_check_report(),
    check_reports.build_worker_check_report(), and failed_job_reports.build_failure_queue_check_report().
    """
    result = 'FAILED' if status == 'FAIL' else status.upper()
    return result
