# Queue Checker

Queue Checker detects incorrect RQ worker-subscription counts, waiting jobs without listeners, and increases in failed jobs. It reports queue registration separately as information. When an alerting check fails, it sends an email containing the results and supporting data.

## Contents

- [Purpose](#purpose)
- [Setup](#setup)
- [Running the checker](#running-the-checker)
- [Running tests](#running-tests)
- [Settings](#settings)
- [Alert email](#alert-email)
- [Dependency note](#dependency-note)

## Purpose

Servers sometimes restart, and occasionally a queue worker does not restart with them. This script checks:

- That each expected queue has the expected number of active worker subscriptions.
- Whether an ordinary queue has waiting jobs but no active subscribers.
- Whether the failed-job count increased beyond the configured limit.

Queue registration is informational because RQ 0.13 may not register a queue name until a job has been enqueued. An unregistered expected queue does not cause an alert when the independent worker inventory shows the correct subscriptions.

The checker runs separate queue and worker inspections. A failed command, malformed output, or incomplete worker record such as `worker example.123 ?` produces a data-collection error instead of inferred queue or worker results. The prior successful data file is not replaced after an inspection error.

## Local install

Though tests can be run locally, to actually _use_ the queue-check locally requires a running Redis instance, with queues and workers in-place.

```zsh
cd /path/to/queue_checker_stuff/
git clone git@github.com:Brown-University-Library/queue_checker.git
cd ./queue_checker/
uv sync --locked --group local
```

The application loads settings from `.env` in the outer `queue_checker_stuff/` directory. For a new local installation, copy the safe example and replace its invented values with settings appropriate for that environment:

```zsh
cp ./sample_dot_env.txt ../.env
```

Values in `.env` take precedence over values already exported in the process environment.

## Running the checker

Run the checker from the repository root:

```zsh
uv run ./queue_check.py
```

To suppress email delivery and print the complete alert body to stdout instead:

```zsh
uv run ./queue_check.py --no-email
```

The flag produces alert or data-collection-error output when attention is required; when both alerting checks pass, there is no report body to print.

The command expects `rqinfo` from the locked environment, Redis on localhost, and the prior-result JSON file in the adjacent `previous_rqinfo_data/` directory. It runs both `rqinfo --by-queue --raw` and `rqinfo --only-workers --raw`. The first run creates the prior-result directory and file if necessary.

Server deployments use the `staging` dependency group on non-production hosts and `prod` on production hosts. For deployment, the project's code-update-script caller specifies the appropriate group and calls the shared uv-aware deployment script.

## Running tests

This project is unusual in that all the tests are doctests.

The test runner executes the doctests embedded in the modules under `lib/`. It replaces the live failed-queue lookup during tests, so Redis and SMTP are not required.

```zsh
uv run ./run_tests.py
```

For detailed doctest output:

```zsh
uv run ./run_tests.py --verbose
```

The runner exits with a nonzero status when a doctest fails, which allows the deployment script to report test failures correctly.

## Settings

The outer `.env` uses these keys:

- `QCHKR__EXPECTATIONS_JSON`: JSON containing `expected_queues`, `expected_workers`, and `surge_failure_limit`.
- `QCHKR__EMAIL_FROM`: sender used for both the message header and SMTP envelope.
- `QCHKR__EMAIL_HOST`: SMTP host.
- `QCHKR__EMAIL_HOST_PORT`: SMTP port.
- `QCHKR__EMAIL_RECIPIENTS_JSON`: JSON list of recipient addresses.
- `QCHKR__LOG_LEVEL`: logging level, normally `INFO` or `DEBUG`.

The expectations value follows this shape:

```python
expectations_dict_example = {
    'expected_queues': [
        'failed',
        'q1',
        'q2',
    ],
    'expected_workers': [
        {'queue': 'failed', 'worker_count': 0},
        {'queue': 'q1', 'worker_count': 1},
        {'queue': 'q2', 'worker_count': 2},
    ],
    'surge_failure_limit': 10,
}
```

See `sample_dot_env.txt` for a complete, non-operational example. Real hostnames, addresses, queues, and other deployment values must remain outside the Git repository.

## Alert email

When a check fails, the email includes:

- A short summary of the worker-subscription and failed-job checks.
- Every expected worker-subscription count, its observed count, and worker identifiers where available.
- Any ordinary queue that has waiting jobs but no active subscriber.
- Informational lists of expected registered names, expected names not registered yet, and additional registered names.
- The previous and current failed-job counts, the change, and the allowed increase.
- Details for at most the three newest selected failed jobs, including the originating queue, function, timestamps, exception, and deepest two traceback frames when available.
- Data-collection status, direct verification commands, and a concise list of alert reasons.

The failed-job check infers selected jobs from the net increase in the failed-queue count. It does not persist job IDs between runs, so the email describes these entries as selected failed jobs rather than guaranteeing that every entry arrived after the previous run.

If an RQ command fails or either command returns malformed or incomplete data, the checker sends a separate inspection-error message. That message does not claim that queues or workers are missing when the source information is unreliable.

## Dependency note

The initial uv conversion intentionally preserves the deployed RQ, Click, Redis, and async-timeout versions. RQ upgrades must be coordinated with other applications sharing Redis because incompatible versions may store queue data differently. `pyproject.toml` and `uv.lock` are the active dependency files.
