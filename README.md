# Purpose

Occasionally, our servers are restarted. We have scripts to auto-restart necessary services, but every once in a while, a queue-worker fails to restart.

This code checks:
- that expected queues exist
- that expected workers are running.
- (experimental) that the number of jobs in the failed-queue is as expected.

If a queue-check or worker-check or failed-queue-count-check fails, an email is sent.

---

# Usage

```zsh
% cd /path/to/queue_checker/
% source ../env/bin/activate                # for access to rqinfo
% source ../venv_settings/env_settings.sh   # for access to settings
% python ./queue_check.py
```

Tests can be run via substituting for the above line:

```zsh
% python -m doctest ./queue_check.py
```
(which will show no output if all tests pass) ...or...

```zsh
% python -m doctest -v ./queue_check.py
```

--- 

# Other

## expectations setting 

The "expectations" setting is loaded from a json envar string, created from this dict-structure:

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
        {'queue': 'q2', 'worker_count': 2}
        ],
    'permitted_failures': 0
}
```

## email

The email sent, when an error is detected, displays:

- the error-check overview. Example:

        {'queue_check': 'ok', 'worker_check': 'FAIL', 'failure_queue_check': 'ok'}

- the full expectations-setting.

- the full rqinfo output.

---

[end]
