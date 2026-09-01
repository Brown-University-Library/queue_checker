# AGENTS.md — Repository Agent Instructions (Source of Truth)

This file defines the canonical coding directives for this repository.

If other instruction files exist (Copilot, IDE rules, contributor docs) and conflict with this file, follow this file and treat the others as stale.

## Table of contents

- [Project basics](#project-basics)
- [How to run code](#how-to-run-code)
- [Coding directives (Python)](#coding-directives-python)
- [Queue-checker structure](#queue-checker-structure)
- [Tests](#tests)
- [Change workflow expectations](#change-workflow-expectations)
- [If instructions are missing or ambiguous](#if-instructions-are-missing-or-ambiguous)
- [Agent repository index](#agent-repository-index)

## Project basics

- Primary language: Python
- Target runtime: Python 3.8, matching `ruff.toml`
- Dependency management: the legacy virtual environment and `requirements.txt`
- Repository root: the directory containing this file, `.git/`, and `.gitignore`
- Purpose: check expected RQ queues and workers, detect a surge in failed jobs, and send an alert email when a check fails

## How to run code

- Assume the user is in the repository-root directory.
- Activate the environment and load settings before running the script:

  ```zsh
  source ../env/bin/activate
  source ../venv_settings/env_settings.sh
  python ./queue_check.py
  ```

- Run the doctests with:

  ```zsh
  python -m doctest ./queue_check.py
  ```

- For verbose doctest output, run:

  ```zsh
  python -m doctest -v ./queue_check.py
  ```

## Coding directives (Python)

### Type hints and imports

- Keep all syntax and type hints compatible with Python 3.8.
- Add type hints to functions and important variables when changing related code.
- Do not use Python 3.9+ builtin generics such as `list[str]` or Python 3.10+ union syntax such as `str | None`.
- Avoid new `typing` imports unless they are necessary for Python 3.8-compatible annotations.

### Script structure

- Keep `run_code()` as the simple entry-point controller unless a task intentionally changes the entry-point name.
- Keep the `if __name__ == '__main__': run_code()` guard.
- Put substantive logic in top-level helper functions; do not define functions inside other functions.
- Rarely use more than three levels of function calls below the entry point.

### Functions and control flow

- Prefer single-return functions with local variables and a final return.
- Do not define functions inside other functions.
- Favor clarity and explicitness over cleverness.

### Logging

- When possible, format logged variable values as a label followed by a comma and a space, with the value enclosed in double backticks.
- Prefer a label that matches the variable name. For example: `log.debug(f'queue, ``{queue}``')`.
- Do not log environment-setting values that can contain credentials, infrastructure details, recipient addresses, or other sensitive data.

### HTTP and networking

- Use `httpx` for new HTTP calls.
- Do not introduce alternate HTTP libraries unless the repository already depends on one and there is a documented reason.
- Preserve the existing standard-library SMTP implementation unless the requested change requires replacing it.

### Docstrings

- Use triple-quoted docstrings.
- Write docstrings in present tense, with triple quotes on their own lines.
- End non-test function docstrings with `Called by: the_caller_function()` or, for external callers, the relevant module or class path.
- Start test-function docstring text with `Checks...`.
- Within functions, begin header comments with two hashes, for example `## parse output`.

### Additional coding directives

- Inspect `ruff.toml` in the repository root for additional directives, including line length, target version, and quote style.

### Markdown formatting

- Do not use hard line breaks in Markdown files; let paragraphs wrap naturally.
- When creating a Markdown file with more than three top-level `##` headings, add a table of contents near the top with links to those headings.

## Queue-checker structure

- `queue_check.py` contains the controller and all queue-checking helpers.
- Keep `run_code()` focused on coordinating these steps: run `rqinfo`, parse its output, load and save the prior result, evaluate the checks, build an alert, and send email when needed.
- Keep parsing, evaluation, persistence, message construction, and email delivery in separate top-level helpers.
- `email_template.txt` is the plain-text alert template used by `build_email_message()`.
- `requirements.txt` pins the legacy RQ and Click versions. RQ upgrades must be coordinated with the other applications sharing Redis because incompatible RQ versions may store data differently.
- The script reads the previous result from `../previous_rqinfo_data/previous_rqinfo_data.json`. Its first-run behavior creates the directory and stores the current result.
- `rqinfo --by-queue --raw` output is an external interface. Changes to `parse_rqinfo()` must account for both queue-count lines and worker-list lines, including the en dash used when no workers are present.
- Expected queues, expected worker counts, and the failed-job surge limit come from the JSON value in `QCHKR__EXPECTATIONS_JSON`.
- Email delivery uses `QCHKR__EMAIL_HOST`, `QCHKR__EMAIL_HOST_PORT`, and `QCHKR__EMAIL_RECIPIENTS_JSON`.
- Logging level comes from `QCHKR__LOG_LEVEL` and defaults to `INFO`.
- Do not add real environment values, hostnames, email addresses, queue names, credentials, or operational data to the repository.

## Tests

- The current test suite consists of doctests embedded in `queue_check.py`.
- New parsing or evaluation behavior should normally include a focused doctest covering the normal case and at least one failure or edge case.
- Run `python -m doctest ./queue_check.py` after code changes.
- Tests must not require a live Redis server, SMTP server, or production environment settings unless the task specifically requires an integration test.

## Change workflow expectations

When implementing a change:

1. Read the relevant surrounding code and match existing conventions.
2. Make the smallest correct change that satisfies the request.
3. Update doctests when behavior changes.
4. Run `python -m doctest ./queue_check.py`.
5. If the environment cannot run the tests, still write or adjust the tests and state exactly what remains to be run.

### Commit messages

- Group related files into logical, focused commits; do not require a separate commit for every file.
- Keep each commit message brief, with no more than ten words.
- Write messages in the present tense so they complete the phrase "This commit..." Begin with a fitting verb such as "Adds," "Implements," or "Updates."

## If instructions are missing or ambiguous

- Do not ask questions unless absolutely necessary to proceed.
- Make reasonable assumptions, state them explicitly, then implement.
- If blocked, provide what you tried, what you found in the repository, and a concrete next step such as a command, file to edit, or minimal decision needed.

## Agent repository index

- `queue_check.py`: executable script, doctests, RQ inspection, result evaluation, persistence, and email delivery
- `email_template.txt`: alert-email body with `string.Template` placeholders
- `requirements.txt`: pinned runtime dependencies (`rq==0.13.0` and its compatible Click version)
- `README.md`: purpose, environment setup, test commands, expectations-data shape, and alert contents
- `ruff.toml`: Python 3.8 linting and formatting rules, including 125-character lines and single quotes
- Runtime inputs: settings from `QCHKR__*` environment variables, `rqinfo` on `PATH`, Redis on localhost, and the prior-result JSON file in the adjacent `previous_rqinfo_data` directory
