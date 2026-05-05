# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project overview

RemoteJobManager (RJM) is a Python package that offloads work to a remote HPC cluster. It uses Globus for file transfer (HTTPS via a guest collection) and Globus Compute (formerly funcX) to invoke commands and Slurm jobs on the remote machine. It is developed primarily for NeSI's Mahuika cluster but the runner and transferer abstractions allow other backends.

The typical workflow per local job directory:

1. Read `rjm_uploads.txt`, upload listed files to a remote directory via Globus HTTPS.
2. Submit a Slurm script (default `run.sl`) on the remote via a Globus Compute function.
3. Poll until the Slurm job finishes (with a faster "warmup" polling interval at first).
4. Read `rjm_downloads.txt` and download outputs (with checksum verification).

State for each job is persisted in `remote_job.json` inside the local job directory so workflows can be resumed.

## Repository layout

- `src/rjm/` package root.
  - `remote_job.py`: `RemoteJob` orchestrates one job (upload, run, wait, download, state persistence).
  - `remote_job_batch.py`: `RemoteJobBatch` drives many `RemoteJob`s concurrently (shared runner/transferer, Globus auth done once).
  - `config.py`: ini-based config at `~/.rjm/rjm_config.ini`. `CONFIG_OPTIONS_REQUIRED` and `CONFIG_OPTIONS_OPTIONAL` define the schema.
  - `utils.py`: logging setup, Globus auth via `fair_research_login`, retry defaults, helpers. Token file lives at `~/.rjm/rjm_tokens.json`.
  - `auth.py`: `do_authentication()` used by `rjm_authenticate` and after `rjm_config`.
  - `errors.py`: custom exception types.
  - `runners/`
    - `runner_base.py`: `RunnerBase` interface (`make_remote_directory`, `start`, `wait`, `cancel`, scope/auth hooks).
    - `globus_compute_slurm_runner.py`: default runner. Submits Slurm via `sbatch`, polls `sacct`.
    - `globus_compute_native_runner.py`: alternative runner that runs natively on the compute endpoint (no Slurm).
  - `transferers/`
    - `transferer_base.py`: `TransfererBase` interface.
    - `globus_https_transferer.py`: HTTPS uploads/downloads against a Globus guest collection.
  - `setup/nesi.py`: `NeSISetup` creates a Globus guest collection on Mahuika and resolves the Globus Compute endpoint id. Used by `rjm_config`.
  - `cli/`: thin argparse entry points wired up in `pyproject.toml`. Each one parses args, calls `utils.setup_logging`, and delegates to the package code.
  - `tests/`, `runners/tests/`, `transferers/tests/`, `cli/tests/`, `setup/tests/`: `pytest` unit tests, mocked with `pytest-mock` and `responses`.
- `docs/`: Sphinx documentation (RST). User-facing docs live here, including `getting_started_nesi.rst` and `command_line_interface.rst`.
- `examples/`: example local job directories used for manual and CI integration tests (see the `nonmem` example).
- `extra/pyinstaller/`: PyInstaller spec for building Windows binaries used in releases.
- `.github/workflows/build.yml`: matrix build/test on Ubuntu, macOS, Windows (Python 3.10-3.12), plus PyInstaller build and an optional "real" Globus integration test gated on repo secrets.

## CLI entry points

Defined in `pyproject.toml` under `[project.scripts]`:

- `rjm_config`: interactive NeSI setup (creates guest collection, writes config, runs auth).
- `rjm_authenticate`: refresh/replace Globus tokens.
- `rjm_batch_submit`: upload + start for a list of local job directories.
- `rjm_batch_wait`: wait + download for that list.
- `rjm_batch_run`: submit then wait in one step.
- `rjm_batch_cancel`: cancel a batch.
- `rjm_health_check`: basic round-trip check against the configured endpoints.
- `rjm_restart`: rerun NeSI setup against an existing install.

Each CLI script takes `-f <localjobdirfile>` (a text file listing local job directories, one per line), plus logging flags `-l`, `-ll`, `-le`. `--force` ignores prior state in `remote_job.json`.

## Configuration

- File: `~/.rjm/rjm_config.ini` (location exported as `config.CONFIG_FILE_LOCATION`).
- Required sections: `GLOBUS` (`remote_endpoint`, `remote_path`), `FUNCX` (`remote_endpoint`).
- Optional sections: `SLURM` (`slurm_script`, `poll_interval`, `warmup_poll_interval`, `warmup_duration`), `FILES` (`uploads_file`, `downloads_file`), `RETRY` (`override_defaults`, `tries`, `backoff`, `delay`, `max_delay`), `LOGGING` (per-logger overrides).
- `SLURM:poll_interval` is clamped to a minimum (see `MIN_POLLING_INTERVAL` in `globus_compute_slurm_runner.py`).
- Tokens cached at `~/.rjm/rjm_tokens.json` via `fair_research_login.JSONTokenStorage`.

When adding a new config option, register it in `CONFIG_OPTIONS_REQUIRED` or `CONFIG_OPTIONS_OPTIONAL` in `src/rjm/config.py` and document it in `docs/configuration.rst`.

## Development

Setup:

```
python -m pip install -e .[dev]
```

Run tests:

```
pytest
```

Tests are unit-level with mocked Globus and Globus Compute calls. CI also runs a real integration test against NeSI when the `RJM_CONFIG` and `RJM_TOKENS` secrets are present; do not rely on that path locally.

Build the Sphinx docs:

```
cd docs && make html
```

Build standalone Windows binaries (requires Windows + `pyinstaller`):

```
cd extra/pyinstaller
pyinstaller --additional-hooks-dir=. -F ../../src/rjm/cli/rjm_batch_submit.py
```

Versioning is via `setuptools_scm`; the version is written to `src/rjm/_version.py` at build time. Release tags trigger the PyInstaller release artefact upload in `.github/workflows/build.yml`.

## Conventions

- Python >= 3.10. Follow existing style: 4-space indent, snake_case, docstrings on public methods, `logger = logging.getLogger(__name__)` in every module.
- Per-job log prefixes: classes that act on a specific job carry a `self._label` and use `self._log(level, msg)` rather than the module logger directly. Preserve this pattern when adding methods.
- Retries: wrap remote calls with `retry.api.retry_call` using values from `utils.get_retry_values_from_config`. Keep retry parameters consistent with existing call sites rather than introducing new defaults.
- Exceptions: raise the typed errors from `rjm.errors` (`RemoteJobRunnerError`, `RemoteJobTransfererError`, `RemoteJobBatchError`, `RemoteJobConfigError`) so callers can catch them.
- Globus Compute functions are defined as plain top-level functions (e.g. `path_join`, `check_dir_exists` in `runner_base.py`). They run on the remote endpoint, so imports must be inside the function body and they must not close over module-level state.
- State persistence: any new field stored on `RemoteJob`, runner, or transferer that needs to survive a restart must be added to the corresponding `_save_state`/`_load_state` (or `save_state`/`load_state`) methods.
- CI matrix is Ubuntu/macOS/Windows; avoid POSIX-only assumptions in path handling. Use `os.path` helpers.

## What not to touch without being asked

- `~/.rjm/` files on the developer's machine (config and tokens). Tests must not write there.
- `src/rjm/_version.py` (auto-generated by `setuptools_scm`).
- Files under `examples/` unless explicitly asked: they back the integration tests in CI.

## Writing style

- Prefer British English spelling (`specialise`, `behaviour`, `colour`).
- No em dashes, en dashes, or spaced hyphens as sentence interrupters. Use commas, periods, or parentheses.
- Be direct and technical. No filler ("Great question!", "I'd be happy to").
- No emojis.
- No unnecessary comments. Only comment when the *why* is non-obvious.

## Coding behaviour

- Do not start implementing, refactoring, or modifying code unless explicitly asked. Discuss and summarise first, then wait for an instruction such as "implement this" or "fix this".
- Prefer idiomatic, straightforward code over defensive or speculative generality.
- Do not add hypothetical edge-case handling, fallbacks, retries, or abstractions unless the task requires them or the codebase already justifies them.
- Solve the current problem; do not design for speculative future reuse.
- Follow existing patterns in this repository in preference to introducing new ones.
