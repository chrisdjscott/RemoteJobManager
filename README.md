# Remote Job Manager

RemoteJobManager (RJM) is a Python package that offloads work to a remote HPC
cluster. It is developed primarily for NeSI's Mahuika cluster (Slurm) but the
runner and transferer abstractions allow other backends. It evolved from the
SSH-based [rjm](https://github.com/mondkaefer/rjm) toward a Globus-first design.

## Backends

RJM supports two interchangeable backends, selected via the `[COMPONENTS]`
section of the config file:

- **Globus stack (default, primary)**: file transfer over HTTPS via a Globus
  guest collection, plus Globus Compute (formerly funcX) to invoke commands and
  submit Slurm jobs on the remote machine. This is the supported configuration
  and where most development effort goes.
- **Paramiko stack (experimental)**: SSH for command execution and SFTP for
  file transfer, using a locally generated keypair. Jobs are launched in a
  detached `tmux` session on the remote and completion is signalled by a
  `.rjm-succeeded` sentinel file. This backend is sparsely tested and lacks
  some features of the Globus stack (for example, job queuing via Slurm;
  all work runs directly under `tmux` without a scheduler).
  Use the Globus backend unless you specifically need an SSH-based alternative.

## How a job runs

For each local job directory, RJM:

1. Reads `rjm_uploads.txt` and uploads the listed files to a remote directory.
2. Starts the job script on the remote (Slurm via `sbatch` for the Globus
   stack, `tmux` for the Paramiko stack).
3. Polls until the job finishes.
4. Reads `rjm_downloads.txt` and downloads the outputs.

State is persisted in `remote_job.json` inside the local job directory, so
workflows can be resumed.

## Installation

On Windows, prebuilt executables are available from the
[GitHub releases page](https://github.com/chrisdjscott/RemoteJobManager/releases).

Otherwise, install with pip:

```bash
python -m pip install git+https://github.com/chrisdjscott/RemoteJobManager
```

To use the Paramiko (SSH/SFTP) backend, install the optional `ssh` extra:

```bash
python -m pip install "RemoteJobManager[ssh] @ git+https://github.com/chrisdjscott/RemoteJobManager"
```

## Configuration

Run the interactive setup once per machine:

```bash
rjm_config              # Globus backend (default)
rjm_config --ssh        # Paramiko/SSH backend (experimental)
```

The Globus path creates a Globus guest collection on NeSI, sets the NeSI
managed Globus Compute endpoint, writes `~/.rjm/rjm_config.ini`, and obtains
the required Globus tokens (cached at `~/.rjm/rjm_tokens.json`). The SSH path
generates a keypair under `~/.rjm/`, prints the public key to add to
`~/.ssh/authorized_keys` on the remote, and writes the `[PARAMIKO]` block to
the config file.

You will need a [Globus account](https://app.globus.org/) (and NeSI
credentials, if NeSI is your remote) to complete the Globus setup.

## Usage

Quick health check:

```bash
rjm_health_check
```

Submit and wait for a batch of local job directories listed in `localdirs.txt`:

```bash
rjm_batch_submit -f localdirs.txt -ll info
rjm_batch_wait   -f localdirs.txt -ll info

# or in a single step:
rjm_batch_run    -f localdirs.txt -ll info
```

Each local job directory needs an `rjm_uploads.txt` (files to upload) and an
`rjm_downloads.txt` (files to fetch back). A worked example lives in
[`examples/nonmem`](examples/nonmem).

Additional commands: `rjm_batch_cancel` to cancel running jobs and
`rjm_authenticate` to refresh Globus tokens.

## Documentation

Full documentation, including the NeSI getting-started guide, the CLI
reference, and troubleshooting notes, is hosted on GitHub Pages:

https://chrisdjscott.github.io/RemoteJobManager/

## Development

```bash
python -m pip install -e .[dev]        # Globus stack only
python -m pip install -e .[dev,ssh]    # add paramiko for the SSH stack
pytest
```

Tests are unit-level with mocked Globus calls. CI runs the matrix on Ubuntu,
macOS, and Windows (Python 3.10-3.12) and optionally a real integration test
against NeSI when secrets are configured.
