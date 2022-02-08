# pyinstaller

In an environment with `rjm` already installed, build the executables with:

```
pip install pyinstaller
pyinstaller -F rjm_batch_submit.py
pyinstaller -F rjm_batch_wait.py
```

Executables will be stored in the *dist* subdirectory.

The `rjm_batch_*.py` scripts in this directory are used because pyinstaller
doesn't work with an entry point directly.
