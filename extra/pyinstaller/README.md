# pyinstaller

In an environment with `rjm` already installed, build the executables with:

```
pip install pyinstaller
pyinstaller -F ../../src/rjm/cli/rjm_batch_submit.py
pyinstaller -F ../../src/rjm/cli/rjm_batch_wait.py
pyinstaller -F ../../src/rjm/cli/rjm_setup.py
pyinstaller -F ../../src/rjm/cli/rjm_restart.py
pyinstaller -F ../../src/rjm/cli/rjm_authenticate.py
pyinstaller -F ../../src/rjm/cli/rjm_health_check.py
```

Executables will be stored in the *dist* subdirectory.
