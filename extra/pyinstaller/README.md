# pyinstaller

In an environment with `rjm` already installed, build the executables with:

```
pip install pyinstaller
pyinstaller --additional-hooks-dir=. -F ../../src/rjm/cli/rjm_batch_submit.py
pyinstaller --additional-hooks-dir=. -F ../../src/rjm/cli/rjm_batch_wait.py
pyinstaller --additional-hooks-dir=. -F ../../src/rjm/cli/rjm_config.py
pyinstaller --additional-hooks-dir=. -F ../../src/rjm/cli/rjm_authenticate.py
pyinstaller --additional-hooks-dir=. -F ../../src/rjm/cli/rjm_health_check.py
```

Executables will be stored in the *dist* subdirectory.
