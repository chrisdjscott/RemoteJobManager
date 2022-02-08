# pyinstaller

In an environment with rjm already installed, build the executables with:

```
pip install pyinstaller
pyinstaller --additional-hooks-dir=. -F rjm_batch_submit.py
pyinstaller --additional-hooks-dir=. -F rjm_batch_wait.py
```

Executables are located in the *dist* directory.

The `rjm_batch_*.py` scripts in this directory are used because pyinstaller
cannot work with an entry point directly.
