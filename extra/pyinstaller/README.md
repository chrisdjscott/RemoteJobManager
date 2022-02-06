# pyinstaller

Build with:

```
pip install pyinstaller
pyinstaller --additional-hooks-dir=. -F ../scripts/rjm_batch_submit.py
pyinstaller --additional-hooks-dir=. -F ../scripts/rjm_batch_wait.py
```
