import os
from PyInstaller.utils.hooks import collect_data_files

# we have to include_py_files so that funcx can extract function source
datas = collect_data_files('rjm', include_py_files=True, excludes=['__pyinstaller', 'tests'])

# force the bash scripts to be included (doesn't on Windows by default)
this_dir = os.path.dirname(os.path.abspath(__file__))
persist_script = os.path.join(this_dir, os.pardir, "setup", "globus-compute-endpoint-persist-nesi.sh")
functions_file = os.path.join(this_dir, os.pardir, "setup", "globus-compute-endpoint-persist-nesi-functions.sh")
dest_dir = os.path.join("rjm", "setup")
datas += [
    (persist_script, dest_dir),
    (functions_file, dest_dir),
]
