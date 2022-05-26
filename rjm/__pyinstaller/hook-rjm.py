import os
from PyInstaller.utils.hooks import collect_data_files

# we have to include_py_files so that funcx can extract function source
datas = collect_data_files('rjm', include_py_files=True, excludes=['__pyinstaller'])
datas += [(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, "setup", "funcx-endpoint-persist-nesi.sh"), os.path.join("rjm", "setup", "funcx-endpoint-persist-nesi.sh"))]
