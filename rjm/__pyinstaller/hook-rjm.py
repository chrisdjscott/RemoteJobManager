from PyInstaller.utils.hooks import collect_data_files

# we have to include_py_files so that funcx can extract function source
datas = collect_data_files('rjm', include_py_files=True, excludes=['__pyinstaller'])
