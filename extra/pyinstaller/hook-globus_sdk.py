from PyInstaller.utils.hooks import collect_data_files

datas = collect_data_files('globus_sdk')

hiddenimports = [
    'globus_sdk.services.transfer',
]
