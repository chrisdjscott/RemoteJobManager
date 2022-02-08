
import os


# tell pyinstaller where to find hooks provided by this package
def get_hook_dirs():
    return [os.path.dirname(__file__)]


# tell pyinstaller where to find tests of the hooks provided by this package
def get_PyInstaller_tests():
    return []
