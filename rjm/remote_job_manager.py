


class RemoteJobManager:
    """
    Manages remote jobs.

    """
    def __init__(self, config_file):
        self._config_file = config_file
        self._remote_jobs = []

