# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/


class BadRequest(Exception):
    pass


class XtraBackupError(Exception):
    """Raised when the backup operation fails."""
