# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/


class BadRequest(Exception):
    pass


class XtraBackupError(Exception):
    """Raised when the backup operation fails."""


class UnknownBackupSite(Exception):
    """Referenced backup site not in configuration."""

    def __init__(self, backup_site_name, known_backup_sites):
        super().__init__()
        self.backup_site_name = backup_site_name
        self.known_backup_sites = known_backup_sites

    def __str__(self):
        return f"backup site {self.backup_site_name} unknown (sites: {self.known_backup_sites!r})"
