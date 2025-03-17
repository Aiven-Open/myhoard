# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from myhoard.util import BinVersion


def xtrabackup_version_to_string(version: BinVersion) -> str:
    v = list(version)
    assert 3 <= len(v) <= 4, f"Unexpected format of tool version: {v}"
    version_str = ".".join(str(v) for v in v[:3])
    if len(v) > 3:
        version_str += f"-{v[3]}"
    print(version_str)
    return version_str
