# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from typing import Dict


def parse_checkpoint_file_content(checkpoint_file_content) -> Dict[str, str]:
    res = {}
    for line in checkpoint_file_content.splitlines():
        var, val = line.split("=")
        res[var.strip()] = val.strip()

    return res
