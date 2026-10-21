from __future__ import annotations

import dataclasses
import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def dumps(value: Any, *, pretty: bool = False) -> str:
    return json.dumps(
        value,
        default=json_default,
        sort_keys=True,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    )
