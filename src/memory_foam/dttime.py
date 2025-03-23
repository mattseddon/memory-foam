from datetime import datetime
from typing import Callable, Optional


def is_modified_after(
    info: dict,
    get_last_modified: Callable[[dict], datetime],
    modified_after: Optional[datetime],
) -> bool:
    if not modified_after:
        return True

    return get_last_modified(info) > modified_after
