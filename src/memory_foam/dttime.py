from collections.abc import Callable
from datetime import datetime


def is_modified_after(
    info: dict,
    get_last_modified: Callable[[dict], datetime],
    modified_after: datetime | None,
) -> bool:
    if not modified_after:
        return True

    return get_last_modified(info) > modified_after
