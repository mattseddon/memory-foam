from collections.abc import Callable
from datetime import datetime


def is_modified_after(
    info: dict,
    get_last_modified: Callable[[dict], datetime],
    modified_after: datetime | None,
) -> bool:
    if modified_after is None:
        return True

    return get_last_modified(info) > modified_after
