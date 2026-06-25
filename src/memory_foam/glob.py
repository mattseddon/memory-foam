import re
from collections.abc import Callable
from fnmatch import translate


def get_glob_match(
    glob: str | None,
) -> Callable | None:
    if glob:
        res = translate(glob)
        return re.compile(res).match

    return None


def is_match(path: str, glob_match: Callable | None) -> bool:
    if not glob_match:
        return True

    return bool(glob_match(path))
