import re
from fnmatch import translate
from typing import Callable, Optional


def get_glob_match(
    glob: Optional[str],
) -> Optional[Callable]:
    if glob:
        res = translate(glob)
        return re.compile(res).match

    return None


def is_match(path: str, glob_match: Optional[Callable]) -> bool:
    if not glob_match:
        return True

    return bool(glob_match(path))
