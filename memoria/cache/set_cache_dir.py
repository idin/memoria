from pathlib import Path
from typing import Literal
CACHE_DIR = None
VERBOSE_CACHE = False

def set_cache_dir(path: str):
    global CACHE_DIR
    CACHE_DIR = path
    if is_cache_verbose():
        print(f"Cache directory set to {CACHE_DIR}")

def unset_cache_dir():
    global CACHE_DIR
    CACHE_DIR = None
    if is_cache_verbose():
        print("Cache directory unset")

def is_cache_dir_set():
    return CACHE_DIR is not None

def get_cache_dir(absolute: bool = False, output: Literal["path", "string", "str"] = "str"):
    global CACHE_DIR
    if CACHE_DIR is None:
        raise ValueError("Cache directory not set")
    path = Path(CACHE_DIR).absolute() if absolute else Path(CACHE_DIR)
    return path.as_posix() if output != "path" else path

def cache_verbose_on():
    global VERBOSE_CACHE
    VERBOSE_CACHE = True

def cache_verbose_off():
    global VERBOSE_CACHE
    VERBOSE_CACHE = False

def is_cache_verbose():
    return VERBOSE_CACHE

def clear_cache():
    # delete all files in the cache directory
    if not is_cache_dir_set():
        raise ValueError("Cache directory not set")
    cache_dir = get_cache_dir(absolute=True, output="path")
    for file in cache_dir.rglob("*.pkl"):
        file.unlink()
    if is_cache_verbose():
        print(f"Cache cleared at {cache_dir}")


