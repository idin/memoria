from memoria.cache import set_cache_dir, get_cache_dir, is_cache_dir_set, unset_cache_dir
import pytest

def test_raise_error_if_cache_dir_not_set():
    unset_cache_dir()
    with pytest.raises(ValueError):
        get_cache_dir()

def test_set_cache_dir():
    set_cache_dir("test_cache")

def test_get_cache_dir():
    cache_dir = get_cache_dir()
    assert cache_dir == "test_cache"


