from memoria.cache import cache, set_cache_dir

def test_cache():
    set_cache_dir("test_cache")
    call_count = {'test_func': 0}

    @cache(dir="test_cache_subdir", pattern="test_func-{a}-{b}")
    def test_func(a, b):
        call_count['test_func'] += 1
        return a + b
    
    test_func.clear_all()
    assert test_func.get_path(1, 2) == "test_cache/test_cache_subdir/test_func-1-2.pkl"

    assert call_count['test_func'] == 0
    assert test_func(1, 2) == 3
    assert call_count['test_func'] == 1
    assert test_func.is_cached(1, 2)

    second_call = test_func(1, 2)
    assert second_call == 3
    assert call_count['test_func'] == 1

    assert not test_func.is_cached(1, 3)
    assert test_func(1, 3) == 4
    assert test_func.is_cached(1, 3)

    
def test_with_no_pattern():
    set_cache_dir("test_cache")
    call_count = {'test_func': 0}

    @cache(dir="test_cache_subdir")
    def test_func(a, b):
        call_count['test_func'] += 1
        return a + b * 2
    
    assert test_func.is_cache_dir_set()
    test_func.clear_all()

    assert call_count['test_func'] == 0
    assert test_func(1, 2) == 5
    assert call_count['test_func'] == 1
    assert test_func.is_cached(1, 2)
    assert test_func.is_cached(a=1, b=2)
    assert test_func(1, 2) == 5
    assert call_count['test_func'] == 1
    assert test_func.is_cached(1, 2)

    assert test_func.get_path(1, 2) == test_func.get_path(a=1, b=2)
    assert test_func.get_path(1, 2) == test_func.get_path(b=2, a=1)
    assert test_func.get_path(1, 2) == test_func.get_path(1, b=2)
    assert test_func.get_path(1, 2) == test_func.get_path(2, a=1)
    assert test_func(1, 2) == test_func(a=1, b=2)
    assert test_func(1, 2) == test_func(b=2, a=1)

    """
    when we provide b as a keyword argument, it should understand that the other is a
    when we provide a as a keyword argument, it should understand that the other is b
    """
    assert test_func(1, 2) == test_func(1, b=2)
    assert test_func(1, 2) == test_func(2, a=1)

    assert not test_func.is_cached(1, 3)
    
