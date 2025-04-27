from .set_cache_dir import (
    get_cache_dir, is_cache_dir_set, set_cache_dir, is_cache_verbose, 
    cache_verbose_on, cache_verbose_off, clear_cache, unset_cache_dir
)
from ..hash import hash_object
import pickle
from functools import wraps
import os
from urllib.parse import quote
from pathlib import Path
import inspect
from typing import Optional

def get_arg_dict(func, *args_, **kwargs_):
    sig = inspect.signature(func)
    param_names = list(sig.parameters.keys())

    # Exclude any param already given via kwargs
    available_params = [p for p in param_names if p not in kwargs_]

    # Map remaining positionals to available param names
    arg_dict = dict(kwargs_)
    for name, val in zip(available_params, args_):
        arg_dict[name] = val

    # Fill in defaults
    for name, param in sig.parameters.items():
        if name not in arg_dict and param.default is not inspect._empty:
            arg_dict[name] = param.default

    return arg_dict

def cache(dir: str = None, pattern: str = None, output_type: Optional[type] = None):
    if output_type is None:
        subextension = ""
    else:
        subextension = f".{output_type.__name__}"
    def decorator(func):
        def get_path_object(*args_, **kwargs_):
            if not is_cache_dir_set():
                return None

            cache_path_ = get_cache_dir(output='path')
            base_dir_ = Path(cache_path_) / (dir or func.__name__)
            arg_dict = get_arg_dict(func, *args_, **kwargs_)

            if pattern:
                safe_args_ = {k: quote(str(v), safe="") for k, v in arg_dict.items()}
                name_ = pattern.format(**safe_args_)
            else:
                key_ = pickle.dumps(dict(sorted(arg_dict.items())))
                name_ = hash_object(key_)
            
            cache_file_ = base_dir_ / f"{name_}{subextension}.pkl"
            return cache_file_
        
        def get_path_string(*args_, **kwargs_):
            # Use get_path_object directly
            cache_file = get_path_object(*args_, **kwargs_)
            if cache_file is None:
                return None
                
            # Convert backslashes to forward slashes for consistency
            return str(cache_file).replace('\\', '/')

        @wraps(func)
        def wrapper(*args, **kwargs):
            if not is_cache_dir_set():
                return func(*args, **kwargs)
            
            # Use get_path_object directly
            cache_file = get_path_object(*args, **kwargs)
            if cache_file is None:
                return func(*args, **kwargs)
                
            cache_file.parent.mkdir(parents=True, exist_ok=True)

            if cache_file.exists():
                with open(cache_file, "rb") as f:
                    if is_cache_verbose():
                        print(f"Cache hit for function: {func.__name__}. Returning cached result from {cache_file}.")
                    result = pickle.load(f)
                    if output_type is not None:
                        if not isinstance(result, output_type):
                            raise TypeError(f"Reading Error: cached result is not of type {output_type.__name__} it is {type(result).__name__}")
                    return result

            result = func(*args, **kwargs)
            if output_type is not None:
                if not isinstance(result, output_type) and result is not None:
                    raise TypeError(f"Writing Error: result is not of type {output_type.__name__} it is {type(result).__name__}")
                
            if result is None:
                if is_cache_verbose():
                    print(f"Cache miss for function: {func.__name__}. Result is None. Not saving to cache at {cache_file}.")
                return None
            
            with open(cache_file, "wb") as f:
                pickle.dump(result, f)
                if is_cache_verbose():
                    print(f"Cache miss for function: {func.__name__}. Saving result to cache at {cache_file}.")

            return result

        def is_cached(*args_, **kwargs_):
            if not is_cache_dir_set():
                return False

            # Use get_path_object directly
            cache_file = get_path_object(*args_, **kwargs_)
            if cache_file is None:
                return False
                
            return cache_file.exists()

        def clear(*args_, **kwargs_):
            if not is_cache_dir_set():
                return

            # Use get_path_object directly
            cache_file = get_path_object(*args_, **kwargs_)
            if cache_file is None:
                return
                
            if cache_file.exists():
                cache_file.unlink()

        def clear_all():
            if not is_cache_dir_set():
                return
            
            base_dir_ = Path(get_cache_dir()) / (dir or func.__name__)
            if base_dir_.exists():
                for file in base_dir_.rglob("*.pkl"):
                    file.unlink()

        wrapper.is_cache_dir_set = is_cache_dir_set
        wrapper.get_path = get_path_string
        wrapper.is_cached = is_cached
        wrapper.clear = clear
        wrapper.clear_all = clear_all
        return wrapper
    return decorator

cache.get_dir = get_cache_dir
cache.set_dir = set_cache_dir
cache.is_dir_set = is_cache_dir_set
cache.is_verbose = is_cache_verbose
cache.verbose_on = cache_verbose_on
cache.verbose_off = cache_verbose_off
cache.clear = clear_cache
cache.unset_dir = unset_cache_dir
