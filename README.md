# Memoria

A Python package for efficient function result caching with type safety and flexible storage options. Memoria helps you cache function results to avoid redundant computations, with built-in type checking and flexible storage options.

## Installation

```bash
pip install memoria
```

## Quick Start

The most basic usage of Memoria involves setting up a cache directory and using the `@cache` decorator on your functions. Here's a simple example:

```python
from memoria import cache

# Set up the cache directory (required before using the cache)
cache.set_dir("./my_cache")

# Basic usage with the @cache decorator
@cache()
def expensive_computation(x, y):
    # Your expensive computation here
    return x + y

# The result will be cached and reused for the same inputs
result = expensive_computation(5, 3)  # Computes and caches
result = expensive_computation(5, 3)  # Returns cached result
```

## Features

### Type Safety

Memoria can enforce type checking on your cached results. This is useful when you want to ensure that your function always returns data of a specific type. If the function returns a value of a different type, a TypeError will be raised:

```python
# Using actual type objects for type checking
@cache(output_type=list)  # Note: using list, not List
def get_numbers():
    return [1, 2, 3]

# This will raise a TypeError if the function returns something other than a list
# For example:
@cache(output_type=str)
def get_string():
    return "hello"  # This is fine
    # return 42  # This would raise TypeError: result is not of type str
```

### Custom Cache Directory Structure

You can organize your cached results in subdirectories by specifying a directory name in the cache decorator. This helps keep your cache organized, especially when dealing with multiple functions:

```python
@cache(dir="my_subdirectory")
def my_function():
    pass
```

The results will be stored in `./my_cache/my_subdirectory/` instead of directly in the cache root.

### Custom Cache File Naming

By default, Memoria uses a hash of the function arguments to name cache files. You can customize this using the `pattern` parameter, which supports Python's string formatting:

```python
@cache(pattern="{param1}_{param2}")
def my_function(param1, param2):
    pass
```

This will create cache files named like `param1_value_param2_value.pkl` instead of using a hash.

### Cache Management

Memoria provides several tools to manage your cached results:

```python
# Check if a result is cached
@cache()
def my_function(x):
    return x * 2

my_function.is_cached(x=5)  # Returns True if cached

# Clear specific cache
my_function.clear(x=5)  # Clears cache for x=5

# Clear all caches for a function
my_function.clear_all()

# Enable/disable verbose logging
cache.verbose_on()  # Shows cache hits/misses
cache.verbose_off()  # Hides cache hits/misses
```

The verbose mode is particularly useful during development as it shows you when results are being retrieved from cache versus being computed.

### Global Cache Management

In addition to per-function cache management, Memoria provides global cache management functions:

```python
# Get current cache directory
current_dir = cache.get_dir()

# Clear all caches
cache.clear()

# Unset cache directory
cache.unset_dir()
```

These functions help you manage the cache at a global level, useful for cleanup operations or changing cache locations.

## Advanced Usage

### Complex Data Types

Memoria works well with custom data types. When using custom classes, make sure to pass the actual class as the `output_type`:

```python
from dataclasses import dataclass

@dataclass
class Result:
    value: int
    description: str

@cache(output_type=Result)  # Using the actual Result class
def process_data(x: int) -> Result:
    return Result(value=x*2, description=f"Processed {x}")
```

The type checking ensures that your function returns an instance of the specified class.

## License

This project is licensed under the Conditional Freedom License (CFL-1.0) - see the LICENSE file for details.
