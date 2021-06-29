# Memoria

*Memoria* is a Python library for hashing and caching.

## Installation
```shell
pip install memoria
```

## Benefits of Memoria

### Consistency
Unfortunately the built-in *hash* method is not consistent. 
For example if you hash a string in two different Python sessions, 
*e.g.*, `hash('hello world!')`, you may get different results, *e.g.*, `-69600567246316219` or 
`-8701498716516122875`. However, Memoria is consistent, *e.g.*, 
`memoria.hash('hello world!')` produces `PwDVM4wattDXKR1HUtszcPP5BHTUVTYQ5X0cO51yAn4=`. 
We should credit the built-in library [hashlib](https://docs.python.org/3/library/hashlib.html) of course.

### Hashing Unhashable Types
Memoria can hash virtually anything. If you use the built-in
*hash* method `hash(dict())` or `hash(list())` you will get an error:
`TypeError: unhashable type: 'dict'` but **Memoria** can even hash
*unhashable* types by converting them into a hashable type and 
hashing the result. To make sure that the hash is still different between
the original type and the hashable representation, Memoria takes some
additional measures.



## Usage


```python
import memoria

# hashing a python object
memoria.hash(123)
# >>> 'zi+wk24s9wwA/UiNRKbjeu6JfDi78yCj7yVL87sS0Ko='

# base is 64 by default but 32 can also be used. 
# base 64 should not be used in the file-system, e.g., file names, because it has inadmissible characters.
memoria.hash(123, base=32)
# >>> 'PONR14RE5JRGO07T926K99N3FBN8IV1ONFPI18VF4L5V7EOIQ2L0----'

# dictionaries are unhashable but Memoria can hash them
memoria.hash({'name': 'John', 'age': 24})
# >>> 'ioCMz5B8pcdk2CxcbIX/3n3qnQRn/yv9/zvC5Wc0YlU='
```
