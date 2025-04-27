import hashlib


def hash_int(x):
    """
    Hashes the input value x using SHA-256 and returns an integer value.
    :param x: Input value to hash.
    :return: Final hashed integer value.
    """
    _hash_maker = hashlib.sha256()
    encoded = str(x).encode('utf-8')  # Ensure the value is encoded to bytes
    _hash_maker.update(encoded)
    return int(_hash_maker.hexdigest(), 16)


def make_hashable(obj):
    """
    Converts an object (list, tuple, dict, or primitive) into a hashable form.
    :param obj: Object to hash (can be nested).
    :return: Hashable version of the object.
    """
    if hasattr(obj, '__hash__') and callable(getattr(obj, '__hash__')):
        # If the object has a __hash__ method, return it directly (don't hash)
        return obj.__hash__()

    if isinstance(obj, (tuple, list)):
        # Recursively make each element hashable
        return tuple(make_hashable(e) for e in obj)
    
    if isinstance(obj, set):
        return tuple(sorted(make_hashable(e) for e in obj))
    
    if isinstance(obj, dict):
        # Recursively make each key-value pair hashable
        return tuple(sorted((make_hashable(k), make_hashable(v)) for k, v in obj.items()))
    
    # If obj is a primitive type, just return it
    return obj


def hash_object(obj):
    """
    Hashes any object (including nested lists, tuples, dicts) by first making it hashable.
    If the object has a __hash__ method, it uses that directly.
    :param obj: Object to hash (can be nested).
    :return: Hashed integer value of the object.
    """
    # First, convert the object to a hashable form
    hashable_obj = make_hashable(obj)
    # Then hash the resulting hashable form
    return hash_int(hashable_obj)
