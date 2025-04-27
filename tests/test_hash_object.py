from memoria.hash import hash_object

def test_hash_object():
    assert hash_object(1) == hash_object(1)
    
    assert hash_object(1) != hash_object(2)
    assert isinstance(hash_object(1), int)

    assert hash_object((1, 2, 3)) == hash_object((1, 2, 3))
    assert hash_object({1,2,3}) == hash_object({1,3,2})

