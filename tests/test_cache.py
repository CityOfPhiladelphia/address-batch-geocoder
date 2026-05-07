from utils.cache import LRUCache

def test_cache_hits_max_size():
    cache = LRUCache(max_size=20)

    for i in range(30):
        cache[i] = "test"
    
    assert len(cache) == 20

def test_cache_ejects_first_inserted():
    cache = LRUCache(max_size=20)

    for i in range(30):
        cache[i] = "test"
    
    cache[2] = "test"
    
    assert cache[1] == None

def test_cache_ejects_last_used():
    cache = LRUCache(max_size=20)

    for i in range(30):
        cache[i] = "test"
    
    assert cache[2] == None
    
    cache[2] = "test"
    
    assert cache[2] == "test"