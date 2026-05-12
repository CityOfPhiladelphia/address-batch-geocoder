from collections import OrderedDict


class LRUCache:
    """
    A cache that stores processed records. The cache
    automatically evicts old records once it grows past a maximum size.
    When a record appears in a cache, move it to the end so it doesn't
    get removed until later.
    """

    def __init__(self, max_size=20_000):
        self._cache = OrderedDict()
        self.max_size = max_size

    def __getitem__(self, key):
        if key in self._cache:
            self._cache.move_to_end(key)

        return self._cache.get(key)

    def __setitem__(self, key, value):
        self._cache[key] = value

        if len(self._cache) > self.max_size:
            self._cache.popitem(last=False)

    def __len__(self):
        return len(self._cache)
