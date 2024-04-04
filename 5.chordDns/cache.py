from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key in self.cache:
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        else:
            return None

    def put(self, key, value):
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.capacity:
            self.cache.popitem(last=False)  # Remove the least recently used item

        self.cache[key] = value

    def delete(self,key):
        if key not in self.cache:return
        else :
            del self.cache[key]

if __name__ =="__main__":
    cache = LRUCache(2)
    cache.put("key1", "value1")
    cache.put("key2", "value2")
    cache.put("key3", "value3")

    print(cache.get("key1"))  # Output: None
    print(cache.get("key2"))  # Output: value2
    print(cache.get("key3"))  # Output: value3