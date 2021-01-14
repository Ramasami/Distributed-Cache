package distributed.cache.store;

import java.util.concurrent.ConcurrentHashMap;

public class Cache {
    private static final ConcurrentHashMap<String,String> cache = new ConcurrentHashMap<>();

    String get(String key) {
        return cache.getOrDefault(key, "Not Found");
    }

    String put(String key, String value) {
        return cache.put(key,value);
    }
}
