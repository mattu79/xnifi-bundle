package io.activedata.xnifi2.support.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;

import javax.annotation.Nonnull;

/**
 * 缓存构造器
 */
public class CacheBuilder {
    public static <K, V> Cache<K, V> build(int cacheSize) {
        if (cacheSize < 1) cacheSize = 1000; // 默认缓存1000个实体
        return Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build();
    }

    public static <K, V> Cache<K, V> build(int cacheSize, long durationNanos) {
        if (cacheSize < 1) cacheSize = 1000; // 默认缓存1000个实体
        if (durationNanos < 1) durationNanos = 3000L; // 默认过期时间为5分钟
        long finalDurationNanos = durationNanos;
        return Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfter(new Expiry<K, V>() {
                    @Override
                    public long expireAfterCreate(@Nonnull K key, @Nonnull V value, long currentTime) {
                        return finalDurationNanos;
                    }

                    @Override
                    public long expireAfterUpdate(@Nonnull K key, @Nonnull V value, long currentTime, long currentDuration) {
                        return currentDuration;
                    }

                    @Override
                    public long expireAfterRead(@Nonnull K key, @Nonnull V value, long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                })
                .build();
    }
}
