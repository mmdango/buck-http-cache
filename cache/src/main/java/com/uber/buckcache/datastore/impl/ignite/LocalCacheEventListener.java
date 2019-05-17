package com.uber.buckcache.datastore.impl.ignite;

import com.uber.buckcache.utils.StatsDClient;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.uber.buckcache.datastore.impl.ignite.IgniteConstants.ARTIFACT_CACHE_NAME;
import static com.uber.buckcache.datastore.impl.ignite.IgniteConstants.EVENT_TYPE_TO_NAME_MAP;

public class LocalCacheEventListener implements IgnitePredicate<CacheEvent> {
  private static Logger logger = LoggerFactory.getLogger(LocalCacheEventListener.class);

  private final IgniteCache<Long, byte[]> metadataCache;
  private final IgniteCache<Long, String[]> reverseCacheKeys;
  private final IgniteCache<String, Long> cacheKeys;

  public LocalCacheEventListener(IgniteCache<Long, byte[]> metadataCache, IgniteCache<Long, String[]> reverseCacheKeys,
      IgniteCache<String, Long> cacheKeys) {
    this.metadataCache = metadataCache;
    this.reverseCacheKeys = reverseCacheKeys;
    this.cacheKeys = cacheKeys;
  }

  @Override
  public boolean apply(CacheEvent evt) {
    String cacheEventName = EVENT_TYPE_TO_NAME_MAP.get(evt.type());
    if (cacheEventName != null && cacheEventName.length() > 0) {
      String cacheEventMetric = String.format("event_cache.%s.%s.count", evt.cacheName(), cacheEventName).toLowerCase();
      logger.info("Reporting artifact-cache event: {} for key: {}", cacheEventMetric, evt.key());
      StatsDClient.get().count(cacheEventMetric, 1L);
    }
    // I suspect codepath below will never execute since there is no cache setup with name ARTIFACT_CACHE_NAME
    if (evt.cacheName().equals(ARTIFACT_CACHE_NAME)) {

      final String eventName =
          EVENT_TYPE_TO_NAME_MAP.containsKey(evt.type()) ? EVENT_TYPE_TO_NAME_MAP.get(evt.type()) : "UNKNOWN";
      Long underlyingKey = evt.key();
      String[] keys = reverseCacheKeys.getAndRemove(underlyingKey);

      logger.info(String.format("Artifact cache event {} with key {}.", eventName, underlyingKey), keys, evt);

      for (String key : keys) {
        cacheKeys.remove(key);
      }

      metadataCache.remove(underlyingKey);
    }

    return true;
  }

}
