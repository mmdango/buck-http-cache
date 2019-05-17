package com.uber.buckcache.utils;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.uber.buckcache.StatsdConfig;

public class StatsDClient {
  private static StatsDClient statsDClient;

  private final StatsdConfig statsDConfig;
  private final com.timgroup.statsd.StatsDClient underlyingClient;

  private StatsDClient(StatsdConfig statsDConfig) {
    this.statsDConfig = statsDConfig;

    if (this.statsDConfig.isEnabled()) {
      underlyingClient = new NonBlockingStatsDClient(this.statsDConfig.getPrefix(), this.statsDConfig.getHost(),
          this.statsDConfig.getPort());
    } else {
      underlyingClient = new NoOpStatsDClient();
    }
  }

  public synchronized static void init(StatsdConfig statsDConfig) {
    if (statsDClient == null) {
      statsDClient = new StatsDClient(statsDConfig);
    }
  }

  public static StatsDClient get() {
    return statsDClient;
  }

  private String[] customUnifiedTag(String[] tags) {
      tags = Arrays.stream(tags)
              .filter(StringUtils::isNotEmpty)
              .map(t -> StringUtils.replaceChars(t, ':', '_'))
              .toArray(String[]::new);
      if (tags.length > 0) {
          tags = ArrayUtils.toArray("cache_tags:" + StringUtils.join(tags, '.'));
      }
      return tags;
  }

  public void count(String metricName, long countValue, String... tags) {
    underlyingClient.count(metricName, countValue, statsDConfig.getSampleRate(), customUnifiedTag(tags));
  }

  public void gauge(String metricName, long countValue, String... tags) {
    underlyingClient.gauge(metricName, countValue, customUnifiedTag(tags));
  }

  public void recordExecutionTime(String metricName, long timeDifferenceInMillis, String... tags) {
    underlyingClient.recordExecutionTime(metricName, timeDifferenceInMillis, statsDConfig.getSampleRate(), customUnifiedTag(tags));
  }

  public void recordExecutionTimeToNow(String metricName, long startTime, String... tags) {
    underlyingClient.recordExecutionTime(metricName, System.currentTimeMillis() - startTime,
        statsDConfig.getSampleRate(), customUnifiedTag(tags));
  }
  
  public com.timgroup.statsd.StatsDClient getUnderlying() {
    return underlyingClient;
  }
}
