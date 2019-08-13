package com.uber.buckcache.datastore.impl.ignite;

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.events.EventType;

public class IgniteConstants {

  public static final String KEYS_CACHE_NAME = "keys-v1";
  public static final String KEYS_REVERSE_CACHE_NAME = "keys-reverse-v1";
  public static final String METADATA_CACHE_NAME = "metadata-v1";
  public static final String ARTIFACT_CACHE_NAME = "artifacts-v1";
  public static final String UNDERLYING_KEY_SEQUENCE_NAME = "underlyingArtifactKeys-v1";

  public static final Map<Integer, String> EVENT_TYPE_TO_NAME_MAP = new HashMap<Integer, String>() {
    {
      put(EventType.EVT_CACHE_OBJECT_EXPIRED, "EXPIRED");
      put(EventType.EVT_CACHE_OBJECT_REMOVED, "REMOVED");
      put(EventType.EVT_CACHE_ENTRY_EVICTED, "EVICTED");
      put(EventType.EVT_CACHE_ENTRY_DESTROYED, "DESTROYED");
    }
  };
}
