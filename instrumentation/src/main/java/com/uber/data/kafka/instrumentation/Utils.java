package com.uber.data.kafka.instrumentation;

import java.util.Map;

public class Utils {
  /**
   * CopyTags from source array to destination array.
   *
   * @param destination array that should be preallocated to the correct size.
   * @param source array to copy tags from.
   * @return offset in the destination array that has been copied up to.
   */
  public static int copyTags(Map<String, String> destination, String... source) {
    String key = "unknown";
    for (int i = 0; i < source.length; i++) {
      String tag = source[i];
      if (i % 2 == 0) {
        key = tag;
      } else {
        destination.put(key, tag);
      }
    }
    return source.length;
  }
}
