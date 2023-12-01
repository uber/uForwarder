package com.uber.data.kafka.datatransfer.common;

/** RoutingUtils contains helper methods for handling routing logic. */
public class RoutingUtils {
  private static final char[] INVALID_CHARS = {'@'};
  private static final String THREE_SLASHES = ":///";

  private RoutingUtils() {}

  /**
   * Returns the uri address in a routing url. Also handles the case when there are 3 slashes.
   *
   * <pre>
   *   e.g., muttley://my-routing-key -> my-routing-key.
   *         dns:///my-routing-key -> my-routing-key.
   * </pre>
   */
  public static String extractAddress(String uri) {
    uri = trimInvalidTail(uri);
    String spliter = "//";
    if (uri.contains(THREE_SLASHES)) {
      spliter = "///";
    }
    String[] parts = uri.split(spliter);
    String result = parts.length > 1 ? parts[1] : "unknown";
    return result;
  }

  /**
   * Trim off invalid part from tail of the uri
   *
   * @param uri
   * @return
   */
  private static String trimInvalidTail(String uri) {
    int result = uri.length();
    for (char ch : INVALID_CHARS) {
      int index = uri.indexOf(ch);
      if (index != -1) {
        result = Math.min(result, index);
      }
    }

    return uri.substring(0, result);
  }
}
