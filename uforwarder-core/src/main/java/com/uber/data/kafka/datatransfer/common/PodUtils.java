package com.uber.data.kafka.datatransfer.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to support pod isolation TODO: remove this class and replace with the PodUtils from
 * Kafka library when it's released
 */
public class PodUtils {
  private static final Pattern POD_RACK_REGEX = Pattern.compile("^(.*)::(.*)$");
  private static final int GROUP_INDEX_POD = 1;
  private static final String EMPTY_STRING = "";

  private PodUtils() {}

  /**
   * Decodes pod from podRack String, if the input is not podRack encoded string returns empty
   * string
   *
   * @param value the value
   * @return the string
   */
  public static String podOf(String value) {
    if (value == null) {
      return EMPTY_STRING;
    }

    Matcher matcher = POD_RACK_REGEX.matcher(value);
    if (matcher.matches()) {
      return matcher.group(GROUP_INDEX_POD);
    }

    return EMPTY_STRING;
  }
}
