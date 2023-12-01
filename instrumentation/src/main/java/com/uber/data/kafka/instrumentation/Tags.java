package com.uber.data.kafka.instrumentation;

/**
 * Tags is a common set of constants key-value tags to be used in structured Json logging and
 * metrics tags.
 */
public class Tags {
  public static class Key {
    public static final String code = "code";
    public static final String reason = "reason";
    public static final String result = "result";
  }

  public static class Value {
    public static final String success = "success";
    public static final String failure = "failure";
  }
}
