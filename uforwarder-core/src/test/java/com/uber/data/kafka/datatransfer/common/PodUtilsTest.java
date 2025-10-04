package com.uber.data.kafka.datatransfer.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PodUtilsTest {
  @Test
  public void testGetPod() {
    String pod = PodUtils.podOf("pod-1::rack-1");
    assertEquals("pod-1", pod);
  }

  @Test
  public void testGetEmptyPod() {
    String pod = PodUtils.podOf("rack-1");
    assertEquals("", pod);
  }
}
