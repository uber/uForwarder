package com.uber.data.kafka.datatransfer.common;

import static org.junit.Assert.assertEquals;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;

public class PodUtilsTest extends FievelTestBase {
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
