package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.protobuf.MessageOrBuilder;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ScalarTest {

  Scalar scalar = Scalar.DEFAULT;
  RebalancingJobGroup rebalancingJobGroup;

  @BeforeEach
  public void setup() {
    rebalancingJobGroup = Mockito.mock(RebalancingJobGroup.class);
  }

  @Test
  public void testDefaultApply() {
    scalar.apply(rebalancingJobGroup, 1.0);
    Mockito.verify(rebalancingJobGroup, Mockito.times(1)).updateScale(1.0, Throughput.ZERO);
  }

  @Test
  public void testSnapshot() {
    MessageOrBuilder msg = scalar.snapshot();
    Assertions.assertNotNull(msg);
  }

  @Test
  public void testOnLoad() {
    scalar.onLoad(1.0);
  }
}
