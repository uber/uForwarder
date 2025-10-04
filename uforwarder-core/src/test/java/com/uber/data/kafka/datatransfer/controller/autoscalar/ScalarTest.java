package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.protobuf.MessageOrBuilder;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ScalarTest extends FievelTestBase {

  Scalar scalar = Scalar.DEFAULT;
  RebalancingJobGroup rebalancingJobGroup;

  @Before
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
    Assert.assertNotNull(msg);
  }

  @Test
  public void testOnLoad() {
    scalar.onLoad(1.0);
  }
}
