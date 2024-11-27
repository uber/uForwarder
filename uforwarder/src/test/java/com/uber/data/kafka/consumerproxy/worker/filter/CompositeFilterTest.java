package com.uber.data.kafka.consumerproxy.worker.filter;

import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorMessage;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CompositeFilterTest extends FievelTestBase {
  private Filter compositeFilter;
  private Filter filter1;
  private Filter filter2;
  private Filter.Factory factory1;
  private Filter.Factory factory2;

  private Job job;
  private ProcessorMessage pm;

  @Before
  public void setup() {
    job = Job.newBuilder().setJobId(100).build();
    pm = Mockito.mock(ProcessorMessage.class);
    factory1 = Mockito.mock(Filter.Factory.class);
    factory2 = Mockito.mock(Filter.Factory.class);
    filter1 = Mockito.mock(Filter.class);
    filter2 = Mockito.mock(Filter.class);
    Mockito.when(filter1.shouldProcess(Mockito.any())).thenReturn(true);
    Mockito.when(filter2.shouldProcess(Mockito.any())).thenReturn(false);
    Mockito.when(factory1.create(job)).thenReturn(filter1);
    Mockito.when(factory2.create(job)).thenReturn(filter2);
  }

  @Test
  public void testEmptyFactory() {
    compositeFilter = CompositeFilter.newFactory(new Filter.Factory[] {}).create(job);
    Assert.assertEquals(Filter.NOOP_FILTER, compositeFilter);
  }

  @Test
  public void testTrueFilter() {
    compositeFilter = CompositeFilter.newFactory(factory1, factory1).create(job);
    Assert.assertTrue(compositeFilter.shouldProcess(ItemAndJob.of(pm, job)));
  }

  @Test
  public void testFalseFilter() {
    compositeFilter = CompositeFilter.newFactory(factory2, factory2).create(job);
    Assert.assertFalse(compositeFilter.shouldProcess(ItemAndJob.of(pm, job)));
  }

  @Test
  public void testTrueFalseFilter() {
    compositeFilter = CompositeFilter.newFactory(factory1, factory2).create(job);
    Assert.assertFalse(compositeFilter.shouldProcess(ItemAndJob.of(pm, job)));
  }

  @Test
  public void testFalseTrueFilter() {
    compositeFilter = CompositeFilter.newFactory(factory2, factory1).create(job);
    Assert.assertFalse(compositeFilter.shouldProcess(ItemAndJob.of(pm, job)));
  }
}
