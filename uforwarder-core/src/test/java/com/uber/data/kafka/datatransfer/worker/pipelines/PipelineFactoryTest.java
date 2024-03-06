package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PipelineFactoryTest extends FievelTestBase {

  private static class PipelineFactoryTestImpl implements PipelineFactory {
    @Override
    public Pipeline createPipeline(String pipelineId, Job job) {
      return Mockito.mock(Pipeline.class);
    }

    @Override
    public String getPipelineId(Job job) {
      return "pipelineId";
    }
  }

  @Test
  public void testGetThreadName() {
    PipelineFactory factory = new PipelineFactoryTestImpl();
    Job job = Mockito.mock(Job.class);
    Assert.assertEquals(
        "serviceName__pipelineId__role", factory.getThreadName(job, "role", "serviceName"));
  }
}
