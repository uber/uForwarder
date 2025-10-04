package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PipelineFactoryTest {

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
    Assertions.assertEquals(
        "serviceName__pipelineId__role", factory.getThreadName(job, "role", "serviceName"));
  }
}
