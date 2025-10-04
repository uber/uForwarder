package com.uber.data.kafka.datatransfer.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JobStatusJsonTest {
  @Test
  public void testRead() throws Exception {
    PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    Pipeline pipelineOne = Mockito.mock(Pipeline.class);
    Mockito.doReturn(ImmutableList.of(JobStatus.newBuilder().build()))
        .when(pipelineOne)
        .getJobStatus();
    Pipeline pipelineTwo = Mockito.mock(Pipeline.class);
    Mockito.doReturn(ImmutableList.of()).when(pipelineTwo).getJobStatus();
    Mockito.doReturn(
            ImmutableMap.of(
                "pipelineOne", pipelineOne,
                "pipelineTwo", pipelineTwo))
        .when(pipelineManager)
        .getPipelines();
    JobStatusJson jobStatusJson =
        new JobStatusJson(pipelineManager, JsonFormat.TypeRegistry.getEmptyTypeRegistry());
    Assertions.assertNotNull(jobStatusJson.read());
  }
}
