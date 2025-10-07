package com.uber.data.kafka.consumerproxy.management;

import com.uber.data.kafka.consumerproxy.worker.PipelineImpl;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorImpl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class WorkerStubsJsonTest {
  @Test
  public void test() throws Exception {
    PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    ProcessorImpl processor = Mockito.mock(ProcessorImpl.class);
    PipelineImpl pipelineOne = Mockito.mock(PipelineImpl.class);
    Mockito.doReturn(Collections.singletonMap("pipeline", pipelineOne))
        .when(pipelineManager)
        .getPipelines();
    Mockito.doReturn(processor).when(pipelineOne).processor();
    Map<Job, Map<Long, MessageStub>> stubs = new HashMap<>();
    stubs.put(Job.getDefaultInstance(), Collections.singletonMap(0L, new MessageStub()));
    Mockito.doReturn(stubs).when(processor).getStubs();

    WorkerStubsJson workerStubsJson = new WorkerStubsJson(pipelineManager);
    Assertions.assertNotNull(workerStubsJson.read());
  }
}
