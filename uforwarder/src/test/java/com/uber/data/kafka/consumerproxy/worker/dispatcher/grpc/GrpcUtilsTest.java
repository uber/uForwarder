package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GrpcUtilsTest {
  private final Tracer tracer = new MockTracer();
  private final ContextManager contextManager = Mockito.mock(ContextManager.class);
  private final GrpcDispatcherConfiguration config = new GrpcDispatcherConfiguration();
  private final CoreInfra infra =
      CoreInfra.builder().withTracer(tracer).withContextManager(contextManager).build();

  @Test
  public void testGetValidTimeout() {
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(600000);
    Job job = jobBuilder.build();
    Assertions.assertEquals(600000, GrpcUtils.getTimeout(job, config, 0, infra.scope()));
    Assertions.assertEquals(1200000, GrpcUtils.getTimeout(job, config, 1, infra.scope()));
    Assertions.assertEquals(1800000, GrpcUtils.getTimeout(job, config, 2, infra.scope()));
  }

  @Test
  public void testGetSmallTimeout() {
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(0);
    Job job = jobBuilder.build();
    Assertions.assertEquals(1, GrpcUtils.getTimeout(job, config, 0, infra.scope()));
    Assertions.assertEquals(2, GrpcUtils.getTimeout(job, config, 1, infra.scope()));
    Assertions.assertEquals(4, GrpcUtils.getTimeout(job, config, 2, infra.scope()));
    Assertions.assertEquals(1800000, GrpcUtils.getTimeout(job, config, 21, infra.scope()));
  }

  @Test
  public void testGetLargeTimeout() {
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1800001);
    Job job = jobBuilder.build();
    Assertions.assertEquals(1800000, GrpcUtils.getTimeout(job, config, 0, infra.scope()));
    Assertions.assertEquals(1800000, GrpcUtils.getTimeout(job, config, 1, infra.scope()));
    Assertions.assertEquals(1800000, GrpcUtils.getTimeout(job, config, 2, infra.scope()));
  }
}
