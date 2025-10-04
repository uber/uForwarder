package com.uber.data.kafka.consumerproxy.worker;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcher;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcherFactory;
import com.uber.data.kafka.consumerproxy.worker.fetcher.KafkaFetcherFactory;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorFactory;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorImpl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.ThreadRegister;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcher;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcherFactory;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcher;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineStateManager;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.concurrent.ThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class PipelineFactoryImplTest extends FievelTestBase {
  private static final String SERVICE_NAME = "service-name";
  private static final String CLUSTER = "dca1a";
  private static final String GROUP = "group";
  private static final String TOPIC = "topic";
  private static final String RESQ_TOPIC = "topic__resq";
  private static final int PARTITION = 0;
  private static final String DELIMITER = "__";
  private static final String uri = "dns://localhost:8080";
  private PipelineFactoryImpl pipelineFactory;
  private Job job;
  private KafkaFetcherFactory kafkaFetcherFactory;
  private KafkaDispatcherFactory kafkaDispatcherFactory;
  private GrpcDispatcherFactory grpcDispatcherFactory;
  private ProcessorFactory processorFactory;
  private CoreInfra infra;
  private KafkaFetcher kafkaFetcher;
  private ProcessorImpl processor;
  private GrpcDispatcher grpcDispatcher;
  private MockedConstruction<KafkaPipelineStateManager> kafkaPipelineStateManagerMockedConstruction;

  @Before
  public void setUp() throws Exception {
    infra = CoreInfra.NOOP;
    kafkaFetcher = Mockito.mock(KafkaFetcher.class);
    grpcDispatcher = Mockito.mock(GrpcDispatcher.class);
    kafkaFetcherFactory = Mockito.mock(KafkaFetcherFactory.class);
    processor = Mockito.mock(ProcessorImpl.class);
    Mockito.doReturn(kafkaFetcher)
        .when(kafkaFetcherFactory)
        .create(
            Mockito.any(), Mockito.anyString(), Mockito.any(ThreadRegister.class), Mockito.any());
    kafkaDispatcherFactory = Mockito.mock(KafkaDispatcherFactory.class);
    Mockito.doReturn(Mockito.mock(KafkaDispatcher.class))
        .when(kafkaDispatcherFactory)
        .create(
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.eq(false),
            Mockito.anyBoolean());
    grpcDispatcherFactory = Mockito.mock(GrpcDispatcherFactory.class);
    kafkaPipelineStateManagerMockedConstruction =
        Mockito.mockConstruction(KafkaPipelineStateManager.class);
    Mockito.doReturn(grpcDispatcher)
        .when(grpcDispatcherFactory)
        .create(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    processorFactory = Mockito.mock(ProcessorFactory.class);
    Mockito.doReturn(processor)
        .when(processorFactory)
        .create(Mockito.any(Job.class), Mockito.anyString(), Mockito.any(ThreadFactory.class));
    pipelineFactory =
        new PipelineFactoryImpl(
            SERVICE_NAME,
            infra,
            kafkaFetcherFactory,
            processorFactory,
            grpcDispatcherFactory,
            kafkaDispatcherFactory);
    job =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster(CLUSTER)
                    .setConsumerGroup(GROUP)
                    .setTopic(TOPIC)
                    .setPartition(PARTITION)
                    .build())
            .setResqConfig(
                ResqConfig.newBuilder()
                    .setResqTopic(RESQ_TOPIC)
                    .setResqEnabled(true)
                    .setResqCluster(CLUSTER)
                    .build())
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().setUri(uri).build())
            .build();
  }

  @After
  public void cleanup() {
    kafkaPipelineStateManagerMockedConstruction.close();
  }

  @Test
  public void testGetPipelineId() {
    String pipelineID = pipelineFactory.getPipelineId(job);
    Assert.assertEquals(GROUP + DELIMITER + CLUSTER + DELIMITER + TOPIC, pipelineID);
  }

  @Test
  public void testCreatePipeline() {
    // test create pipeline with OriginalTopicKafkaFetcher
    pipelineFactory.createPipeline(pipelineFactory.getPipelineId(job), job);
    // test create pipeline with RetryTopicKafkaFetcher
    Job job1 =
        Job.newBuilder(job)
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().setRetryQueueTopic(TOPIC).build())
            .build();
    pipelineFactory.createPipeline(pipelineFactory.getPipelineId(job1), job1);
    // test create pipeline with DlqTopicKafkaFetcher
    Job job2 =
        Job.newBuilder(job)
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().setDlqTopic(TOPIC).build())
            .build();
    pipelineFactory.createPipeline(pipelineFactory.getPipelineId(job2), job2);
  }

  @Test(expected = RuntimeException.class)
  public void testCreatePipelineWithException() throws Exception {
    Mockito.when(
            kafkaFetcherFactory.create(
                ArgumentMatchers.any(),
                ArgumentMatchers.anyString(),
                ArgumentMatchers.any(ThreadRegister.class),
                ArgumentMatchers.any()))
        .thenThrow(new Exception());
    pipelineFactory.createPipeline(pipelineFactory.getPipelineId(job), job);
  }

  @Test
  public void testCreatePipelineWithExceptionClosesComponents() throws Exception {
    Mockito.when(
            kafkaDispatcherFactory.create(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString(),
                ArgumentMatchers.any(),
                ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean()))
        .thenThrow(new Exception("failed to create dispatcher"));
    Exception e =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              pipelineFactory.createPipeline(pipelineFactory.getPipelineId(job), job);
            });
    Assert.assertEquals("java.lang.Exception: failed to create dispatcher", e.getMessage());
    Mockito.verify(kafkaFetcher, Mockito.atLeastOnce()).stop();
    Mockito.verify(processor, Mockito.atLeastOnce()).stop();
    Mockito.verify(grpcDispatcher, Mockito.atLeastOnce()).stop();
    Mockito.verify(
            kafkaPipelineStateManagerMockedConstruction.constructed().get(0), Mockito.times(1))
        .cancel(job);
  }
}
