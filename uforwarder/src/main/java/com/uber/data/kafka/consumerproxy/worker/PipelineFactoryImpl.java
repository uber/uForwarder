package com.uber.data.kafka.consumerproxy.worker;

import com.uber.data.kafka.consumerproxy.utils.RetryUtils;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherImpl;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.LatencyTracker;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcher;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcherFactory;
import com.uber.data.kafka.consumerproxy.worker.fetcher.KafkaFetcherFactory;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorFactory;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorImpl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.ThreadRegister;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcher;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcherFactory;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcher;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineFactory;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PipelineFactoryImpl is used to create pipelines for the consumer proxy {@code
 * ConsumerRecord<byte[], byte[]>} is the data type sent from the fetcher to the processor Long is
 * the data type received by the fetcher from the processor DispatcherMessage is the data type sent
 * from the processor to the dispatcher DispatcherResponse is the data type received by the
 * processor from the dispatcher
 */
public class PipelineFactoryImpl implements PipelineFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineFactoryImpl.class);
  private static final String DELIMITER = "__";
  private static final String CONSUMER = "consumer";
  private static final String PRODUCER = "producer";
  private static final String PROCESSOR = "processor";
  private static final String DISPATCHER = "dispatcher";
  // TODO(T4902455): allow retry and dlq topics from any cluster
  private static final String DLQ = "dlq";
  private final String serviceName;
  private final CoreInfra infra;
  private final KafkaFetcherFactory kafkaFetcherFactory;
  private final ProcessorFactory processorFactory;
  private final GrpcDispatcherFactory grpcDispatcherFactory;
  private final KafkaDispatcherFactory<byte[], byte[]> kafkaDispatcherFactory;

  PipelineFactoryImpl(
      String serviceName,
      CoreInfra infra,
      KafkaFetcherFactory kafkaFetcherFactory,
      ProcessorFactory processorFactory,
      GrpcDispatcherFactory grpcDispatcherFactory,
      KafkaDispatcherFactory<byte[], byte[]> kafkaDispatcherFactory) {
    this.serviceName = serviceName;
    this.infra = infra;
    this.kafkaFetcherFactory = kafkaFetcherFactory;
    this.processorFactory = processorFactory;
    this.grpcDispatcherFactory = grpcDispatcherFactory;
    this.kafkaDispatcherFactory = kafkaDispatcherFactory;
  }

  @Override
  public Pipeline createPipeline(String pipelineId, Job job) {
    Optional<KafkaFetcher<byte[], byte[]>> fetcher = Optional.empty();
    Optional<ProcessorImpl> processor = Optional.empty();
    Optional<DispatcherImpl> dispatcher = Optional.empty();
    Optional<GrpcDispatcher> grpcDispatcher = Optional.empty();
    Optional<PipelineStateManager> kafkaPipelineStateManager = Optional.empty();
    ThreadRegister threadRegister = new ThreadRegister(infra.getThreadMXBean());
    try {
      kafkaPipelineStateManager =
          Optional.of(new KafkaPipelineStateManager(job, threadRegister::getUsage, infra.scope()));
      boolean isSecure = job.hasSecurityConfig() && job.getSecurityConfig().getIsSecure();
      fetcher =
          Optional.of(
              kafkaFetcherFactory.create(
                  job, getThreadName(job, CONSUMER, serviceName), threadRegister, infra));
      String processorId = getThreadName(job, PROCESSOR, serviceName);
      final String dispatcherId = getThreadName(job, DISPATCHER, serviceName);
      processor =
          Optional.of(processorFactory.create(job, processorId, threadRegister.asThreadFactory()));
      grpcDispatcher =
          Optional.of(
              grpcDispatcherFactory.create(
                  serviceName,
                  dispatcherId,
                  threadRegister.asThreadFactory(),
                  job.getRpcDispatcherTask().getUri(),
                  job.getRpcDispatcherTask().getProcedure()));
      final String clientId = getThreadName(job, PRODUCER, serviceName);
      Optional<KafkaDispatcher<byte[], byte[]>> resqKafkaProducer =
          RetryUtils.hasResqTopic(job)
              ? Optional.of(
                  kafkaDispatcherFactory.create(
                      clientId, job.getResqConfig().getResqCluster(), infra, isSecure, true))
              : Optional.empty();
      dispatcher =
          Optional.of(
              new DispatcherImpl(
                  infra,
                  grpcDispatcher.get(),
                  // DLQ always use lossless producer
                  kafkaDispatcherFactory.create(clientId, DLQ, infra, isSecure, false),
                  resqKafkaProducer,
                  new LatencyTracker(
                      processorFactory.getMaxInboundCacheCount(),
                      processorFactory.getMaxAckCommitSkew())));
      return new PipelineImpl(
          pipelineId,
          fetcher.get(),
          processor.get(),
          dispatcher.get(),
          kafkaPipelineStateManager.get());
    } catch (Exception e) {
      LOGGER.info("failed to create pipeline", e);
      kafkaPipelineStateManager.ifPresent(pipelineStateManager -> pipelineStateManager.cancel(job));
      fetcher.ifPresent(KafkaFetcher::stop);
      processor.ifPresent(ProcessorImpl::stop);
      grpcDispatcher.ifPresent(GrpcDispatcher::stop);
      dispatcher.ifPresent(DispatcherImpl::stop);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getPipelineId(Job job) {
    // use this as the pipeline id so jobs with the same (group, cluster, topic) are handled by the
    // same pipeline
    // replace characters to workaround M3 query limit
    return String.join(
        DELIMITER,
        job.getKafkaConsumerTask().getConsumerGroup(),
        job.getKafkaConsumerTask().getCluster(),
        job.getKafkaConsumerTask().getTopic());
  }
}
