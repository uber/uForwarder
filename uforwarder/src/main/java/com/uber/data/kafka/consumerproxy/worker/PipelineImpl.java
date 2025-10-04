package com.uber.data.kafka.consumerproxy.worker;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherImpl;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorImpl;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcher;
import com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PipelineImpl implements Pipeline which is the data-transfer pipeline. */
public final class PipelineImpl implements Pipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineImpl.class);
  private final KafkaFetcher<byte[], byte[]> fetcher;
  private final ProcessorImpl processor;
  private final DispatcherImpl dispatcher;
  private final PipelineStateManager stateManager;
  private final String pipelineId;

  public PipelineImpl(
      String pipelineId,
      KafkaFetcher<byte[], byte[]> fetcher,
      ProcessorImpl processor,
      DispatcherImpl dispatcher,
      PipelineStateManager stateManager) {
    this.pipelineId = pipelineId;
    this.fetcher = fetcher;
    this.processor = processor;
    this.dispatcher = dispatcher;
    this.stateManager = stateManager;
    setup(pipelineId, fetcher, processor, dispatcher, stateManager);
  }

  private static void setup(
      String pipelineId,
      KafkaFetcher<byte[], byte[]> fetcher,
      ProcessorImpl processor,
      DispatcherImpl dispatcher,
      PipelineStateManager configManager) {
    fetcher.setNextStage(processor);
    fetcher.setPipelineStateManager(configManager);
    processor.setNextStage(dispatcher);
    processor.setPipelineStateManager(configManager);
    dispatcher.setPipelineStateManager(configManager);
    LOGGER.debug("setup pipeline", StructuredLogging.pipelineId(pipelineId));
  }

  @Override
  public void start() {
    LOGGER.info("starting pipeline", StructuredLogging.pipelineId(pipelineId));
    dispatcher.start();
    processor.start();
    fetcher.start();
    LOGGER.info("started pipeline", StructuredLogging.pipelineId(pipelineId));
  }

  @Override
  public boolean isRunning() {
    return fetcher.isRunning() && processor.isRunning() && dispatcher.isRunning();
  }

  @Override
  public void stop() {
    LOGGER.info("stopping pipeline", StructuredLogging.pipelineId(pipelineId));
    fetcher.stop();
    processor.stop();
    dispatcher.stop();
    stateManager.clear();
    LOGGER.info("stopped pipeline", StructuredLogging.pipelineId(pipelineId));
  }

  public ProcessorImpl processor() {
    return processor;
  }

  private static BiConsumer<Void, Throwable> logCommand(String command, Job job) {
    return (v, t) -> {
      if (t != null) {
        LOGGER.warn(
            "failed to {} on pipeline",
            command,
            StructuredLogging.jobId(job.getJobId()),
            StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
            StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
            StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
            StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
      } else {
        LOGGER.info(
            "{} on pipeline",
            command,
            StructuredLogging.jobId(job.getJobId()),
            StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
            StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
            StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
            StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
      }
    };
  }

  private static BiConsumer<Void, Throwable> logCommand(String command) {
    return (v, t) -> {
      if (t != null) {
        LOGGER.warn("failed to {} on pipeline", command);
      } else {
        LOGGER.info("{} on pipeline", command);
      }
    };
  }

  @Override
  public CompletionStage<Void> run(Job job) {
    // skip dispatcher run b/c it does not have special behavior for each job

    // run job in pipeline
    // NOTE: run job in processor before stateManager, because if run job in stateManager before in
    // processor, the fetcher might pick up new jobs before processor, which causes the processor
    // to drop messages of those new jobs, which leads to data loss.
    CompletionStage<Void> result = processor.run(job);

    // add job to state manager
    result = result.thenCompose(r -> stateManager.run(job));

    // rerun job in processor to update quota
    // TODO(haitao.zhang): do we need to expose an update quota interface in processor? Or should
    //  the processor fetch quota update periodically?
    result = result.thenCompose(r -> processor.run(job));

    // signal fetcher to pickup updates to job state manager.
    result = result.thenCompose(r -> fetcher.signal());

    return result.whenComplete(logCommand(CommandType.COMMAND_TYPE_RUN_JOB.toString(), job));
  }

  @Override
  public CompletionStage<Void> update(Job job) {
    // skip dispatcher run b/c it does not have special behavior for each job

    // update job to state manager
    CompletionStage<Void> result = stateManager.update(job);

    // update job in pipeline
    result = result.thenCompose(r -> processor.update(job));

    // signal fetcher to pickup updates to job state manager.
    result = result.thenCompose(r -> fetcher.signal());

    return result.whenComplete(logCommand(CommandType.COMMAND_TYPE_UPDATE_JOB.toString(), job));
  }

  @Override
  public CompletionStage<Void> cancel(Job job) {
    // cancel job from state manager
    CompletionStage<Void> result = stateManager.cancel(job);

    // signal fetcher to pickup updates to job state manager.
    result = result.thenCompose(r -> fetcher.signal());

    // cancel job in pipeline
    result = result.thenCompose(r -> processor.cancel(job));

    // skip dispatcher cancel b/c it does not have special behavior for each job

    return result.whenComplete(logCommand(CommandType.COMMAND_TYPE_CANCEL_JOB.toString(), job));
  }

  @Override
  public CompletionStage<Void> cancelAll() {
    // cancelAll jobs to state manager
    // fetcher polls from state manager so this will cancel it in the fetcher.
    CompletionStage<Void> result = stateManager.cancelAll();

    // signal fetcher to pickup updates to job state manager.
    result = result.thenCompose(r -> fetcher.signal());

    // cancelAll job in pipeline
    result = result.thenCompose(r -> processor.cancelAll());

    // skip dispatcher cancelAll b/c it does not have special behavior for each job

    return result.whenComplete(logCommand("cancelall"));
  }

  @Override
  public Collection<JobStatus> getJobStatus() {
    // return pipeline state from state manager
    // fetcher will update actual state in the state manager.
    return stateManager.getJobStatus();
  }

  @Override
  public Collection<Job> getJobs() {
    // return pipeline state from state manager
    // fetcher will update actual state in the state manager.
    return stateManager.getJobs();
  }

  @Override
  public void publishMetrics() {
    processor.publishMetrics();
    stateManager.publishMetrics();
  }
}
