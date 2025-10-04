package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.concurrent.ThreadSafe;
import net.logstash.logback.argument.StructuredArgument;
import net.logstash.logback.argument.StructuredArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class KafkaPipelineStateManager implements PipelineStateManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPipelineStateManager.class);
  private static final double MINIMUM_VALID_RATE = 1.0;

  private static final double DEFAULT_BYTE_RATE = Double.MAX_VALUE;
  private static final double MINIMUM_VALID_INFLIGHT_MESSAGES = 1.0;
  private static final FlowControl MINIMUM_VALID_FLOW =
      FlowControl.newBuilder()
          .setMessagesPerSec(MINIMUM_VALID_RATE)
          // we need to set the default byteRate to Double.MAX so that
          // stale jobs being processed will not be blocked when messages are submitted to
          // rateLimiter
          .setBytesPerSec(DEFAULT_BYTE_RATE)
          .setMaxInflightMessages(MINIMUM_VALID_INFLIGHT_MESSAGES)
          .build();

  private final Job jobTemplate;

  // The currently running jobs map.
  // It is changed by add(), update(), cancel(), cancelAll(), i.e., it is changed when the master
  // issues new commands.
  private final ConcurrentMap<Long, Job> expectedRunningJobMap = new ConcurrentHashMap<>();
  // The job status map.
  // It is changed by updateStatus().
  private ImmutableList<JobStatus> actualRunningJobStatusList = ImmutableList.of();
  private final Object updateMapLock = new Object();
  private final Object publishMetricsLock = new Object();

  private final Scope scope;

  private final PipelineHealthManager healthManager;
  private volatile FlowControl flowControl;

  public KafkaPipelineStateManager(Job job, Scope scope) {
    this.jobTemplate = job;
    this.scope = scope;
    this.flowControl = MINIMUM_VALID_FLOW;
    this.healthManager = PipelineHealthManager.newBuilder().setScope(scope).build(job);
  }

  @Override
  public void updateActualRunningJobStatus(List<JobStatus> jobStatusList) {
    actualRunningJobStatusList = ImmutableList.copyOf(jobStatusList);
  }

  @Override
  public Map<Long, Job> getExpectedRunningJobMap() {
    // get the update map lock in case the job is removed and added back again.
    synchronized (updateMapLock) {
      return ImmutableMap.copyOf(expectedRunningJobMap);
    }
  }

  @Override
  public boolean shouldJobBeRunning(Job job) {
    // get the update map lock in case the job is removed and added back again.
    synchronized (updateMapLock) {
      return expectedRunningJobMap.containsKey(job.getJobId());
    }
  }

  @Override
  public Optional<Job> getExpectedJob(long jobId) {
    // get the update map lock in case the job is removed and added back again.
    synchronized (updateMapLock) {
      Job job = expectedRunningJobMap.get(jobId);
      return Optional.ofNullable(job);
    }
  }

  @Override
  public Job getJobTemplate() {
    return jobTemplate;
  }

  @Override
  public FlowControl getFlowControl() {
    return flowControl;
  }

  @Override
  public void clear() {
    LOGGER.info("clearing pipeline state manager");
    synchronized (updateMapLock) {
      expectedRunningJobMap.clear();
    }
    flowControl = MINIMUM_VALID_FLOW;
    actualRunningJobStatusList = ImmutableList.of();
    LOGGER.info("cleared pipeline state manager");
  }

  private static String[] instrumentationTags(Job job, String command) {
    List<String> tags = new ArrayList<>();
    tags.add(StructuredFields.KAFKA_GROUP);
    tags.add(job.getKafkaConsumerTask().getConsumerGroup());
    tags.add(StructuredFields.KAFKA_CLUSTER);
    tags.add(job.getKafkaConsumerTask().getCluster());
    tags.add(StructuredFields.KAFKA_TOPIC);
    tags.add(job.getKafkaConsumerTask().getTopic());
    tags.add(StructuredFields.KAFKA_PARTITION);
    tags.add(Integer.toString(job.getKafkaConsumerTask().getPartition()));
    if (job.hasRpcDispatcherTask()) {
      tags.add(StructuredFields.URI);
      tags.add(RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri()));
    }
    tags.add("command");
    tags.add(command);
    return tags.toArray(new String[0]);
  }

  private static StructuredArgument[] loggingTags(Job job, String command) {
    List<StructuredArgument> keyValues = new ArrayList<>();
    keyValues.add(
        StructuredArguments.keyValue(
            StructuredFields.KAFKA_GROUP, job.getKafkaConsumerTask().getConsumerGroup()));
    keyValues.add(
        StructuredArguments.keyValue(
            StructuredFields.KAFKA_CLUSTER, job.getKafkaConsumerTask().getCluster()));
    keyValues.add(
        StructuredArguments.keyValue(
            StructuredFields.KAFKA_TOPIC, job.getKafkaConsumerTask().getTopic()));
    keyValues.add(
        StructuredArguments.keyValue(
            StructuredFields.KAFKA_PARTITION,
            Integer.toString(job.getKafkaConsumerTask().getPartition())));
    if (job.hasRpcDispatcherTask()) {
      keyValues.add(
          StructuredArguments.keyValue(
              StructuredFields.URI,
              RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri())));
    }
    keyValues.add(StructuredArguments.keyValue("command", command));
    return keyValues.toArray(new StructuredArgument[0]);
  }

  private CompletionStage<Void> instrument(Job job, String command, Runnable runnable) {
    return Instrumentation.instrument.withExceptionalCompletion(
        LOGGER,
        scope,
        () -> {
          CompletableFuture<Void> future = new CompletableFuture<>();
          try {
            runnable.run();
            future.complete(null);
            LOGGER.info("pipeline.command", loggingTags(job, command));
          } catch (Exception e) {
            future.completeExceptionally(e);
          } finally {
            return future;
          }
        },
        "pipeline.command",
        instrumentationTags(job, command));
  }

  @Override
  public void reportIssue(Job job, PipelineHealthIssue issue) {
    healthManager.reportIssue(job, issue);
  }

  // TODO (T4367183): handle the case that the same job definition is assigned to multiple job_ids.
  @Override
  public CompletionStage<Void> run(Job job) {
    return instrument(
        job,
        "run",
        () -> {
          healthManager.init(job);
          assertFlowPositive(job);
          assertValidConsumerGroup(job);
          synchronized (updateMapLock) {
            // remove the old job
            expectedRunningJobMap.remove(job.getJobId());

            // add the new job
            expectedRunningJobMap.put(job.getJobId(), job);

            // update quota
            handleFlowChange();
          }
        });
  }

  // TODO (T4367183): handle the case that the same job definition is assigned to multiple job_ids.
  @Override
  public CompletionStage<Void> cancel(Job job) {
    return instrument(
        job,
        "cancel",
        () -> {
          assertFlowPositive(job);
          assertValidConsumerGroup(job);
          synchronized (updateMapLock) {
            assertValidConsumerGroup(job);
            synchronized (updateMapLock) {
              // remove the old job
              if (expectedRunningJobMap.remove(job.getJobId()) != null) {
                // update quota
                handleFlowChange();
              }
            }
          }
          healthManager.cancel(job);
        });
  }

  // TODO (T4367183): handle the case that the same job definition is assigned to multiple job_ids.
  @Override
  public CompletionStage<Void> update(Job job) {
    return instrument(
        job,
        "update",
        () -> {
          assertFlowPositive(job);
          assertValidConsumerGroup(job);
          synchronized (updateMapLock) {
            // remove the old job
            if (expectedRunningJobMap.remove(job.getJobId()) != null) {
              // add the new job
              expectedRunningJobMap.put(job.getJobId(), job);
              // update quota
              handleFlowChange();
            }
          }
        });
  }

  @Override
  public CompletionStage<Void> cancelAll() {
    return Instrumentation.instrument.withExceptionalCompletion(
        LOGGER,
        scope,
        () -> {
          CompletableFuture<Void> future = new CompletableFuture<>();
          try {
            healthManager.cancelAll();
            synchronized (updateMapLock) {
              expectedRunningJobMap.clear();
              handleFlowChange();
            }
            future.complete(null);
            LOGGER.info("pipeline.command", StructuredArguments.keyValue("command", "cancelall"));
          } catch (Throwable e) {
            future.completeExceptionally(e);
          } finally {
            return future;
          }
        },
        "pipeline.command",
        "command",
        "cancelall");
  }

  @Override
  public List<JobStatus> getJobStatus() {
    return Instrumentation.instrument.withRuntimeException(
        LOGGER, scope, () -> actualRunningJobStatusList, "pipeline.getall");
  }

  private void assertValidConsumerGroup(Job job) {
    KafkaConsumerTask task = job.getKafkaConsumerTask();
    if (!task.getConsumerGroup().equals(jobTemplate.getKafkaConsumerTask().getConsumerGroup())) {
      LOGGER.error(
          "consumer group for job  doesn't match consumer group for fetcher thread",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaTopic(task.getTopic()),
          StructuredLogging.kafkaPartition(task.getPartition()),
          StructuredLogging.kafkaCluster(task.getCluster()),
          StructuredLogging.kafkaGroup(task.getConsumerGroup()));
      scope
          .tagged(
              StructuredTags.builder()
                  .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                  .setKafkaTopic(job.getKafkaConsumerTask().getTopic())
                  .setKafkaPartition(job.getKafkaConsumerTask().getPartition())
                  .build())
          .counter(MetricNames.JOB_ASSIGNMENT_WRONG)
          .inc(1);
      throw new RuntimeException(
          String.format(
              "consumer group for task is %s doesn't match consumer group for fetcher thread %s",
              task.getConsumerGroup(), jobTemplate.getKafkaConsumerTask().getConsumerGroup()));
    }
  }

  private void assertFlowPositive(Job job) {
    FlowControl flow = job.getFlowControl();
    if (flow.getBytesPerSec() <= 0 || flow.getMessagesPerSec() <= 0) {
      KafkaConsumerTask task = job.getKafkaConsumerTask();
      LOGGER.error(
          "flow control is not positive",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaTopic(task.getTopic()),
          StructuredLogging.kafkaPartition(task.getPartition()),
          StructuredLogging.kafkaCluster(task.getCluster()),
          StructuredLogging.kafkaGroup(task.getConsumerGroup()));
      scope
          .tagged(
              StructuredTags.builder()
                  .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                  .setKafkaTopic(job.getKafkaConsumerTask().getTopic())
                  .setKafkaPartition(job.getKafkaConsumerTask().getPartition())
                  .build())
          .counter(MetricNames.QUOTA_INVALID)
          .inc(1);
      throw new RuntimeException(
          String.format("flow control for job %d is not positive", job.getJobId()));
    }
  }

  private void handleFlowChange() {
    double messagesPerSecTotal = 0;
    double bytesPerSecTotal = 0;
    double maxInflightTotal = 0;
    for (Job job : expectedRunningJobMap.values()) {
      messagesPerSecTotal += job.getFlowControl().getMessagesPerSec();
      bytesPerSecTotal += job.getFlowControl().getBytesPerSec();
      maxInflightTotal += job.getFlowControl().getMaxInflightMessages();
    }
    // as the rate limit should be positive, so we change it to be 1 when it's 0
    FlowControl.Builder flowControlBuilder = FlowControl.newBuilder();
    if (messagesPerSecTotal == 0) {
      flowControlBuilder.setMessagesPerSec(MINIMUM_VALID_RATE);
    } else {
      flowControlBuilder.setMessagesPerSec(messagesPerSecTotal);
    }
    if (bytesPerSecTotal == 0) {
      flowControlBuilder.setBytesPerSec(DEFAULT_BYTE_RATE);
    } else {
      flowControlBuilder.setBytesPerSec(bytesPerSecTotal);
    }
    if (maxInflightTotal == 0) {
      flowControlBuilder.setMaxInflightMessages(MINIMUM_VALID_INFLIGHT_MESSAGES);
    } else {
      flowControlBuilder.setMaxInflightMessages(maxInflightTotal);
    }
    flowControl = flowControlBuilder.build();
    LOGGER.info(
        "the total messages per second quota is " + flowControl.getMessagesPerSec(),
        StructuredLogging.kafkaGroup(jobTemplate.getKafkaConsumerTask().getConsumerGroup()),
        StructuredLogging.kafkaTopic(jobTemplate.getKafkaConsumerTask().getTopic()));
    LOGGER.info(
        "the total message bytes per second quota is " + flowControl.getBytesPerSec(),
        StructuredLogging.kafkaGroup(jobTemplate.getKafkaConsumerTask().getConsumerGroup()),
        StructuredLogging.kafkaTopic(jobTemplate.getKafkaConsumerTask().getTopic()));
    LOGGER.info(
        "the total message max inflight quota is " + flowControl.getMaxInflightMessages(),
        StructuredLogging.kafkaGroup(jobTemplate.getKafkaConsumerTask().getConsumerGroup()),
        StructuredLogging.kafkaTopic(jobTemplate.getKafkaConsumerTask().getTopic()));
  }

  @Override
  public void publishMetrics() {
    synchronized (publishMetricsLock) {
      for (Job job : expectedRunningJobMap.values()) {
        StructuredTags structuredTags =
            StructuredTags.builder()
                .setKafkaCluster(job.getKafkaConsumerTask().getCluster())
                .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                .setKafkaTopic(job.getKafkaConsumerTask().getTopic())
                .setKafkaPartition(job.getKafkaConsumerTask().getPartition());
        if (job.hasRpcDispatcherTask()) {
          structuredTags.setURI(RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri()));
        }
        if (job.hasMiscConfig()) {
          structuredTags.setConsumerService(job.getMiscConfig().getOwnerServiceName());
        }
        Scope jobScope = scope.tagged(structuredTags.build());
        jobScope
            .gauge(MetricNames.TOPIC_PARTITION_MESSAGE_QUOTA)
            .update(job.getFlowControl().getMessagesPerSec());
        jobScope
            .gauge(MetricNames.TOPIC_PARTITION_BYTE_QUOTA)
            .update(job.getFlowControl().getBytesPerSec());
        jobScope
            .gauge(MetricNames.TOPIC_PARTITION_MAX_INFLIGHT_QUOTA)
            .update(job.getFlowControl().getMaxInflightMessages());
      }
    }
    healthManager.publishMetrics();
  }

  private static class MetricNames {

    static final String JOB_ASSIGNMENT_WRONG = "pipeline.state-manager.job-assignment.wrong";
    static final String QUOTA_INVALID = "pipeline.state-manager.quota.invalid";
    static final String TOPIC_PARTITION_MESSAGE_QUOTA =
        "pipeline.state-manager.topic.partition.message-quota";
    static final String TOPIC_PARTITION_BYTE_QUOTA =
        "pipeline.state-manager.topic.partition.byte-quota";
    static final String TOPIC_PARTITION_MAX_INFLIGHT_QUOTA =
        "pipeline.state-manager.topic.partition.max-inflight-quota";
  }
}
