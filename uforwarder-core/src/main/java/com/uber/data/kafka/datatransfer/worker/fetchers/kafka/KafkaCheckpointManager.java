package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.facebook.infer.annotation.SynchronizedCollection;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.m3.tally.Scope;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** KafkaCheckpointManager uses kafka consumer group offset for topic partition offset management */
public class KafkaCheckpointManager extends AbstractCheckpointManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCheckpointManager.class);

  // the map will be a ConcurrentHashMap, so, multi-threaded access should not have any issues.
  // annotation indicates this info.
  private final @SynchronizedCollection Map<TopicPartition, CheckpointInfo> checkpointInfoMap;

  private final Scope scope;

  public KafkaCheckpointManager(Scope scope) {
    this.checkpointInfoMap = new ConcurrentHashMap<>();
    this.scope = scope;
  }

  @Override
  public CheckpointInfo getCheckpointInfo(Job job) {
    return getOrCreateOrRefreshCheckpointInfo(job, false);
  }

  @Override
  public long getOffsetToCommit(Job job) {
    preConditionCheck(job);

    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    final Scope scopeWithGroupTopicPartition =
        scope.tagged(
            StructuredTags.builder()
                .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                .setKafkaTopic(topicPartition.topic())
                .setKafkaPartition(topicPartition.partition())
                .build());
    CheckpointInfo checkpointInfo = checkpointInfoMap.get(topicPartition);
    if (checkpointInfo == null) {
      LOGGER.warn(
          "skip offset commit",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredLogging.kafkaTopic(topicPartition.topic()),
          StructuredLogging.kafkaPartition(topicPartition.partition()),
          StructuredLogging.reason("no checkpoint info"));
      scopeWithGroupTopicPartition.counter(MetricNames.OFFSET_COMMIT_SKIP).inc(1);
    } else if (checkpointInfo.getOffsetToCommit() <= KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT) {
      LOGGER.debug(
          "skip offset commit",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredLogging.kafkaTopic(topicPartition.topic()),
          StructuredLogging.kafkaPartition(topicPartition.partition()),
          StructuredLogging.reason("checkpoint info = -1"));
      scopeWithGroupTopicPartition.counter(MetricNames.OFFSET_COMMIT_SKIP).inc(1);
    } else if (checkpointInfo.getOffsetToCommit() < checkpointInfo.getCommittedOffset()) {
      LOGGER.debug(
          "skip offset commit",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredLogging.kafkaTopic(topicPartition.topic()),
          StructuredLogging.kafkaPartition(topicPartition.partition()),
          StructuredLogging.kafkaOffset(checkpointInfo.getOffsetToCommit()),
          StructuredLogging.reason("offset already committed"));
      scopeWithGroupTopicPartition.counter(MetricNames.OFFSET_COMMIT_SKIP).inc(1);
    } else if (checkpointInfo.getOffsetToCommit() == checkpointInfo.getCommittedOffset()) {
      LOGGER.debug(
          "commit offset already exist",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredLogging.kafkaTopic(topicPartition.topic()),
          StructuredLogging.kafkaPartition(topicPartition.partition()),
          StructuredLogging.kafkaOffset(checkpointInfo.getOffsetToCommit()),
          StructuredLogging.reason("offset already committed"));
      scopeWithGroupTopicPartition.counter(MetricNames.OFFSET_COMMIT_DUPLICATION).inc(1);
      return checkpointInfo.getOffsetToCommit();
    } else {
      return checkpointInfo.getOffsetToCommit();
    }
    return KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT;
  }

  @Override
  public CheckpointInfo addCheckpointInfo(Job job) {
    return getOrCreateOrRefreshCheckpointInfo(job, true);
  }

  private CheckpointInfo getOrCreateOrRefreshCheckpointInfo(Job job, boolean refresh) {
    preConditionCheck(job);
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    if (!checkpointInfoMap.containsKey(topicPartition) || refresh) {
      CheckpointInfo checkpointInfo =
          new CheckpointInfo(
              job,
              Math.max(
                  job.getKafkaConsumerTask().getStartOffset(), KafkaUtils.MAX_INVALID_START_OFFSET),
              job.getKafkaConsumerTask().getEndOffset() > KafkaUtils.MAX_INVALID_END_OFFSET
                  ? job.getKafkaConsumerTask().getEndOffset()
                  : null);
      checkpointInfoMap.put(topicPartition, checkpointInfo);
    }
    return checkpointInfoMap.get(topicPartition);
  }

  @Override
  public void close() {
    checkpointInfoMap.clear();
  }

  private static class MetricNames {

    static final String OFFSET_COMMIT_FAILURE = "fetcher.kafka.offset.commit.failure";
    static final String OFFSET_COMMIT_SKIP = "fetcher.kafka.offset.commit.skip";
    static final String OFFSET_COMMIT_DUPLICATION = "fetcher.kafka.offset.commit.duplication";
  }
}
