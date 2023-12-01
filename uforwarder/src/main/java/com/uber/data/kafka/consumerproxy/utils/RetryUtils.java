package com.uber.data.kafka.consumerproxy.utils;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Retry Queue related utility functions. */
public class RetryUtils {

  /**
   * Retrieves the kafka retry topic to publish the given message retry count
   *
   * @param job the job configuration
   * @param currentMessageRetryCount the retry count of current message
   * @return topicName
   */
  public static String getKafkaDestinationRetryTopic(Job job, long currentMessageRetryCount) {
    if (!isTieredRetryConfigAvailable(job)) {
      return job.getRpcDispatcherTask().getDlqTopic();
    }
    RetryConfig retryQueueConfig = job.getRetryConfig();
    List<RetryQueue> sortedRetryQueues = getSortedRetryQueues(retryQueueConfig);
    int currentCeilingIndex = 0;
    for (RetryQueue rtq : sortedRetryQueues) {
      currentCeilingIndex += rtq.getMaxRetryCount();
      if (currentMessageRetryCount <= currentCeilingIndex) {
        return rtq.getRetryQueueTopic();
      }
    }

    // return the last retry queue in case dlq topic is empty
    String dlqTopic = job.getRpcDispatcherTask().getDlqTopic();
    if (dlqTopic.isEmpty()) {
      return sortedRetryQueues.get(sortedRetryQueues.size() - 1).getRetryQueueTopic();
    }
    return dlqTopic;
  }

  /**
   * Sorts retry queues based on their getProcessingDelayMs
   *
   * @param retryQueueConfig which contains retry queues
   * @return sorted Retry queue list
   */
  public static List<RetryQueue> getSortedRetryQueues(RetryConfig retryQueueConfig) {

    return Stream.of(retryQueueConfig.getRetryQueuesList())
        .flatMap(Collection::stream)
        .sorted(Comparator.comparingInt(RetryQueue::getProcessingDelayMs))
        .collect(Collectors.toList());
  }

  /**
   * Validates if job contains Retry Queue configuration
   *
   * @param job the job with retry queue configuration
   * @return a boolean indicating if the job has TRQ config available
   */
  public static boolean isTieredRetryConfigAvailable(Job job) {
    return job.hasRetryConfig()
        && job.getRetryConfig().getRetryEnabled()
        && !job.getRetryConfig().getRetryQueuesList().isEmpty();
  }

  /**
   * Finds the retry queue with its topic name
   *
   * @param job the job containing retry queue config
   * @param retryTopicName the name of the retry topic
   * @return the founded retry queue, could be null
   */
  public static Optional<RetryQueue> findRetryQueueWithTopicName(Job job, String retryTopicName) {
    if (!isTieredRetryConfigAvailable(job)) {
      return Optional.empty();
    }
    return job.getRetryConfig()
        .getRetryQueuesList()
        .stream()
        .filter(rq -> rq.getRetryQueueTopic().equals(retryTopicName))
        .findFirst();
  }

  // TODO (T4576211): we compare topic with RQ in config in order to avoid introducing additional
  //  metadata into the system from the legacy system. We should remove this once we've migrated to
  //  header based DLQ metadata.
  public static boolean isRetryTopic(String topic, Job job) {
    return RetryUtils.findRetryQueueWithTopicName(job, topic).isPresent();
  }

  // TODO (T4576211): we compare topic with DLQ in config in order to avoid introducing additional
  //  metadata into the system from the legacy system. We should remove this once we've migrated to
  //  header based DLQ metadata.
  public static boolean isDLQTopic(String topic, Job job) {
    return job.hasRpcDispatcherTask() && job.getRpcDispatcherTask().getDlqTopic().equals(topic);
  }

  // TODO (T4576211): we compare topic with RESQ in config in order to avoid introducing additional
  //  metadata into the system from the legacy system. We should remove this once we've migrated to
  //  header based DLQ metadata.
  public static boolean isResqTopic(String topic, Job job) {
    return job.hasResqConfig() && job.getResqConfig().getResqTopic().equals(topic);
  }

  /**
   * Job has retry queue enabled
   *
   * @param job
   * @return
   */
  public static boolean hasRetryTopic(Job job) {
    return !job.getRpcDispatcherTask().getRetryQueueTopic().isEmpty()
        || RetryUtils.isTieredRetryConfigAvailable(job);
  }

  /**
   * Job has resilient queue enabled
   *
   * @param job
   * @return
   */
  public static boolean hasResqTopic(Job job) {
    return job.hasResqConfig()
        && job.getResqConfig().getResqEnabled()
        && !job.getResqConfig().getResqTopic().isEmpty();
  }
}
