package com.uber.data.kafka.datatransfer.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * JobPodAssigner is responsible for determining which pod a job should be assigned to based on the
 * topic partition information.
 */
public class JobPodAssigner {
  public static final JobPodAssigner NoopJobPodAssigner = new JobPodAssigner();

  /** Protected constructor to prevent instantiation. */
  protected JobPodAssigner() {}

  /**
   * Assigns a job pod based on the topic partition information.
   *
   * @param topicPartitionInfo the topic partition information
   * @return the pod assignment string, or empty string if no assignment can be determined
   */
  public String assignJobPod(TopicPartitionInfo topicPartitionInfo) {
    return StringUtils.EMPTY;
  }
}
