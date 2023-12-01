package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import java.util.Objects;

/** JobGroupKey identifies a job group */
public class JobGroupKey {
  private final String cluster;
  private final String topic;
  private final String group;

  private JobGroupKey(String cluster, String topic, String group) {
    this.cluster = cluster;
    this.topic = topic;
    this.group = group;
  }

  /**
   * extract job group key from a job
   *
   * @param job the job
   * @return the job group key
   */
  public static JobGroupKey of(Job job) {
    return new JobGroupKey(
        job.getKafkaConsumerTask().getCluster(),
        job.getKafkaConsumerTask().getTopic(),
        job.getKafkaConsumerTask().getConsumerGroup());
  }

  /**
   * extra job group key from a job group
   *
   * @param jobGroup the job group
   * @return the job group key
   */
  public static JobGroupKey of(JobGroup jobGroup) {
    return new JobGroupKey(
        jobGroup.getKafkaConsumerTaskGroup().getCluster(),
        jobGroup.getKafkaConsumerTaskGroup().getTopic(),
        jobGroup.getKafkaConsumerTaskGroup().getConsumerGroup());
  }

  /**
   * Gets kafka cluster id of the job group
   *
   * @return the cluster
   */
  public String getCluster() {
    return cluster;
  }

  /**
   * Gets topic.
   *
   * @return the topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * Gets group.
   *
   * @return the group
   */
  public String getGroup() {
    return group;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobGroupKey that = (JobGroupKey) o;
    return Objects.equals(cluster, that.cluster)
        && Objects.equals(topic, that.topic)
        && Objects.equals(group, that.group);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cluster, topic, group);
  }
}
