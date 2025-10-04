package com.uber.data.kafka.datatransfer.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.creator.JobCreator;
import com.uber.data.kafka.datatransfer.controller.storage.IdProvider;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.m3.tally.Scope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * KafkaPartitionExpansionWatcher will periodically poll Kafka broker via AdminClient to check for
 * the current partition count for all jobs registered to the master. If there is a mismatch, it
 * will add a new Job for that partition (for partition increase) or remove the Job for that
 * partition (for topic deletion).
 */
@ThreadSafe
public final class KafkaPartitionExpansionWatcher {
  private static final Logger logger =
      LoggerFactory.getLogger(KafkaPartitionExpansionWatcher.class);
  private static final Duration KAFKA_ADMIN_CLIENT_DESCRIBE_TOPICS_TIMEOUT = Duration.ofMinutes(1);

  private final CoreInfra infra;
  private final Store<String, StoredJobGroup> jobGroupStore;
  private final IdProvider<Long, StoredJob> jobIdProvider;
  private final JobCreator jobCreator;
  private final AdminClient.Builder adminBuilder;
  private final LeaderSelector leaderSelector;

  public KafkaPartitionExpansionWatcher(
      CoreInfra infra,
      Store<String, StoredJobGroup> jobGroupStore,
      IdProvider<Long, StoredJob> jobIdProvider,
      JobCreator jobCreator,
      AdminClient.Builder adminBuilder,
      LeaderSelector leaderSelector) {
    this.infra = infra;
    this.jobGroupStore = jobGroupStore;
    this.jobIdProvider = jobIdProvider;
    this.jobCreator = jobCreator;
    this.adminBuilder = adminBuilder;
    this.leaderSelector = leaderSelector;
  }

  @Scheduled(fixedDelayString = "${master.kafka.partition.expansion.watcher.interval}")
  public void watchPartitionExpansion() {
    Instrumentation.instrument.returnVoidCatchThrowable(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          if (!leaderSelector.isLeader()) {
            logger.debug("skipped watch because of current instance is not leader");
            return;
          }
          Map<String, Versioned<StoredJobGroup>> jobGroups = jobGroupStore.getAll();
          Map<String, List<String>> clusterTopicsMap = groupTopicsByCluster(jobGroups.values());
          Map<String, Map<String, AllPartitionInfo>> clusterTopicInfosMap =
              getPartitionInfosForTopics(clusterTopicsMap);
          updatePartitionCounts(jobGroups.values(), clusterTopicInfosMap);
        },
        "kafka-partition-expansion-watcher.watch");
  }

  private Map<String, List<String>> groupTopicsByCluster(
      Collection<Versioned<StoredJobGroup>> jobGroups) {
    return Instrumentation.instrument.withRuntimeException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          Map<String, List<String>> map = new HashMap<>();
          for (Versioned<StoredJobGroup> jobGroup : jobGroups) {
            String cluster =
                jobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getCluster();
            String topic = jobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getTopic();
            if (cluster.isEmpty() || topic.isEmpty()) {
              continue;
            }
            List<String> topicList = map.getOrDefault(cluster, new ArrayList<>());
            topicList.add(topic);
            map.put(cluster, topicList);
          }
          return map;
        },
        "kafka-partition-expansion-watcher.get-topics-by-cluster");
  }

  private Map<String, Map<String, AllPartitionInfo>> getPartitionInfosForTopics(
      Map<String, List<String>> clusterTopicMap) {
    ImmutableMap.Builder<String, Map<String, AllPartitionInfo>> clusterTopicPartitionMap =
        ImmutableMap.builder();
    for (Map.Entry<String, List<String>> entry : clusterTopicMap.entrySet()) {
      Instrumentation.instrument.returnVoidCatchThrowable(
          logger,
          infra.scope(),
          infra.tracer(),
          () -> {
            Set<String> allAvailableTopics =
                adminBuilder
                    .build(entry.getKey())
                    .listTopics()
                    .listings()
                    .get()
                    .stream()
                    .map(TopicListing::name)
                    .collect(Collectors.toSet());
            Sets.SetView<String> missingTopics =
                Sets.difference(new HashSet<>(entry.getValue()), allAvailableTopics);
            if (!missingTopics.isEmpty()) {
              logger.error(
                  "topics not found on cluster",
                  StructuredLogging.kafkaCluster(entry.getKey()),
                  StructuredLogging.kafkaTopics(new ArrayList<>(missingTopics)));
              infra
                  .scope()
                  .tagged(StructuredTags.builder().setKafkaCluster(entry.getKey()).build())
                  .gauge("kafka-partition-expansion-watcher.missing-topics")
                  .update(missingTopics.size());
              entry.getValue().retainAll(allAvailableTopics);
            }

            Map<String, AllPartitionInfo> topicPartitionMap =
                adminBuilder
                    .build(entry.getKey())
                    .describeTopics(entry.getValue())
                    .all()
                    .get(
                        KAFKA_ADMIN_CLIENT_DESCRIBE_TOPICS_TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS)
                    .entrySet()
                    .stream()
                    .collect(
                        Collectors.toMap(
                            k -> k.getKey(), v -> new AllPartitionInfo(v.getValue().partitions())));
            clusterTopicPartitionMap.put(entry.getKey(), topicPartitionMap);
          },
          "kafka-partition-expansion-watcher.describe-topics",
          StructuredFields.KAFKA_CLUSTER,
          entry.getKey());
    }
    return clusterTopicPartitionMap.build();
  }

  private void expandOrShrink(
      Versioned<StoredJobGroup> versionedJobGroup, List<TopicPartitionInfo> partitionInfos) {
    Instrumentation.instrument.returnVoidCatchThrowable(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          boolean isChanged = false;
          StoredJobGroup jobGroup = versionedJobGroup.model();

          // Verify that job_key is [0, expectedJobCount)
          // Removing any jobs that are not valid
          Map<Integer, StoredJob> currentJobs =
              jobGroup
                  .getJobsList()
                  .stream()
                  .collect(Collectors.toMap(JobUtils::getJobKey, v -> v));

          Map<Integer, StoredJob> newJobs = new HashMap<>();
          for (int i = 0; i < partitionInfos.size(); i++) {
            String jobPod = getJobPod(partitionInfos.get(i));
            if (currentJobs.containsKey(i)) {
              StoredJob oldJob = currentJobs.get(i);
              Preconditions.checkNotNull(
                  oldJob, "oldJob should not be null since we just checked map contains it");
              if (!jobPod.equals(oldJob.getJobPod())) {
                // update StoredJob if jobPod is changed, this will be persisted into job store
                StoredJob newJob = StoredJob.newBuilder(oldJob).setJobPod(jobPod).build();
                newJobs.put(i, newJob);
                isChanged = true;
              } else {
                newJobs.put(i, oldJob);
              }
              // remove old job from currentJobs map so that we can later check
              // currentJobs map for any jobs that remain. These jobs are removed so we should be
              // aware
              // of this.
              currentJobs.remove(i);
              continue;
            }
            // Else, no jobs exist for this partition so we must add it.
            // initialize the job state to match the job group state
            StoredJob newJob =
                jobCreator.newJob(
                    jobGroup,
                    jobIdProvider.getId(
                        StoredJob.newBuilder()
                            .setJob(JobUtils.newJob(jobGroup.getJobGroup()))
                            .setJobPod(jobPod)
                            .build()),
                    i);

            newJobs.put(i, newJob);
            isChanged = true;
          }

          // if any jobs remain in current jobs, these are extra partitions so we must remove it.
          if (currentJobs.size() > 0) {
            isChanged = true;
          }

          // skip ZK write if no change
          if (!isChanged) {
            return;
          }

          StoredJobGroup.Builder newJobGroupBuilder = StoredJobGroup.newBuilder(jobGroup);
          newJobGroupBuilder.clearJobs();
          newJobGroupBuilder.addAllJobs(newJobs.values());
          StoredJobGroup newJobGroup = newJobGroupBuilder.build();
          jobGroupStore.put(
              newJobGroup.getJobGroup().getJobGroupId(),
              VersionedProto.from(newJobGroup, versionedJobGroup.version()));
        },
        "kafka-partition-expansion-watcher.expand-or-shrink",
        StructuredFields.KAFKA_CLUSTER,
        versionedJobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getCluster(),
        StructuredFields.KAFKA_TOPIC,
        versionedJobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getTopic(),
        StructuredFields.KAFKA_GROUP,
        versionedJobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getConsumerGroup());
  }

  private void updatePartitionCounts(
      Collection<Versioned<StoredJobGroup>> jobGroups,
      Map<String, Map<String, AllPartitionInfo>> clusterTopicInfosMap) {
    for (Versioned<StoredJobGroup> jobGroup : jobGroups) {
      String topic = jobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getTopic();
      String group = jobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getConsumerGroup();
      String cluster = jobGroup.model().getJobGroup().getKafkaConsumerTaskGroup().getCluster();
      Map<String, AllPartitionInfo> topicInfoMap = clusterTopicInfosMap.get(cluster);
      if (topicInfoMap == null) {
        // if this cluster is not in the clusterTopicPartitionMap, Kafka Admin Client has failed
        // so we cannot make any assumptions on the expected partition counts
        // exceptions are logged in getTopicPartitionCounts so we don't need to re-log.
        continue;
      }
      Scope taggedScope =
          infra
              .scope()
              .tagged(
                  ImmutableMap.of(
                      StructuredFields.KAFKA_CLUSTER, cluster,
                      StructuredFields.KAFKA_TOPIC, topic,
                      StructuredFields.KAFKA_GROUP, group));
      int expectedPartitionCount = 0;
      List<TopicPartitionInfo> partitionInfos = new ArrayList<>();
      if (topicInfoMap.containsKey(topic)) {
        expectedPartitionCount = topicInfoMap.get(topic).partitionInfos.size();
        partitionInfos.addAll(topicInfoMap.get(topic).partitionInfos);
      }
      int actualPartitionCount = jobGroup.model().getJobsCount();
      taggedScope
          .gauge("kafka-partition-expansion-watcher.actual-partition-count")
          .update((double) actualPartitionCount);
      taggedScope
          .gauge("kafka-partition-expansion-watcher.expected-partition-count")
          .update((double) expectedPartitionCount);
      expandOrShrink(jobGroup, partitionInfos);
    }
  }

  private String getJobPod(TopicPartitionInfo topicPartitionInfo) {
    String brokerPod = "";
    if (topicPartitionInfo.leader() != null && topicPartitionInfo.leader().pod() != null) {
      brokerPod = topicPartitionInfo.leader().pod();
    }

    return brokerPod;
  }

  private static class AllPartitionInfo {
    private List<TopicPartitionInfo> partitionInfos;

    AllPartitionInfo(List<TopicPartitionInfo> partitionInfos) {
      this.partitionInfos = partitionInfos;
    }
  }
}
