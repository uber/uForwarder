package com.uber.data.kafka.datatransfer.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.creator.JobCreator;
import com.uber.data.kafka.datatransfer.controller.storage.IdProvider;
import com.uber.data.kafka.datatransfer.controller.storage.LocalSequencer;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import java.util.Collection;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KafkaPartitionExpansionWatcherTest extends FievelTestBase {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_GROUP = "test-group";
  private static final String TEST_TOPIC = "test-topic";
  private CoreInfra infra;
  private Store<String, StoredJobGroup> jobGroupStore;
  private AdminClient adminClient;
  private AdminClient.Builder adminBuilder;
  private KafkaPartitionExpansionWatcher kafkaPartitionExpansionWatcher;
  private IdProvider<Long, StoredJob> jobIdProvider;
  private JobCreator jobCreator;
  private LeaderSelector leaderSelector;

  @Before
  public void setup() throws Exception {
    Scope scope = Mockito.mock(Scope.class);
    Tracer tracer = new MockTracer();
    infra = CoreInfra.builder().withScope(scope).withTracer(tracer).build();
    jobGroupStore = mock(Store.class);
    adminClient = mock(AdminClient.class);
    adminBuilder = mock(AdminClient.Builder.class);
    jobIdProvider = new LocalSequencer<>();
    jobCreator = new JobCreator() {};
    leaderSelector = Mockito.mock(LeaderSelector.class);
    when(adminBuilder.build(Mockito.anyString())).thenReturn(adminClient);
    when(leaderSelector.isLeader()).thenReturn(true);
    Timer timer = Mockito.mock(Timer.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Histogram histogram = Mockito.mock(Histogram.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(scope.histogram(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
        .thenReturn(histogram);
    Mockito.when(timer.start()).thenReturn(stopwatch);
    kafkaPartitionExpansionWatcher =
        new KafkaPartitionExpansionWatcher(
            infra, jobGroupStore, jobIdProvider, jobCreator, adminBuilder, leaderSelector);

    // mock describeTopics call returning a topic with 2 partitions
    TopicDescription topicDescription =
        new TopicDescription(
            TEST_TOPIC,
            false,
            ImmutableList.of(
                new TopicPartitionInfo(0, null, ImmutableList.of(), ImmutableList.of()),
                new TopicPartitionInfo(1, null, ImmutableList.of(), ImmutableList.of())));
    KafkaFuture<Map<String, TopicDescription>> topicDescriptionFuture = mock(KafkaFuture.class);
    when(topicDescriptionFuture.get(Mockito.anyLong(), Mockito.any()))
        .thenReturn(ImmutableMap.of(TEST_TOPIC, topicDescription));
    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    when(describeTopicsResult.all()).thenReturn(topicDescriptionFuture);
    when(adminClient.describeTopics(ImmutableList.of(TEST_TOPIC))).thenReturn(describeTopicsResult);

    // mock listTopics call returns topics in the cluster
    KafkaFuture<Collection<TopicListing>> topicListingFuture = mock(KafkaFuture.class);
    when(topicListingFuture.get()).thenReturn(ImmutableSet.of(new TopicListing(TEST_TOPIC, false)));
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    when(listTopicsResult.listings()).thenReturn(topicListingFuture);
    when(adminClient.listTopics()).thenReturn(listTopicsResult);
  }

  private Versioned<StoredJobGroup> createJobGroup(int numOfJobs) {
    return createJobGroup(numOfJobs, TEST_TOPIC, TEST_CLUSTER);
  }

  private Versioned<StoredJobGroup> createJobGroup(int numOfJobs, String topic, String cluster) {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    builder
        .getJobGroupBuilder()
        .getKafkaConsumerTaskGroupBuilder()
        .setTopic(topic)
        .setCluster(cluster)
        .setConsumerGroup(TEST_GROUP);
    for (int i = 0; i < numOfJobs; i++) {
      builder.addJobs(
          StoredJob.newBuilder()
              .setJob(
                  Job.newBuilder()
                      .setJobId(i)
                      .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER)
                      .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setPartition(i).build())
                      .build())
              .build());
    }
    return VersionedProto.from(builder.build());
  }

  @Test
  public void testPartitionExpansion() throws Exception {
    // There is a job with 0 partitions.
    when(jobGroupStore.getAll())
        .thenReturn(
            ImmutableMap.of(
                "group1", createJobGroup(0),
                "group2", createJobGroup(0, "", TEST_CLUSTER),
                "group3", createJobGroup(0, TEST_TOPIC, "")));

    ArgumentCaptor<String> idArgumentCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);

    // Run
    kafkaPartitionExpansionWatcher.watchPartitionExpansion();

    // Verify that numJobs expands to 2
    verify(jobGroupStore).put(idArgumentCaptor.capture(), itemCaptor.capture());
    Assert.assertEquals(2, itemCaptor.getValue().model().getJobsCount());
  }

  @Test
  public void testPartitionExpansionAdminClientFactoryException() throws Exception {
    // partition expansion watcher should gracefully handle factory exceptions.
    when(adminBuilder.build(Mockito.anyString())).thenThrow(new RuntimeException());

    kafkaPartitionExpansionWatcher.watchPartitionExpansion();

    // no change (since admin client factory threw exception) so don't put to zk
    verify(jobGroupStore, times(0)).put(Mockito.any(), Mockito.any());
  }

  @Test
  public void testPartitionShrink() throws Exception {
    // There is a job with 4 partitions.
    when(jobGroupStore.getAll()).thenReturn(ImmutableMap.of("group1", createJobGroup(4)));

    ArgumentCaptor<String> idArgumentCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);

    kafkaPartitionExpansionWatcher.watchPartitionExpansion();

    // Verify that numJobs shrinks  4 to 2.
    verify(jobGroupStore).put(idArgumentCaptor.capture(), itemCaptor.capture());
    Assert.assertEquals(2, itemCaptor.getValue().model().getJobsCount());
  }

  @Test
  public void testPartitionShrinkAdminClientFactoryException() throws Exception {
    when(jobGroupStore.getAll()).thenReturn(ImmutableMap.of("group1", createJobGroup(4)));

    // partition expansion watcher should gracefully handle factory exceptions.
    when(adminBuilder.build(Mockito.anyString())).thenThrow(new RuntimeException());

    kafkaPartitionExpansionWatcher.watchPartitionExpansion();

    // no change (since admin client factory threw exception) so don't put to zk
    verify(jobGroupStore, times(0)).put(Mockito.any(), Mockito.any());
  }

  @Test
  public void testPartitionNoChange() throws Exception {
    // There is a job with 2 partitions.
    when(jobGroupStore.getAll()).thenReturn(ImmutableMap.of("group1", createJobGroup(2)));

    ArgumentCaptor<String> idArgumentCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);

    kafkaPartitionExpansionWatcher.watchPartitionExpansion();

    // Verify that numJobs shrinks  4 to 2.
    verify(jobGroupStore, times(0)).put(Mockito.any(), Mockito.any());
  }

  @Test
  public void testExceptionHandlers() throws Exception {
    Mockito.doThrow(new Exception()).when(jobGroupStore).put(Mockito.anyString(), Mockito.any());
    // There is a job with 0 partitions.
    when(jobGroupStore.getAll()).thenReturn(ImmutableMap.of("group1", createJobGroup(0)));
    kafkaPartitionExpansionWatcher.watchPartitionExpansion();

    Mockito.when(jobGroupStore.getAll()).thenThrow(new Exception());
    kafkaPartitionExpansionWatcher.watchPartitionExpansion();
  }

  @Test
  public void testIsNotLeader() {
    when(leaderSelector.isLeader()).thenReturn(false);
    kafkaPartitionExpansionWatcher.watchPartitionExpansion();
    Mockito.verify(infra.scope(), Mockito.times(1)).counter(ArgumentMatchers.anyString());
    Mockito.verify(infra.scope(), Mockito.times(1))
        .histogram(ArgumentMatchers.anyString(), Mockito.any());
    Mockito.verify(infra.scope(), Mockito.never()).gauge(ArgumentMatchers.anyString());
  }
}
