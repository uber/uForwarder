package com.uber.data.kafka.datatransfer.controller.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import java.util.List;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JobStoreTest {
  private Store<String, StoredJobGroup> jobGroupStore;
  private JobStore jobStore;

  @BeforeEach
  public void setup() throws Exception {
    jobGroupStore = Mockito.mock(Store.class);
    jobStore = new JobStore(jobGroupStore);

    Map<String, Versioned<StoredJobGroup>> map =
        ImmutableMap.of(
            "jobGroup1", VersionedProto.from(newJobGroup("jobGroup1", 5, 10)),
            "jobGroup2", VersionedProto.from(newJobGroup("jobGroup2", 10, 20)));
    Mockito.when(jobGroupStore.getAll()).thenReturn(map);
  }

  @Test
  public void getAll() throws Exception {
    Assertions.assertEquals(15, jobStore.getAll().size());
  }

  @Test
  public void getAllSelector() throws Exception {
    List<Long> jobIds = ImmutableList.of(5L, 6L, 7L);
    Assertions.assertEquals(3, jobStore.getAll(j -> jobIds.contains(j.getJob().getJobId())).size());
  }

  @Test
  public void getSuccess() throws Exception {
    Assertions.assertEquals(9L, jobStore.get(9L).model().getJob().getJobId());
  }

  @Test
  public void getFailure() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> jobStore.get(1L));
  }

  @Test
  public void getThroughSuccess() throws Exception {
    Assertions.assertEquals(11L, jobStore.get(11L).model().getJob().getJobId());
  }

  @Test
  public void getThroughFailure() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> jobStore.get(2L));
  }

  private static StoredJobGroup newJobGroup(String groupId, int jobIdStart, int jobIdEnd) {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    builder.getJobGroupBuilder().setJobGroupId(groupId);
    for (int i = jobIdStart; i < jobIdEnd; i++) {
      StoredJob.Builder jobBuilder = StoredJob.newBuilder();
      jobBuilder.getJobBuilder().setJobId(i);
      builder.addJobs(jobBuilder.build());
    }
    return builder.build();
  }
}
