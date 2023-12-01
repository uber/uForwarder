package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.List;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JobStoreTest extends FievelTestBase {
  private Store<String, StoredJobGroup> jobGroupStore;
  private JobStore jobStore;

  @Before
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
    Assert.assertEquals(15, jobStore.getAll().size());
  }

  @Test
  public void getAllSelector() throws Exception {
    List<Long> jobIds = ImmutableList.of(5L, 6L, 7L);
    Assert.assertEquals(3, jobStore.getAll(j -> jobIds.contains(j.getJob().getJobId())).size());
  }

  @Test
  public void getSuccess() throws Exception {
    Assert.assertEquals(9L, jobStore.get(9L).model().getJob().getJobId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getFailure() throws Exception {
    jobStore.get(1L);
  }

  @Test
  public void getThroughSuccess() throws Exception {
    Assert.assertEquals(11L, jobStore.get(11L).model().getJob().getJobId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getThroughFailure() throws Exception {
    jobStore.get(2L);
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
