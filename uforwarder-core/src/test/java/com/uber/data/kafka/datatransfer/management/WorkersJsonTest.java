package com.uber.data.kafka.datatransfer.management;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class WorkersJsonTest extends FievelTestBase {
  @Test
  public void testRead() throws Exception {
    Store<Long, StoredWorker> workerStore = Mockito.mock(Store.class);
    Mockito.doReturn(
            ImmutableMap.of(
                1L,
                Versioned.from(StoredWorker.getDefaultInstance(), 0),
                2L,
                Versioned.from(StoredWorker.getDefaultInstance(), 0)))
        .when(workerStore)
        .getAll();
    Store<String, StoredJobGroup> jobGroupStore = Mockito.mock(Store.class);
    Mockito.doReturn(
            ImmutableMap.of(
                "g1", Versioned.from(StoredJobGroup.getDefaultInstance(), 0),
                "g2", Versioned.from(StoredJobGroup.getDefaultInstance(), 0)))
        .when(jobGroupStore)
        .getAll();
    WorkerManagementConfiguration config = new WorkerManagementConfiguration();
    Assert.assertNotNull(new WorkersJson(workerStore, jobGroupStore, config).read());
  }
}
