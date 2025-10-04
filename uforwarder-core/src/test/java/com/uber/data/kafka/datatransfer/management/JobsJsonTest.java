package com.uber.data.kafka.datatransfer.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class JobsJsonTest extends FievelTestBase {
  @Test
  public void testMasterJobJsonRead() throws Exception {
    Store<String, StoredJobGroup> jobGroupStore = Mockito.mock(Store.class);
    Mockito.doReturn(
            ImmutableMap.of(
                "g1",
                Versioned.from(
                    StoredJobGroup.newBuilder().addJobs(StoredJob.getDefaultInstance()).build(),
                    0)))
        .when(jobGroupStore)
        .getAll();
    MasterJobsJson jobsJson =
        new MasterJobsJson(
            jobGroupStore,
            "hostname",
            "https://%s:5328/",
            JsonFormat.TypeRegistry.getEmptyTypeRegistry());
    Assert.assertNotNull(jobsJson.read());
  }

  @Test
  public void testWorkerJobJsonRead() throws Exception {
    PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    Pipeline pipeline = Mockito.mock(Pipeline.class);
    Mockito.doReturn(ImmutableMap.of("p1", pipeline)).when(pipelineManager).getPipelines();
    Mockito.doReturn(ImmutableList.of(Job.getDefaultInstance())).when(pipeline).getJobs();
    WorkerJobsJson jobsJson =
        new WorkerJobsJson(
            pipelineManager,
            "hostname",
            "https://%s:5328/",
            JsonFormat.TypeRegistry.getEmptyTypeRegistry());
    Assert.assertNotNull(jobsJson.read());
  }
}
