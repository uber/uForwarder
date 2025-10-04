package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.DebugJobRow;
import com.uber.data.kafka.datatransfer.DebugJobsTable;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import java.util.Objects;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "jobsJson")
public class MasterJobsJson extends AbstractJobsJson {
  private final Store<String, StoredJobGroup> jobGroupStore;

  MasterJobsJson(
      Store<String, StoredJobGroup> jobGroupStore,
      String hostName,
      String debugUrlFormat,
      JsonFormat.TypeRegistry typeRegistry) {
    super(hostName, debugUrlFormat, typeRegistry);
    this.jobGroupStore = jobGroupStore;
  }

  @ReadOperation
  public String read() throws Exception {
    DebugJobsTable.Builder tableBuilder = DebugJobsTable.newBuilder();
    for (Versioned<StoredJobGroup> versioned : jobGroupStore.getAll().values()) {
      StoredJobGroup jobGroup = versioned.model();
      String jobGroupId = jobGroup.getJobGroup().getJobGroupId();
      for (StoredJob job : jobGroup.getJobsList()) {
        DebugJobRow.Builder rowBuilder = DebugJobRow.newBuilder();
        rowBuilder.setJob(job);
        rowBuilder.setJobGroupId(jobGroupId);
        rowBuilder.setJobPartition(jobPartition(job.getJob().getRpcDispatcherTask().getUri()));
        tableBuilder.addData(rowBuilder.build());
      }
    }
    return jsonPrinter.print(tableBuilder.build());
  }

  private String jobPartition(String uri) {
    int result = Math.floorMod(Objects.hash(uri), 10000);
    return Integer.toString(result);
  }
}
