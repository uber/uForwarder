package com.uber.data.kafka.datatransfer.management;

import com.uber.data.kafka.datatransfer.DebugJobRow;
import com.uber.data.kafka.datatransfer.DebugJobsTable;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import java.util.Map;
import java.util.Objects;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "jobsJson")
public class WorkerJobsJson extends AbstractJobsJson {
  private final PipelineManager pipelineManger;

  WorkerJobsJson(PipelineManager pipelineManager, String hostName, String debugUrlFormat) {
    super(hostName, debugUrlFormat);
    this.pipelineManger = pipelineManager;
  }

  @ReadOperation
  public String read() throws Exception {
    DebugJobsTable.Builder tableBuilder = DebugJobsTable.newBuilder();
    for (Map.Entry<String, Pipeline> pipelineEntry : pipelineManger.getPipelines().entrySet()) {
      String pipelineId = pipelineEntry.getKey();
      for (Job job : pipelineEntry.getValue().getJobs()) {
        DebugJobRow.Builder rowBuilder = DebugJobRow.newBuilder();
        rowBuilder.setPipelineId(pipelineId);
        rowBuilder.setRpcDebugUrl(getRpcDebugUrl());
        rowBuilder.setJob(StoredJob.newBuilder().setJob(job).build());
        rowBuilder.setJobPartition(jobPartition(job.getRpcDispatcherTask().getUri()));
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
