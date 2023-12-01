package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.DebugJobStatusRow;
import com.uber.data.kafka.datatransfer.DebugJobStatusTable;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import java.util.Collection;
import java.util.Map;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "jobStatusJson")
public class JobStatusJson {
  private final PipelineManager pipelineManager;
  private final JsonFormat.Printer jsonPrinter;

  public JobStatusJson(PipelineManager pipelineManager) {
    this.pipelineManager = pipelineManager;
    this.jsonPrinter =
        JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields();
  }

  @ReadOperation
  public String read() throws Exception {
    DebugJobStatusTable.Builder tableBuilder = DebugJobStatusTable.newBuilder();
    Map<String, Pipeline> pipelines = pipelineManager.getPipelines();
    for (Map.Entry<String, Pipeline> entry : pipelines.entrySet()) {
      String pipelineId = entry.getKey();
      Collection<JobStatus> jobStatusList = entry.getValue().getJobStatus();
      for (JobStatus jobStatus : jobStatusList) {
        DebugJobStatusRow.Builder rowBuilder = DebugJobStatusRow.newBuilder();
        rowBuilder.setPipelineId(pipelineId);
        rowBuilder.setJobStatus(jobStatus);
        tableBuilder.addData(rowBuilder.build());
      }
      if (jobStatusList.size() == 0) {
        // add noop row to show that the pipeline exists with no jobs.
        DebugJobStatusRow.Builder rowBuilder = DebugJobStatusRow.newBuilder();
        rowBuilder.setPipelineId(pipelineId);
        tableBuilder.addData(rowBuilder.build());
      }
    }
    return jsonPrinter.print(tableBuilder.build());
  }
}
