package com.uber.data.kafka.consumerproxy.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.consumerproxy.worker.PipelineImpl;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorImpl;
import com.uber.data.kafka.datatransfer.DebugMessageStubRow;
import com.uber.data.kafka.datatransfer.DebugMessageStubTable;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import java.util.Map;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "stubsJson")
public class WorkerStubsJson {
  private final JsonFormat.Printer jsonPrinter;
  private final PipelineManager pipelineManger;

  WorkerStubsJson(PipelineManager pipelineManager) {
    this.pipelineManger = pipelineManager;
    this.jsonPrinter =
        JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields();
  }

  @ReadOperation
  public String read() throws Exception {
    DebugMessageStubTable.Builder tableBuilder = DebugMessageStubTable.newBuilder();
    for (Map.Entry<String, Pipeline> pipelineEntry : pipelineManger.getPipelines().entrySet()) {
      String pipelineId = pipelineEntry.getKey();
      Pipeline pipeline = pipelineEntry.getValue();
      if (pipeline instanceof PipelineImpl) {
        ProcessorImpl processor = ((PipelineImpl) pipeline).processor();
        Map<Job, Map<Long, MessageStub>> jobStubs = processor.getStubs();
        for (Map.Entry<Job, Map<Long, MessageStub>> jobEntry : jobStubs.entrySet()) {
          Job job = jobEntry.getKey();
          for (Map.Entry<Long, MessageStub> stubEntry : jobEntry.getValue().entrySet()) {
            DebugMessageStubRow.Builder rowBuilder = DebugMessageStubRow.newBuilder();
            rowBuilder.setPipelineId(pipelineId);
            rowBuilder.setPartition(job.getKafkaConsumerTask().getPartition());
            rowBuilder.setOffset(stubEntry.getKey());
            rowBuilder.addAllEvents(stubEntry.getValue().getDebugInfo());
            tableBuilder.addData(rowBuilder.build());
          }
        }
      }
    }
    return jsonPrinter.print(tableBuilder.build());
  }
}
