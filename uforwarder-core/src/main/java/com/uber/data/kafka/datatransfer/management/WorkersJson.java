package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.DebugWorkerRow;
import com.uber.data.kafka.datatransfer.DebugWorkersTable;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "workersJson")
public class WorkersJson {
  private final Store<Long, StoredWorker> workerStore;
  private final Store<String, StoredJobGroup> jobGroupStore;
  private final JsonFormat.Printer jsonPrinter;
  private final NodeUrlResolver workerUrlResolver;

  WorkersJson(
      Store<Long, StoredWorker> workerStore,
      Store<String, StoredJobGroup> jobGroupStore,
      NodeUrlResolver workerUrlResolver) {
    this.workerUrlResolver = workerUrlResolver;
    this.workerStore = workerStore;
    this.jobGroupStore = jobGroupStore;
    this.jsonPrinter =
        JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields();
  }

  @ReadOperation
  public String read() throws Exception {
    Map<String, Versioned<StoredJobGroup>> jobGroups = jobGroupStore.getAll();

    // count jobs by worker_id
    Map<Long, Long> jobCountByWorkerId =
        jobGroups.values().stream()
            .flatMap(jg -> jg.model().getJobsList().stream())
            .map(j -> j.getWorkerId())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    Map<Long, Double> totalLoadByWorkerId =
        jobGroups.values().stream()
            .flatMap(jg -> jg.model().getJobsList().stream())
            .collect(
                Collectors.groupingBy(
                    j -> j.getWorkerId(), Collectors.summingDouble(j -> j.getScale())));

    Map<Long, String> workerId2RoutingDest = new HashMap<>();
    jobGroups
        .values()
        .forEach(
            jobGroup ->
                jobGroup
                    .model()
                    .getJobsList()
                    .forEach(
                        storedJob -> {
                          // as of 09/21/2020, one worker only serves on routing destination
                          if (!workerId2RoutingDest.containsKey(storedJob.getWorkerId())) {
                            workerId2RoutingDest.put(
                                storedJob.getWorkerId(),
                                jobGroup
                                    .model()
                                    .getJobGroup()
                                    .getRpcDispatcherTaskGroup()
                                    .getUri());
                          }
                        }));

    // build debug table
    DebugWorkersTable.Builder tableBuilder = DebugWorkersTable.newBuilder();
    for (Map.Entry<Long, Versioned<StoredWorker>> workerEntry : workerStore.getAll().entrySet()) {
      // build debug row
      String hostPort =
          String.format(
              "%s:%d",
              workerEntry.getValue().model().getNode().getHost(),
              workerEntry.getValue().model().getNode().getPort());
      DebugWorkerRow.Builder rowBuilder = DebugWorkerRow.newBuilder();
      rowBuilder.setWorker(workerEntry.getValue().model());
      rowBuilder.setExpectedJobCount(jobCountByWorkerId.getOrDefault(workerEntry.getKey(), 0L));
      rowBuilder.setUrl(workerUrlResolver.resolveLink(workerEntry.getValue().model().getNode()));
      rowBuilder.setRoutingDestination(workerId2RoutingDest.getOrDefault(workerEntry.getKey(), ""));
      rowBuilder.setTotalLoad(totalLoadByWorkerId.getOrDefault(workerEntry.getKey(), 0.0));
      tableBuilder.addData(rowBuilder.build());
    }

    return jsonPrinter.print(tableBuilder.build());
  }
}
