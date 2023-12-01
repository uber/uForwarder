package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.DebugMasterRow;
import com.uber.data.kafka.datatransfer.DebugMasterTable;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "mastersJson")
public class MastersJson {
  private static final String LEADER_ROLE = "leader";
  private static final String FOLLOWER_ROLE = "follower";
  private final LeaderSelector leaderSelector;
  private final JsonFormat.Printer jsonPrinter;

  MastersJson(LeaderSelector leaderSelector) {
    this.leaderSelector = leaderSelector;
    this.jsonPrinter =
        JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields();
  }

  @ReadOperation
  public String read() throws Exception {
    DebugMasterTable.Builder tableBuilder = DebugMasterTable.newBuilder();
    tableBuilder.addData(
        DebugMasterRow.newBuilder()
            .setHostPort(leaderSelector.getLeaderId())
            .setRole(LEADER_ROLE)
            .build());

    for (String hostPort : leaderSelector.getFollowers()) {
      tableBuilder.addData(
          DebugMasterRow.newBuilder().setHostPort(hostPort).setRole(FOLLOWER_ROLE).build());
    }

    return jsonPrinter.print(tableBuilder.build());
  }
}
