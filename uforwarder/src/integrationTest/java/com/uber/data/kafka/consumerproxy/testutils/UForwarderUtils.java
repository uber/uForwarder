package com.uber.data.kafka.consumerproxy.testutils;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.net.HostAndPort;
import com.uber.data.kafka.datatransfer.AddJobGroupRequest;
import com.uber.data.kafka.datatransfer.AddJobGroupResponse;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsRequest;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsResponse;
import com.uber.data.kafka.datatransfer.MasterAdminServiceGrpc;
import com.uber.data.kafka.datatransfer.common.ZKStringSerializer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;

public class UForwarderUtils {
  private static final String UFORWARDER_ZK_LEADER_NODE = "leader";
  private static final String UFORWARDER_ZK_ROOT_PATH = "/uforwarder";
  private static final int ZOOKEEPER_TIMEOUT = 3000;
  private static final int MAX_AWAIT_TIME_IN_SEC = 10;

  public static void prepareZookeeperForController(String address) {
    ZkClient zkClient =
        new ZkClient(address, ZOOKEEPER_TIMEOUT, ZOOKEEPER_TIMEOUT, new ZKStringSerializer());
    zkClient.deleteRecursive(UFORWARDER_ZK_ROOT_PATH);
    zkClient.createPersistent(
        String.format("%s/%s", UFORWARDER_ZK_ROOT_PATH, UFORWARDER_ZK_LEADER_NODE), true);
    List<String> children = zkClient.getChildren(UFORWARDER_ZK_ROOT_PATH);
    Assert.assertTrue(children.contains(UFORWARDER_ZK_LEADER_NODE));
  }

  public static void createJob(String address, AddJobGroupRequest addJobGroupRequest) {
    HostAndPort hostAndPort = HostAndPort.fromString(address);
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
            .usePlaintext()
            .build();
    MasterAdminServiceGrpc.MasterAdminServiceBlockingStub blockingStub =
        MetadataUtils.attachHeaders(
            MasterAdminServiceGrpc.newBlockingStub(channel), new Metadata());
    AddJobGroupResponse response = blockingStub.addJobGroup(addJobGroupRequest);
    if (!response.hasGroup()) {
      throw new IllegalStateException("Add JobGroup Failed");
    }
    Assert.assertTrue(response.hasGroup());
    Assert.assertNotNull(response.getGroup().getJobGroup());

    await()
        .atMost(MAX_AWAIT_TIME_IN_SEC, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Iterator<GetAllJobGroupsResponse> allJobGroups =
                  blockingStub.getAllJobGroups(GetAllJobGroupsRequest.newBuilder().build());
              Assert.assertTrue(allJobGroups.hasNext());
            });
  }
}
