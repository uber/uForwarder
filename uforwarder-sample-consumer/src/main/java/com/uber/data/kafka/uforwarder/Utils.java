package com.uber.data.kafka.uforwarder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.uber.data.kafka.datatransfer.AddJobGroupRequest;
import com.uber.data.kafka.datatransfer.AddJobGroupResponse;
import com.uber.data.kafka.datatransfer.MasterAdminServiceGrpc;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Utils {
  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static void createTopic(String topicName, int numOfPartitions, String bootstrapServers) {
    AdminClient adminClient =
        KafkaAdminClient.create(
            ImmutableMap.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers,
                AdminClientConfig.CLIENT_ID_CONFIG,
                "kcp-unittest"));
    NewTopic newTopic = new NewTopic(topicName, numOfPartitions, (short) 1);
    try {
      adminClient.createTopics(ImmutableList.of(newTopic));
      logger.info("Topic {} created", topicName);
    } catch (TopicExistsException e) {
      logger.warn("Topic {} already existed", topicName);
    }
  }

  public static StoredJobGroup createConsumerJob(
      String uforwarderConnect, AddJobGroupRequest addJobGroupRequest) {
    HostAndPort hostAndPort = HostAndPort.fromString(uforwarderConnect);
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
            .usePlaintext()
            .build();
    MasterAdminServiceGrpc.MasterAdminServiceBlockingStub blockingStub =
        MetadataUtils.attachHeaders(
            MasterAdminServiceGrpc.newBlockingStub(channel), new Metadata());
    AddJobGroupResponse response = blockingStub.addJobGroup(addJobGroupRequest);
    return response.getGroup();
  }
}
