package com.uber.data.kafka.consumerproxy.testutils;

public class Constants {
  public static final String MASTER_HOST = "127.0.0.1";
  public static final String KCP_RUNTIME_ENVIRONMENT = "test";

  public static final Integer KAFKA_PORT = 9092;
  public static final Integer ZOOKEEPER_PORT = 2181;
  public static final Integer ZOOKEEPER_TIMEOUT = 3000;
  public static final String UFORWARDER_ZK_ROOT = "/uforwarder";
  public static final String UFORWARDER_ZK_LEADER_NODE = "leader";
  public static final String UFORWARDER_ZK_WORKERS_NODE = "workers";

  public static final int MAX_AWAIT_TIME_IN_SEC = 30;

  public static final String JOB_GROUP_ID_DELIMITER = "@";
}
