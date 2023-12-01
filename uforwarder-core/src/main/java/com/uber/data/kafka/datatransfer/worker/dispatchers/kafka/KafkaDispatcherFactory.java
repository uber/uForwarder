package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import com.uber.data.kafka.datatransfer.common.CoreInfra;

public class KafkaDispatcherFactory<K, V> {

  private final KafkaDispatcherConfiguration configuration;

  public KafkaDispatcherFactory(KafkaDispatcherConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Creates a new KafkaDispatcher
   *
   * @param clientId client id for kafka producer
   * @param cluster the cluster kafka producer connect to
   * @param infra wrap of widely used infrastructure objects such as metrics and tracing
   * @param isSecure whether to create secure kafka producer or not
   * @param acksOneProducer whether to create kafka producer with acks = 1 insteald of acks = all
   * @return
   * @throws Exception
   */
  public KafkaDispatcher<K, V> create(
      String clientId, String cluster, CoreInfra infra, boolean isSecure, boolean acksOneProducer)
      throws Exception {
    return new KafkaDispatcher<K, V>(
        infra, configuration, clientId, cluster, isSecure, acksOneProducer);
  }
}
