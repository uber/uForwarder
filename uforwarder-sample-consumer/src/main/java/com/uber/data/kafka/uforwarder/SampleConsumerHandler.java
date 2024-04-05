package com.uber.data.kafka.uforwarder;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.uber.data.kafka.consumerproxy.client.grpc.ConsumerMetadata;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SampleConsumerHandler implements ServerCalls.UnaryMethod<ByteString, Empty> {
  private static final Logger logger = LoggerFactory.getLogger(SampleConsumerHandler.class);

  @Override
  public void invoke(ByteString data, StreamObserver responseObserver) {
    String topic = ConsumerMetadata.getTopic();
    int partition = ConsumerMetadata.getPartition();
    long offset = ConsumerMetadata.getOffset();
    logger.info(
        "Received a message data={} topic={} partition={} offset={}",
        data,
        topic,
        partition,
        offset);
    ConsumerResponse.commit(responseObserver);
  }
}
