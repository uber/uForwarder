package com.uber.data.kafka.uforwarder;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class SampleConsumerProducer {
  private static final Logger logger = LoggerFactory.getLogger(SampleConsumerProducer.class);
  private final KafkaProducer kafkaProducer;
  private final String topic;
  private final AtomicInteger counter;

  public SampleConsumerProducer(
      @Value("${topic}") String topic, @Value("${kafkaConnect}") String bootstrapServers) {
    this.kafkaProducer = prepareProducer(bootstrapServers);
    this.topic = topic;
    this.counter = new AtomicInteger(0);
    Utils.createTopic(topic, 1, bootstrapServers);
  }

  public String topic() {
    return topic;
  }

  private KafkaProducer<Byte[], Byte[]> prepareProducer(String bootstrapServers) {
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test");
    producerProps.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    return new KafkaProducer<>(producerProps);
  }

  @Scheduled(fixedRate = 1000)
  public void produce() {
    ProducerRecord record =
        new ProducerRecord(
            topic, String.format("test message %d", counter.incrementAndGet()).getBytes());

    kafkaProducer.send(record);
    logger.info("Produced message counter = {}", counter.get());
  }
}
