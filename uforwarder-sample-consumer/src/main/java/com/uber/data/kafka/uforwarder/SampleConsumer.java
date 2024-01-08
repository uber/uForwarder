package com.uber.data.kafka.uforwarder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SampleConsumer {

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(SampleConsumer.class);
    app.run(SampleConsumer.class);
  }
}
