package com.uber.data.kafka.datatransfer.controller.manager;

import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.storage.LocalSequencer;
import com.uber.data.kafka.datatransfer.controller.storage.LocalStore;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkerStoreAutoConfiguration {
  @Bean
  public Store<Long, StoredWorker> workerStore() {
    return new LocalStore<>(new LocalSequencer<>());
  }
}
