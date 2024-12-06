package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.protobuf.util.JsonFormat;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("uforwarder-controller")
public class TypeRegistryAutoConfiguration {

  /**
   * used to enable serialization/deserialization of {@link
   * com.uber.data.kafka.datatransfer.JobGroup} extension call
   * TypeRegistry.add(message.getDescriptor()) to add supported message type
   *
   * @return type registry
   */
  @Bean
  public JsonFormat.TypeRegistry typeRegistry() {
    return JsonFormat.TypeRegistry.getEmptyTypeRegistry();
  }
}
