package com.uber.data.kafka.datatransfer.controller.config;

/** Configuration for jobstore. */
public class JobStoreConfiguration extends StoreConfiguration {

  @Override
  public String getZkSequencerPath() {
    return "/sequencer/job";
  }
}
