package com.uber.data.kafka.datatransfer.controller.config;

/** Configuration for jobstore. */
public class JobGroupStoreConfiguration extends StoreConfiguration {
  @Override
  public String getZkDataPath() {
    // dst is the destination task in job definition
    // src is the source task in job definition.
    return "/jobgroups/{id}";
  }

  @Override
  public String getZkSequencerPath() {
    return "/sequencer/jobgroup";
  }
}
