package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.StoredJobGroup;

public class JobGroupIdProvider implements IdProvider<String, StoredJobGroup> {
  @Override
  public String getId(StoredJobGroup item) throws Exception {
    return getId(item.getJobGroup());
  }

  public static String getId(JobGroup jobGroup) {
    return jobGroup.getJobGroupId();
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean isRunning() {
    return true;
  }
}
