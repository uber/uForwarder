package com.uber.data.kafka.consumerproxy.worker.filter;

import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorMessage;
import com.uber.data.kafka.datatransfer.Job;

/**
 * Filters out messages that should be ignored. Ignored message will skip dispatcher and ack
 * immediately.
 */
public interface Filter {
  Filter NOOP_FILTER = pm -> true;

  /**
   * Tests the message see if it should be processed.
   *
   * @param pm the processor message
   * @return the boolean indicates whether the message should be processed.
   */
  boolean shouldProcess(ProcessorMessage pm);

  /** Factory to create a filter. */
  interface Factory {

    /**
     * Creates filter.
     *
     * @param job the job
     * @return the filter
     */
    Filter create(Job job);
  }
}
