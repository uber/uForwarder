package com.uber.data.kafka.consumerproxy.worker.filter;

import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorMessage;
import com.uber.data.kafka.datatransfer.Job;

/** Filters messages that ensures messages are from the same cluster as the consumer */
public class OriginalClusterFilter implements Filter {
  private final Job job;

  /**
   * Instantiates a new Original cluster filter.
   *
   * @param job the job
   */
  public OriginalClusterFilter(Job job) {
    this.job = job;
  }

  /**
   * if clusterFilterEnabled, filter out (do not send) kafka messages where producer cluster is non
   * empty and does not equal the consuming cluster.
   *
   * @param pm the processor message
   * @return indicates if the message should be processed
   */
  @Override
  public boolean shouldProcess(ProcessorMessage pm) {
    String producerCluster = pm.getProducerCluster();

    return producerCluster.isEmpty()
        || producerCluster.equalsIgnoreCase(job.getKafkaConsumerTask().getCluster());
  }

  /**
   * Creates a new factory.
   *
   * @return the factory
   */
  public static Factory newFactory() {
    return new Factory();
  }

  private static class Factory implements Filter.Factory {
    @Override
    public Filter create(Job job) {
      return new OriginalClusterFilter(job);
    }
  }
}
