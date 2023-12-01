package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.worker.common.Chainable;
import com.uber.data.kafka.datatransfer.worker.common.Configurable;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.Lifecycle;

/**
 * KafkaFetcher wraps the AbstractKafkaFetcherThread and implements the Fetcher interface.
 *
 * @implNote we need this additional level of indirection b/c Fetcher requires us to implement
 *     {@code stop} method, but {@code FetcherThread} extends {@code ShutdownableThread}, which
 *     contains a final stop method we cannot override via inheritence so we opt to extend
 *     functionality via composition.
 */
public class KafkaFetcher<K, V>
    implements Lifecycle, Configurable, Chainable<ConsumerRecord<K, V>, Long> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFetcher.class);
  private final AbstractKafkaFetcherThread<K, V> fetcherThread;

  public KafkaFetcher(AbstractKafkaFetcherThread<K, V> fetcherThread) {
    this.fetcherThread = fetcherThread;
  }

  @Override
  public void start() {
    LOGGER.info("starting message fetcher");
    fetcherThread.start();
    LOGGER.info("started message fetcher");
  }

  @Override
  public void stop() {
    LOGGER.info("stopping message fetcher");
    fetcherThread.close();
    LOGGER.info("stopped message fetcher");
  }

  @Override
  public boolean isRunning() {
    return fetcherThread.isRunning();
  }

  @Override
  public void setNextStage(Sink<ConsumerRecord<K, V>, Long> sink) {
    fetcherThread.setNextStage(sink);
  }

  @Override
  public void setPipelineStateManager(PipelineStateManager pipelineStateManager) {
    fetcherThread.setPipelineStateManager(pipelineStateManager);
  }

  /** Signals to the fetcher thread that there is updates to poll from PipelineStateManager. */
  public CompletionStage<Void> signal() {
    return fetcherThread.signal();
  }
}
