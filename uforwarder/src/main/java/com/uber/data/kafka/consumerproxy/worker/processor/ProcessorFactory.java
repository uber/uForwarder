package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.uber.data.kafka.consumerproxy.config.ProcessorConfiguration;
import com.uber.data.kafka.consumerproxy.worker.filter.Filter;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import java.util.concurrent.Executors;

public class ProcessorFactory {
  private final ProcessorConfiguration config;
  private final OutboundMessageLimiter.Builder outboundMessageLimiterBuilder;
  private final CoreInfra coreInfra;
  private final MessageAckStatusManager.Builder ackStatusManagerBuilder;
  private final UnprocessedMessageManager.Builder unprocessedManagerBuilder;
  private final Filter.Factory filterFactory;

  public ProcessorFactory(
      CoreInfra coreInfra,
      ProcessorConfiguration config,
      OutboundMessageLimiter.Builder outboundMessageLimiterBuilder,
      MessageAckStatusManager.Builder ackStatusManagerBuilder,
      UnprocessedMessageManager.Builder unprocessedManagerBuilder,
      Filter.Factory filterFactory) {
    this.coreInfra = coreInfra;
    this.config = config;
    this.outboundMessageLimiterBuilder = outboundMessageLimiterBuilder;
    this.ackStatusManagerBuilder = ackStatusManagerBuilder;
    this.unprocessedManagerBuilder = unprocessedManagerBuilder;
    this.filterFactory = filterFactory;
  }

  public ProcessorImpl create(Job job, String processorId) {
    return new ProcessorImpl(
        job,
        coreInfra
            .contextManager()
            .wrap(
                Executors.newScheduledThreadPool(
                    config.getThreadPoolSize(),
                    new ThreadFactoryBuilder().setNameFormat(processorId + "-%d").build())),
        outboundMessageLimiterBuilder,
        ackStatusManagerBuilder,
        unprocessedManagerBuilder,
        filterFactory.create(job),
        config.getMaxOutboundCacheCount(),
        coreInfra);
  }
}
