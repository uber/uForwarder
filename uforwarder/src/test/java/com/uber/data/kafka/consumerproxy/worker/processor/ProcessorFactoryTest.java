package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.config.ProcessorConfiguration;
import com.uber.data.kafka.consumerproxy.worker.filter.Filter;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ProcessorFactoryTest extends FievelTestBase {
  private CoreInfra coreInfra;
  private ProcessorConfiguration config;
  private OutboundMessageLimiter.Builder outboundMessageLimiterBuilder;
  private OutboundMessageLimiter outboundMessageLimiter;
  private MessageAckStatusManager.Builder ackStatusManagerBuilder;
  private UnprocessedMessageManager.Builder unprocessedManagerBuilder;
  private ProcessorFactory processorFactory;
  private Filter.Factory filterFactory;

  @Before
  public void setUP() {
    coreInfra = CoreInfra.NOOP;
    config = new ProcessorConfiguration();
    outboundMessageLimiterBuilder = Mockito.mock(OutboundMessageLimiter.Builder.class);
    ackStatusManagerBuilder = Mockito.mock(MessageAckStatusManager.Builder.class);
    unprocessedManagerBuilder = Mockito.mock(UnprocessedMessageManager.Builder.class);
    outboundMessageLimiter = Mockito.mock(OutboundMessageLimiter.class);
    filterFactory = Mockito.mock(Filter.Factory.class);

    Mockito.when(outboundMessageLimiterBuilder.build(Mockito.any(Job.class)))
        .thenReturn(outboundMessageLimiter);
    processorFactory =
        new ProcessorFactory(
            coreInfra,
            config,
            outboundMessageLimiterBuilder,
            ackStatusManagerBuilder,
            unprocessedManagerBuilder,
            filterFactory);
  }

  @Test
  public void testCreate() {
    ProcessorImpl processor = processorFactory.create(Job.newBuilder().build(), "processor-id");
    Assert.assertNotNull(processor);
  }
}
