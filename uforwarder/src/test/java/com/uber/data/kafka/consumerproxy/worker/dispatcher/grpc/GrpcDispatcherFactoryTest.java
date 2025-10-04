package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.concurrent.ThreadFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcDispatcherFactoryTest extends FievelTestBase {
  private GrpcDispatcherFactory grpcDispatcherFactory;

  @Mock private GrpcDispatcherConfiguration config;

  @Mock private CoreInfra coreInfra;

  @Mock private ThreadFactory threadFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(coreInfra.contextManager()).thenReturn(ContextManager.NOOP);
    when(config.getGrpcChannelPoolSize()).thenReturn(1);
    when(config.getThreadPoolSize()).thenReturn(1);
    grpcDispatcherFactory = new GrpcDispatcherFactory(config, coreInfra);
  }

  @Test
  public void testCreateGrpcDispatcher() throws Exception {
    // Given
    String caller = "test-caller";
    String dispatcherId = "test-dispatcher";
    String uri = "test-uri";
    String procedure = "test-procedure";

    // When
    GrpcDispatcher dispatcher =
        grpcDispatcherFactory.create(caller, dispatcherId, threadFactory, uri, procedure);

    // Then
    assertNotNull("GrpcDispatcher should not be null", dispatcher);
  }

  @Test
  public void testGetConfig() {
    // When
    GrpcDispatcherConfiguration result = grpcDispatcherFactory.getConfig();

    // Then
    assertNotNull("Config should not be null", result);
  }
}
