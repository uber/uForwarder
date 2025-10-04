package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import java.util.concurrent.ThreadFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GrpcDispatcherFactoryTest {
  private AutoCloseable mocks;
  private GrpcDispatcherFactory grpcDispatcherFactory;

  @Mock private GrpcDispatcherConfiguration config;

  @Mock private CoreInfra coreInfra;

  @Mock private ThreadFactory threadFactory;

  @BeforeEach
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
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
    assertNotNull(dispatcher, "GrpcDispatcher should not be null");
  }

  @Test
  public void testGetConfig() {
    // When
    GrpcDispatcherConfiguration result = grpcDispatcherFactory.getConfig();

    // Then
    assertNotNull(result, "Config should not be null");
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }
}
