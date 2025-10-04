package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/** The type Grpc dispatcher factory. */
public class GrpcDispatcherFactory {
  private final GrpcDispatcherConfiguration config;
  private final CoreInfra coreInfra;

  public GrpcDispatcherFactory(GrpcDispatcherConfiguration config, CoreInfra coreInfra) {
    this.config = config;
    this.coreInfra = coreInfra;
  }

  /**
   * Creates grpc dispatcher.
   *
   * @param caller the caller
   * @param dispatcherId the dispatcherId
   * @param uri the uri
   * @param procedure the procedure
   * @return the grpc dispatcher
   * @throws Exception the exception
   */
  public GrpcDispatcher create(
      String caller, String dispatcherId, ThreadFactory threadFactory, String uri, String procedure)
      throws Exception {
    return new GrpcDispatcher(
        coreInfra,
        config.getThreadPoolSize() > 0
            ? Optional.of(
                coreInfra
                    .contextManager()
                    .wrap(
                        Executors.newScheduledThreadPool(
                            config.getThreadPoolSize(),
                            new ThreadFactoryBuilder()
                                .setNameFormat(dispatcherId + "-%d")
                                .setThreadFactory(threadFactory)
                                .build())))
            : Optional.empty(),
        config,
        caller,
        uri,
        procedure,
        GrpcFilter.NOOP);
  }

  /**
   * Gets config.
   *
   * @return the config
   */
  public GrpcDispatcherConfiguration getConfig() {
    return config;
  }

  /**
   * Gets app infra.
   *
   * @return the app infra
   */
  public CoreInfra getInfra() {
    return coreInfra;
  }
}
