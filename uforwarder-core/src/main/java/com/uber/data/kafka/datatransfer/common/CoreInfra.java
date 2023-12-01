package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import java.util.Map;

/**
 * CoreInfra wraps widely used infrastructure objects such as metrics and tracing
 *
 * <p>Application can extend this class to support more infrastructure level functionalities.
 *
 * <p>It's introduced to reduce number of parameter passed to construct new functionality
 */
public class CoreInfra {

  /** The no operation infra instance */
  public static final CoreInfra NOOP = CoreInfra.builder().build();

  private final Scope scope;
  private final Tracer tracer;
  private final ContextManager contextManager;
  private final DynamicConfiguration dynamicConfiguration;
  private final Placement placement;

  /** Instantiates a new Core infra. */
  private CoreInfra(Builder builder) {
    this.scope = builder.scope;
    this.tracer = builder.tracer;
    this.contextManager = builder.contextManager;
    this.dynamicConfiguration = builder.dynamicConfiguration;
    this.placement = builder.placement;
  }

  /**
   * Gets client for metrics emitting
   *
   * @return the scope
   */
  public Scope scope() {
    return scope;
  }

  /**
   * Gets client for tracing
   *
   * @return the tracer
   */
  public Tracer tracer() {
    return tracer;
  }

  /**
   * Creates a new CoreInfra by extending metrics scope with sub scope
   *
   * @param name the name
   * @return the core infra
   */
  public CoreInfra subScope(String name) {
    return CoreInfra.builder()
        .withScope(scope.subScope(name))
        .withTracer(tracer)
        .withContextManager(contextManager)
        .withDynamicConfiguration(dynamicConfiguration)
        .withPlacement(placement)
        .build();
  }

  /**
   * Creates a new CoreInfra by extending metrics scope with tags
   *
   * @param tags the tags
   * @return the core infra
   */
  public CoreInfra tagged(Map<String, String> tags) {
    return CoreInfra.builder()
        .withScope(scope.tagged(tags))
        .withTracer(tracer)
        .withContextManager(contextManager)
        .withDynamicConfiguration(dynamicConfiguration)
        .withPlacement(placement)
        .build();
  }

  /**
   * Gets dynamic configuration client.
   *
   * @return the dynamic configuration client
   */
  public DynamicConfiguration getDynamicConfiguration() {
    return dynamicConfiguration;
  }

  /**
   * Gets the {@link ContextManager}
   *
   * @return the context manager
   */
  public ContextManager contextManager() {
    return contextManager;
  }

  /**
   * Gets the {@link Placement}, containing the placement(region, zone) information
   *
   * @return the placement object
   */
  public Placement getPlacement() {
    return placement;
  }

  /**
   * Builder builder.
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** The type Builder. */
  public static class Builder {
    private Scope scope = new NoopScope();
    private Tracer tracer = NoopTracerFactory.create();
    private ContextManager contextManager = ContextManager.NOOP;
    private DynamicConfiguration dynamicConfiguration = DynamicConfiguration.DEFAULT;
    private Placement placement = Placement.DEFAULT;

    /**
     * Sets scope.
     *
     * @param scope the scope
     * @return the builder
     */
    public Builder withScope(Scope scope) {
      this.scope = scope;
      return this;
    }

    /**
     * Sets tracer.
     *
     * @param tracer the tracer
     * @return the builder
     */
    public Builder withTracer(Tracer tracer) {
      this.tracer = tracer;
      return this;
    }

    /**
     * Sets ContextManager
     *
     * @param contextManager the context manager
     * @return the builder
     */
    public Builder withContextManager(ContextManager contextManager) {
      this.contextManager = contextManager;
      return this;
    }

    /**
     * Sets DynamicConfiguration
     *
     * @param dynamicConfiguration the dynamic configuration
     * @return the builder
     */
    public Builder withDynamicConfiguration(DynamicConfiguration dynamicConfiguration) {
      this.dynamicConfiguration = dynamicConfiguration;
      return this;
    }

    /**
     * Sets Placement
     *
     * @param placement the placement information object
     * @return the builder
     */
    public Builder withPlacement(Placement placement) {
      this.placement = placement;
      return this;
    }

    /**
     * Builds the CoreInfra
     *
     * @return the core infra
     */
    public CoreInfra build() {
      return new CoreInfra(this);
    }
  }
}
