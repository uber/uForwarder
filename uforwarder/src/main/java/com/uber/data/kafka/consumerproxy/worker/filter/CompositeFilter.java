package com.uber.data.kafka.consumerproxy.worker.filter;

import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorMessage;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;

/**
 * CompositeFilter filters messages by applying multiple filters in sequence. Message will be
 * processed if all filters agrees to process the message.
 */
public class CompositeFilter implements Filter {
  private final Filter[] filters;

  /**
   * Instantiates a new instance of CompositeFilter.
   *
   * @param filters the filters
   */
  private CompositeFilter(Filter... filters) {
    this.filters = filters;
  }

  @Override
  public boolean shouldProcess(ItemAndJob<ProcessorMessage> itemAndJob) {
    for (Filter filter : filters) {
      if (!filter.shouldProcess(itemAndJob)) {
        return false;
      }
    }
    return true;
  }

  /**
   * New factory filter . factory.
   *
   * @param factories the factories
   * @return the filter . factory
   */
  public static Filter.Factory newFactory(Filter.Factory... factories) {
    return new Factory(factories);
  }

  private static class Factory implements Filter.Factory {
    private final Filter.Factory[] factories;

    /**
     * Instantiates a new Factory.
     *
     * @param factories the factories
     */
    private Factory(Filter.Factory... factories) {
      this.factories = factories;
    }

    @Override
    public Filter create(Job job) {
      if (factories.length == 0) {
        return NOOP_FILTER;
      }

      Filter[] filters = new Filter[factories.length];
      for (int i = 0; i < factories.length; i++) {
        filters[i] = factories[i].create(job);
      }
      return new CompositeFilter(filters);
    }
  }
}
