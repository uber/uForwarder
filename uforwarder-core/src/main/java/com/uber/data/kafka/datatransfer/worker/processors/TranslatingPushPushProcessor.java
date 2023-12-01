package com.uber.data.kafka.datatransfer.worker.processors;

import com.uber.data.kafka.datatransfer.worker.common.Chainable;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TranslatingPushPushProcessor is a simple processor that converts IN type to OUT type.
 *
 * <p>We assume that translation algorithm is fast and CPU bound.
 */
public final class TranslatingPushPushProcessor<IN, OUT>
    implements Chainable<OUT, Void>, Sink<IN, Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TranslatingPushPushProcessor.class);
  private volatile Sink<OUT, Void> sink;
  private final Scope scope;
  private final Function<IN, OUT> translator;

  public TranslatingPushPushProcessor(Scope scope, Function<IN, OUT> translator) {
    this.sink = new Sink<OUT, Void>() {};
    this.scope = scope;
    this.translator = translator;
  }

  @Override
  public CompletionStage<Void> submit(ItemAndJob<IN> item) {
    Stopwatch processorLatencyStopwatch = scope.timer("processor.latency").start();
    try {
      return sink.submit(ItemAndJob.of(translator.apply(item.getItem()), item.getJob()));
    } finally {
      processorLatencyStopwatch.stop();
    }
  }

  @Override
  public void setNextStage(Sink<OUT, Void> sink) {
    this.sink = sink;
  }
}
