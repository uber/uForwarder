package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.api.core.InternalApi;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/** The interface computes scale of work load by taking samples */
@InternalApi
@ThreadSafe
public interface ScaleComputer {

  /** no operation ScaleComputer */
  ScaleComputer NOOP = sample -> Optional.empty();

  /**
   * Takes scale sample and return proposed scale if there is
   *
   * @param sample the scale sample
   * @return the optional proposal of scale
   */
  Optional<Double> onSample(double sample);
}
