package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.api.core.InternalApi;
import com.uber.data.kafka.datatransfer.ScaleComputerSnapshot;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/** The interface computes scale of work load by taking samples */
@InternalApi
@ThreadSafe
public interface ScaleComputer {

  /** no operation ScaleComputer */
  ScaleComputer NOOP =
      new ScaleComputer() {
        @Override
        public Optional<Double> onSample(double sample) {
          return Optional.empty();
        }

        @Override
        public ScaleComputerSnapshot snapshot() {
          return ScaleComputerSnapshot.getDefaultInstance();
        }
      };

  /**
   * Takes scale sample and return proposed scale if there is
   *
   * @param sample the scale sample
   * @return the optional proposal of scale
   */
  Optional<Double> onSample(double sample);

  /**
   * Takes a dump of internal state of the computer for data analysis
   *
   * @return the dump of internal state
   */
  ScaleComputerSnapshot snapshot();
}
