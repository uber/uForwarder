package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.controller.rpc.Workload;

/** converts workload metrics to scale */
public interface ScaleConverter {
  double convert(Workload workload);
}
