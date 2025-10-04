package com.uber.data.kafka.datatransfer.controller.autoscalar;

/** mode to convert workload metrics to scales */
public enum ScaleConverterMode {
  // Throughput to scale
  THROUGHPUT,
  // Cpu usage to scale
  CPU
}
