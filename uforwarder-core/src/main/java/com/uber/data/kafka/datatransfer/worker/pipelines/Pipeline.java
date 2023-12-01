package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import org.springframework.context.Lifecycle;

/**
 * Pipeline is a working unit in the data transfer worker.
 *
 * <p>The properties of a pipeline are:
 *
 * <ol>
 *   <li>A pipeline is assigned >= 0 jobs
 *   <li>A worker may run multiple pipelines
 *   <li>The pipeline manager runs multiple pipelines and is responsible for maintaining the
 *       lifecycle of the pipeline via {@code Lifecycle} interface
 *   <li>Each pipeline managed by the pipeline manager is assigned a disjoint subset of jobs.
 *   <li>The control plane may run/update/cancel/cancelAll jobs on a pipeline via the {@code
 *       Controllable} interface.
 * </ol>
 */
public interface Pipeline extends Controllable, Lifecycle, MetricSource {}
