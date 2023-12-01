package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import java.util.Comparator;

/**
 * WorkerUtils for simplifying creating and updating Job proto entities.
 *
 * <p>The following naming conventions shall be used:
 *
 * <ol>
 *   <li>X getX() returns a field within an object.
 *   <li>X newX() creates anew object of type X. The input parameters of newX must not include the
 *       object X. To create a new object X from a parent prototype of the same type, use withX
 *       instead.
 *   <li>X withX() to create a new object with a single field set. This is useful for setting fields
 *       within an immutable object.
 *   <li>void setX() to set a field in a mutable object
 *   <li>boolean isX() for validation that returns boolean
 *   <li>void assertX() throws Exception for validations that throws exception
 * </ol>
 */
public final class WorkerUtils {
  /** UNSET_WORKER_ID is the worker_id to indicate that it is not assigned to any worker */
  public static final long UNSET_WORKER_ID = 0;

  public static final Comparator<StoredWorker> workerComparator =
      new Comparator<StoredWorker>() {
        @Override
        public int compare(StoredWorker o1, StoredWorker o2) {
          return Long.compare(o1.getNode().getId(), o2.getNode().getId());
        }
      };

  /**
   * Creates new worker for this node.
   *
   * @param node information for this worker.
   * @return worker with node set.
   */
  public static StoredWorker newWorker(Node node) {
    return StoredWorker.newBuilder().setNode(node).build();
  }

  /**
   * Gets the worker id for this worker.
   *
   * @param worker to query.
   * @return worker id for this worker.
   */
  public static long getWorkerId(StoredWorker worker) {
    return worker.getNode().getId();
  }

  /**
   * Sets the id for the worker.
   *
   * @param workerId to set.
   * @param worker to set workerId into.
   * @return a new Worker object with workerId set.
   * @implNote this method reverses the {@code withWorkerId(worker, workerId)} pattern b/c the
   *     creator method follows the pattern {@code BiFunction<K,V,V>}.
   */
  public static StoredWorker withWorkerId(Long workerId, StoredWorker worker) {
    StoredWorker.Builder builder = StoredWorker.newBuilder(worker);
    builder.getNodeBuilder().setId(workerId);
    return builder.build();
  }
}
