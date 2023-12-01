package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import java.util.Map;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;

/**
 * JobStore is an materialized view of StoredJob on top of StoredJobGroup.
 *
 * @implNote the implementation isn't particularly performant due to having to iterate through the
 *     JobGroupStore for lookup. It exists for easy of use for low performance use cases and for
 *     backwards compatibility during migration to the StoredJobGroup batched interface.
 */
public final class JobStore implements ReadStore<Long, StoredJob> {
  private final Store<String, StoredJobGroup> jobGroupStore;

  public JobStore(Store<String, StoredJobGroup> jobGroupStore) {
    this.jobGroupStore = jobGroupStore;
  }

  private static Map<Long, Versioned<StoredJob>> storedJobGroupMapToStoredJobMap(
      Map<String, Versioned<StoredJobGroup>> jobGroupMap, Function<StoredJob, Boolean> filter) {
    ImmutableMap.Builder<Long, Versioned<StoredJob>> builder = ImmutableMap.builder();
    for (Map.Entry<String, Versioned<StoredJobGroup>> jobGroupEntry : jobGroupMap.entrySet()) {
      for (StoredJob job : jobGroupEntry.getValue().model().getJobsList()) {
        // we assume that job_id is globally unique across all job groups.
        // ImmutableMap will throw exception on build if this invariant is violated.
        if (filter.apply(job)) {
          builder.put(
              job.getJob().getJobId(),
              VersionedProto.from(job, jobGroupEntry.getValue().version()));
        }
      }
    }
    return builder.build();
  }

  private static Map<Long, Versioned<StoredJob>> storedJobGroupMapToStoredJobMap(
      Map<String, Versioned<StoredJobGroup>> jobGroupMap) {
    return storedJobGroupMapToStoredJobMap(jobGroupMap, item -> true);
  }

  /** Get all jobs keyed by job_id. */
  @Override
  public Map<Long, Versioned<StoredJob>> getAll() throws Exception {
    return storedJobGroupMapToStoredJobMap(jobGroupStore.getAll());
  }

  /** Get all jobs keyed by job_id for which the provided filter function returns true. */
  @Override
  public Map<Long, Versioned<StoredJob>> getAll(Function<StoredJob, Boolean> selector)
      throws Exception {
    return storedJobGroupMapToStoredJobMap(jobGroupStore.getAll(), selector);
  }

  /**
   * Get a job by job_id.
   *
   * @implNote instead of actually doing a keyed lookup, this method actually invokes getAll then
   *     filters the output by the provided id so this should not be used where performance is
   *     required.
   */
  @Override
  public Versioned<StoredJob> get(Long id) throws Exception {
    Versioned<StoredJob> item = getAll().get(id);
    if (item == null) {
      throw new IllegalArgumentException("no item found");
    }
    return item;
  }

  /**
   * Get a job by job_id.
   *
   * @implNote this method invokes get so doesn't actually getThrough to underlying storage and
   *     inherits all of the limitations from the get method.
   */
  @Override
  public Versioned<StoredJob> getThrough(Long id) throws Exception {
    return get(id);
  }
}
