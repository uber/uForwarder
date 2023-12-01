package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.data.kafka.datatransfer.Job;
import java.util.Objects;

/**
 * Input object type of {@link Sink}
 *
 * @param <R> the type parameter
 */
public class ItemAndJob<R> {
  private final R item;
  private final Job job;

  private ItemAndJob(R item, Job job) {
    this.item = item;
    this.job = job;
  }

  /**
   * Gets item.
   *
   * @return the item
   */
  public R getItem() {
    return item;
  }

  /**
   * Gets job.
   *
   * @return the job
   */
  public Job getJob() {
    return job;
  }

  /**
   * Constructs instance
   *
   * @param <R> the type parameter
   * @param record the record
   * @param job the job
   * @return the item and job
   */
  public static <R> ItemAndJob<R> of(R record, Job job) {
    return new ItemAndJob<>(record, job);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ItemAndJob<?> that = (ItemAndJob<?>) o;
    return Objects.equals(item, that.item) && Objects.equals(job, that.job);
  }

  @Override
  public int hashCode() {
    return Objects.hash(item, job);
  }
}
