package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.base.Preconditions;
import java.util.Objects;

/** health issue of the pipeline */
public class PipelineHealthIssue {
  private final int value;

  /**
   * Instantiates a new Pipeline health issue.
   *
   * @param id the identity of the issue
   */
  public PipelineHealthIssue(int id) {
    Preconditions.checkArgument(id >= 0 && id < 32, "id should be between 0 and 31");
    this.value = 1 << id;
  }

  /**
   * Gets issue value that will be reported with metrics value will be 2 power of identity
   *
   * @return the value
   */
  public int getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipelineHealthIssue that = (PipelineHealthIssue) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
