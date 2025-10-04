package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.base.Preconditions;
import java.util.Objects;

/** health issue of the pipeline */
public class PipelineHealthIssue {
  private final String name;

  /**
   * Instantiates a new Pipeline health issue.
   *
   * @param name the identity of the issue
   */
  public PipelineHealthIssue(String name) {
    Preconditions.checkArgument(
        org.apache.commons.lang3.StringUtils.isNotEmpty(name), "tag should not be empty");
    this.name = name;
  }

  /**
   * Gets tag of the issue, issue be reported with the name
   *
   * @return the value
   */
  public String getName() {
    return name;
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
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
