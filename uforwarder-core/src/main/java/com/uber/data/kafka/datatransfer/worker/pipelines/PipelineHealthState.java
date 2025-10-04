package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** The type Pipeline heath state. */
class PipelineHealthState {
  private final Ticker ticker;
  private final MutableHealthStateWindow[] windows;
  private final Duration windowDuration;

  /**
   * Instantiates a new Pipeline heath state.
   *
   * @param ticker the ticker
   * @param windowDuration the window duration
   * @param windowCount the window count
   */
  PipelineHealthState(Ticker ticker, Duration windowDuration, int windowCount) {
    this.ticker = ticker;
    this.windows = new MutableHealthStateWindow[windowCount];
    this.windowDuration = windowDuration;
    Arrays.fill(windows, new MutableHealthStateWindow());
  }

  /**
   * Record issue into the state States store is lossy, concurrent record may cause some issue lost
   *
   * @param issue the issue
   */
  void recordIssue(PipelineHealthIssue issue) {
    long newId = newId();
    int index = (int) (newId % windows.length);
    MutableHealthStateWindow currentWindow = windows[index];
    if (currentWindow.id() != newId) {
      currentWindow = new MutableHealthStateWindow();
      windows[index] = currentWindow;
    }
    currentWindow.recordIssue(issue);
  }

  /**
   * Gets all issues
   *
   * @return set of issues
   */
  Set<PipelineHealthIssue> getIssues() {
    long newId = newId();
    int index = (int) (newId % windows.length);
    ImmutableSet.Builder<PipelineHealthIssue> builder = ImmutableSet.builder();
    for (int i = 0; i < windows.length; ++i) {
      MutableHealthStateWindow thisWindow = windows[(index - i + windows.length) % windows.length];
      if (thisWindow.id() == newId - i) {
        // include value when window id is adjacent to current window
        builder.addAll(thisWindow.getIssues());
      }
    }
    return builder.build();
  }

  private long newId() {
    return ticker.read() / windowDuration.toNanos();
  }

  /** The type Mutable health state window. */
  @VisibleForTesting
  protected class MutableHealthStateWindow {
    private final Set<PipelineHealthIssue> issues;
    private final long id;

    protected MutableHealthStateWindow() {
      this.id = newId();
      this.issues = ConcurrentHashMap.newKeySet();
    }

    /**
     * Records issue into current window
     *
     * @param issue the issue
     */
    protected void recordIssue(PipelineHealthIssue issue) {
      issues.add(issue);
    }

    long id() {
      return id;
    }

    /**
     * Gets issues
     *
     * @return the issue set
     */
    Set<PipelineHealthIssue> getIssues() {
      return issues;
    }
  }
}
