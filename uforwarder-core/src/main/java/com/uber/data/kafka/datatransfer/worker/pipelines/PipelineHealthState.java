package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

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
   * Gets state value. Value is a bit set of all issues
   *
   * @return the state value
   */
  int getStateValue() {
    long newId = newId();
    int index = (int) (newId % windows.length);
    int value = 0;
    for (int i = 0; i < windows.length; ++i) {
      MutableHealthStateWindow thisWindow = windows[(index - i + windows.length) % windows.length];
      if (thisWindow.id() == newId - i) {
        // include value when window id is adjacent to current window
        value |= thisWindow.getValue();
      }
    }
    return value;
  }

  private long newId() {
    return ticker.read() / windowDuration.toNanos();
  }

  /** The type Mutable health state window. */
  @VisibleForTesting
  protected class MutableHealthStateWindow {
    private AtomicInteger value;
    private final long id;

    protected MutableHealthStateWindow() {
      this.id = newId();
      this.value = new AtomicInteger();
    }

    /**
     * Records issue into current window
     *
     * @param issue the issue
     */
    protected void recordIssue(PipelineHealthIssue issue) {
      while (true) {
        int oldValue = value.get();
        if (value.compareAndSet(oldValue, oldValue | issue.getValue())) {
          break;
        }
      }
    }

    long id() {
      return id;
    }

    /**
     * Gets state value The value is a bit set of the issues
     *
     * @return the value
     */
    int getValue() {
      return value.get();
    }
  }
}
