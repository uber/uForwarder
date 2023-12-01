package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

/** SeekStartOffsetOption indicates how to seek start offset. */
public enum SeekStartOffsetOption {
  /** DO_NOT_SEEK indicates that there is no need to seek start offset. */
  DO_NOT_SEEK,
  /** SEEK_TO_EARLIEST_OFFSET indicates seeking to the earliest available offset. */
  SEEK_TO_EARLIEST_OFFSET,
  /** SEEK_TO_LATEST_OFFSET indicates seeking to the latest available offset. */
  SEEK_TO_LATEST_OFFSET,
  /** SEEK_TO_SPECIFIED_OFFSET indicates seeking to the one that's specified in configurations. */
  SEEK_TO_SPECIFIED_OFFSET
}
