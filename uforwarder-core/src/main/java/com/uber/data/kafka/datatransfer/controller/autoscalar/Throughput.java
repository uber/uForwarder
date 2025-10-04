package com.uber.data.kafka.datatransfer.controller.autoscalar;

/** Consumer job/group Throughput */
public class Throughput {

  /** The constant ZERO. */
  public static final Throughput ZERO = Throughput.of(Scalar.ZERO, Scalar.ZERO);

  private final double messagesPerSecond;
  private final double bytesPerSecond;

  /**
   * Instantiates a new Throughput.
   *
   * @param messagesPerSecond the messages per second
   * @param bytesPerSecond the bytes per second
   */
  public Throughput(double messagesPerSecond, double bytesPerSecond) {
    this.messagesPerSecond = messagesPerSecond;
    this.bytesPerSecond = bytesPerSecond;
  }

  /**
   * Gets messages per second.
   *
   * @return the messages per second
   */
  public double getMessagesPerSecond() {
    return messagesPerSecond;
  }

  /**
   * Gets bytes per second.
   *
   * @return the bytes per second
   */
  public double getBytesPerSecond() {
    return bytesPerSecond;
  }

  /**
   * Instantiates a new Throughput.
   *
   * @param messagesPerSecond the messages per second
   * @param bytesPerSecond the bytes per second
   * @return the throughput
   */
  public static Throughput of(double messagesPerSecond, double bytesPerSecond) {
    return new Throughput(messagesPerSecond, bytesPerSecond);
  }
}
