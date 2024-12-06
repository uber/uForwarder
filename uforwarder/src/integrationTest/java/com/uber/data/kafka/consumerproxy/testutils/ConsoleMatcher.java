package com.uber.data.kafka.consumerproxy.testutils;

import static org.awaitility.Awaitility.await;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** test ulitity to find matching string using regular expression from console output */
public class ConsoleMatcher {
  private final PrintStream originalOut;
  private final PrintStream newOut;
  private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Instantiates a new Console matcher. */
  public ConsoleMatcher() {
    this.originalOut = System.out;
    this.newOut = new PrintStream(new CompositeOutputStream(buffer, System.out));

    System.setOut(newOut);
  }

  /**
   * Find matching regular expression in console output
   *
   * @param regex the regex
   * @return the boolean indicates if match found
   */
  public boolean findMatch(String regex) {
    String output = buffer.toString();
    Pattern pattern = Pattern.compile(regex);
    Matcher m = pattern.matcher(output);
    return m.find();
  }

  /**
   * Closes the instance and release resources
   *
   * @return the boolean
   * @throws IOException the io exception
   */
  public boolean close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      System.setOut(originalOut);
      buffer.close();
      return true;
    }
    return false;
  }

  /**
   * Tests if the console output matches the regex with bounded timeout
   *
   * @param regex the regex
   * @param maxAwaitTimeInSec the max await time in sec in total
   * @throws IOException the io exception
   */
  public static void assertMatches(String regex, int maxAwaitTimeInSec) throws IOException {
    ConsoleMatcher captor = new ConsoleMatcher();
    try {
      await().atMost(maxAwaitTimeInSec, TimeUnit.SECONDS).until(() -> captor.findMatch(regex));
    } finally {
      captor.close();
    }
  }

  /**
   * Combines output streams together, so each write to teh composite output stream will apply to
   * each internal output stream
   */
  private static class CompositeOutputStream extends OutputStream {
    private List<OutputStream> outputStreams;

    /**
     * Instantiates a new composite output stream.
     *
     * @param outputStreams the output streams
     */
    public CompositeOutputStream(OutputStream... outputStreams) {
      this.outputStreams = Arrays.asList(outputStreams);
    }

    @Override
    public void write(int b) throws IOException {
      for (OutputStream os : outputStreams) {
        os.write(b);
      }
    }

    @Override
    public void flush() throws IOException {
      for (OutputStream os : outputStreams) {
        os.flush();
      }
    }
  }
}
