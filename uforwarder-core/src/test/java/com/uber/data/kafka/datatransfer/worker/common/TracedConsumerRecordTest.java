package com.uber.data.kafka.datatransfer.worker.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.uber.fievel.testing.base.FievelTestBase;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.tag.Tag;
import io.opentracing.tag.Tags;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TracedConsumerRecordTest extends FievelTestBase {
  private static final String TRACE_ID = "uber-trace-id";
  boolean extracted;
  boolean injected;
  Tracer tracer;
  Tracer.SpanBuilder spanBuilder;
  SpanContext spanContext;
  Span span;
  int count;

  @Before
  public void setup() {
    extracted = false;
    injected = false;
    tracer = mock(Tracer.class);
    spanBuilder = mock(Tracer.SpanBuilder.class);
    spanContext = mock(SpanContext.class);
    span = mock(Span.class);
    when(tracer.buildSpan(Mockito.anyString())).thenReturn(spanBuilder);
    doReturn(spanBuilder).when(spanBuilder).withTag(Mockito.anyString(), Mockito.anyString());
    doReturn(spanBuilder).when(spanBuilder).withTag(Mockito.any(Tag.class), Mockito.anyString());
    doReturn(spanBuilder).when(spanBuilder).withTag(Mockito.any(Tag.class), Mockito.anyBoolean());
    when(spanBuilder.start()).thenReturn(span);
    when(span.context()).thenReturn(spanContext);
    Mockito.doAnswer(
            invocationOnMock -> {
              Object[] args = invocationOnMock.getArguments();
              Assert.assertTrue(args[2] instanceof TracedConsumerRecord.HeadersMapInjectAdapter);
              TextMap textMap = (TextMap) args[2];
              textMap.put(TRACE_ID, "trace-id-2");
              injected = true;
              return null;
            })
        .when(tracer)
        .inject(
            Mockito.any(SpanContext.class), Mockito.any(Format.class), Mockito.any(TextMap.class));
    count = 0;
  }

  ConsumerRecord<String, byte[]> createConsumerRecord(String topic) {
    return new ConsumerRecord<String, byte[]>(
        topic,
        0,
        0,
        0,
        TimestampType.CREATE_TIME,
        0L,
        0,
        0,
        "key",
        new byte[0],
        new RecordHeaders());
  }

  @Test
  public void testTryActivateSpan() {
    when(tracer.extract(Mockito.any(Format.class), Mockito.any(TextMap.class)))
        .thenAnswer(
            (Answer)
                invocationOnMock -> {
                  Object[] args = invocationOnMock.getArguments();

                  Assert.assertTrue(
                      args[1] instanceof TracedConsumerRecord.HeadersMapExtractAdapter);
                  TextMap textMap = (TextMap) args[1];
                  Iterator<Map.Entry<String, String>> iter = textMap.iterator();
                  while (iter.hasNext()) {
                    Map.Entry<String, String> entry = iter.next();
                    if (entry.getKey().equals(TRACE_ID)) {
                      Assert.assertEquals("trace-id-1", entry.getValue());
                      extracted = true;
                    }
                  }
                  return spanContext;
                });
    ConsumerRecord record = createConsumerRecord("topic1");
    record.headers().add(new RecordHeader("header1", "value1".getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader(TRACE_ID, "trace-id-1".getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader("header2", "value2".getBytes(StandardCharsets.UTF_8)));
    Long offset = 10L;

    TracedConsumerRecord tracedConsumerRecord = TracedConsumerRecord.of(record, tracer, "cg");
    Assert.assertTrue(extracted);
    Assert.assertTrue(injected);
    Assert.assertEquals(3, record.headers().toArray().length);
    boolean matched =
        StreamSupport.stream(record.headers().spliterator(), false)
            .filter(
                header -> {
                  if (header.key().equals(TRACE_ID)) {
                    return true;
                  }
                  return false;
                })
            .map(
                header -> {
                  count++;
                  return header;
                })
            .allMatch(
                header -> new String(header.value(), StandardCharsets.UTF_8).equals("trace-id-2"));
    Assert.assertEquals(1, count);
    Assert.assertTrue(matched);
    tracedConsumerRecord.complete(offset, null);
    verify(span).log(ImmutableMap.of("message", offset));
    verify(span).finish();
  }

  @Test
  public void testTryInjectSpanEmpty() {
    when(tracer.extract(Mockito.any(Format.class), Mockito.any(TextMap.class))).thenReturn(null);
    ConsumerRecord record = createConsumerRecord("topic");
    record.headers().add(new RecordHeader("header1", "value1".getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader(TRACE_ID, "trace-id-1".getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader("header2", "value2".getBytes(StandardCharsets.UTF_8)));

    Exception e = new IllegalStateException();

    TracedConsumerRecord tracedConsumerRecord = TracedConsumerRecord.of(record, tracer, "cg");
    Mockito.verify(spanBuilder, Mockito.never()).asChildOf(Mockito.any(SpanContext.class));
    Mockito.verify(spanBuilder, Mockito.never()).withTag(Tags.ERROR, Boolean.TRUE);
    Assert.assertFalse(extracted);
    Assert.assertFalse(injected);
    Assert.assertEquals(3, record.headers().toArray().length);
    boolean matched =
        StreamSupport.stream(record.headers().spliterator(), false)
            .filter(
                header -> {
                  if (header.key().equals(TRACE_ID)) {
                    return true;
                  }
                  return false;
                })
            .map(
                header -> {
                  count++;
                  return header;
                })
            .allMatch(
                header -> new String(header.value(), StandardCharsets.UTF_8).equals("trace-id-1"));
    Assert.assertEquals(1, count);
    Assert.assertTrue(matched);
    tracedConsumerRecord.complete(null, e);
    verify(span, never()).log(ImmutableMap.of("error.object", e));
    verify(span, never()).finish();
  }

  @Test
  public void testTryInjectSpanFailed() {
    when(tracer.extract(Mockito.any(Format.class), Mockito.any(TextMap.class)))
        .thenThrow(new IllegalArgumentException());
    ConsumerRecord record = createConsumerRecord("topic");
    record.headers().add(new RecordHeader("header1", "value1".getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader(TRACE_ID, "trace-id-1".getBytes(StandardCharsets.UTF_8)));
    record.headers().add(new RecordHeader("header2", "value2".getBytes(StandardCharsets.UTF_8)));

    Exception e = new IllegalStateException();
    TracedConsumerRecord tracedConsumerRecord = TracedConsumerRecord.of(record, tracer, "cg");
    Mockito.verify(spanBuilder, Mockito.never()).asChildOf(Mockito.any(SpanContext.class));
    Mockito.verify(spanBuilder).withTag(Tags.ERROR, Boolean.TRUE);
    Assert.assertFalse(extracted);
    Assert.assertTrue(injected);
    Assert.assertEquals(3, record.headers().toArray().length);
    boolean matched =
        StreamSupport.stream(record.headers().spliterator(), false)
            .filter(
                header -> {
                  if (header.key().equals(TRACE_ID)) {
                    return true;
                  }
                  return false;
                })
            .map(
                header -> {
                  count++;
                  return header;
                })
            .allMatch(
                header -> new String(header.value(), StandardCharsets.UTF_8).equals("trace-id-2"));
    Assert.assertEquals(1, count);
    Assert.assertTrue(matched);
    tracedConsumerRecord.complete(null, e);
    verify(span).log(ImmutableMap.of("error.object", e));
    verify(span).finish();
  }

  @Test
  public void verifyNullHeaderHandled() {
    Headers headers = new RecordHeaders();
    headers.add("test_null_header", null);
    TracedConsumerRecord.HeadersMapExtractAdapter headersMapExtractAdapter =
        new TracedConsumerRecord.HeadersMapExtractAdapter(headers);
    Map.Entry<String, String> header = headersMapExtractAdapter.iterator().next();
    assertNotNull(header);
    assertEquals(header.getKey(), "test_null_header");
    assertNull(header.getValue());
  }

  @Test
  public void verifyBase64EncodedHeaderHandled() {
    Headers headers = new RecordHeaders();
    String valueFromPresto =
        "eyJzZXJ2aWNlcyI6W3sicG9vbCI6ImJpdHMtdWNpIiwic2VydmljZV9uYW1lIjoiZnVsZmlsbG1lbnQtdGhpcmQtcGFydHkiLCJob3N0IjoiZGNhOC0zdzgucHJvZC51YmVyLmludGVybmFsIiwicG9ydHMiOnsiaHR0cCI6MzEyNzcsImh0dHAyIjozMTU5NiwidGNoYW5uZWwiOjMxNjQwfX1dfQ%3D%3D";
    headers.add("test_null_header", valueFromPresto.getBytes());
    TracedConsumerRecord.HeadersMapExtractAdapter headersMapExtractAdapter =
        new TracedConsumerRecord.HeadersMapExtractAdapter(headers);
    Map.Entry<String, String> header = headersMapExtractAdapter.iterator().next();
    assertNotNull(header);
    assertEquals(header.getKey(), "test_null_header");
    assertEquals(
        "eyJzZXJ2aWNlcyI6W3sicG9vbCI6ImJpdHMtdWNpIiwic2VydmljZV9uYW1lIjoiZnVsZmlsbG1lbnQtdGhpcmQtcGFydHkiLCJob3N0IjoiZGNhOC0zdzgucHJvZC51YmVyLmludGVybmFsIiwicG9ydHMiOnsiaHR0cCI6MzEyNzcsImh0dHAyIjozMTU5NiwidGNoYW5uZWwiOjMxNjQwfX1dfQ==",
        header.getValue());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testPut() {
    new TracedConsumerRecord.HeadersMapExtractAdapter(new RecordHeaders()).put("key", "value");
  }
}
