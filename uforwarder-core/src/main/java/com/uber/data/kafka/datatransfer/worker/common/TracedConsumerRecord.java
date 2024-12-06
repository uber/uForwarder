package com.uber.data.kafka.datatransfer.worker.common;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.tag.Tags;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.codec.net.URLCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerRecord with tracing span context. Span created by constructor should be finished by
 * calling complete() function. The class is needed to propagate span context cross fetcher and
 * processor thread boundary
 *
 * <p>TODO: (chenz) move kafka tracing utilities into common library
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 */
public class TracedConsumerRecord<K, V> extends ConsumerRecord<K, V> {
  public static final Logger LOGGER = LoggerFactory.getLogger(TracedConsumerRecord.class);
  public static final String TAG_TOPIC = "topic";
  public static final String TAG_CONSUMER_GROUP = "consumerGroup";
  private static final String OPERATION_NAME = "TransferMessage";
  private final Optional<Span> span;

  private TracedConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
    super(
        consumerRecord.topic(),
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        consumerRecord.timestampType(),
        consumerRecord.checksum(),
        consumerRecord.serializedKeySize(),
        consumerRecord.serializedValueSize(),
        consumerRecord.key(),
        consumerRecord.value());
    this.span = Optional.empty();
  }

  private TracedConsumerRecord(
      ConsumerRecord<K, V> consumerRecord, Tracer tracer, String consumerGroup) {
    super(
        consumerRecord.topic(),
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        consumerRecord.timestampType(),
        consumerRecord.checksum(),
        consumerRecord.serializedKeySize(),
        consumerRecord.serializedValueSize(),
        consumerRecord.key(),
        consumerRecord.value(),
        consumerRecord.headers(),
        consumerRecord.leaderEpoch());
    this.span = tryCreateSpan(tracer, consumerGroup);
  }

  public static <K, V> TracedConsumerRecord<K, V> of(
      ConsumerRecord<K, V> consumerRecord, Tracer tracer, String consumerGroup) {
    if (consumerRecord.headers() == null) {
      return new TracedConsumerRecord(consumerRecord);
    } else {
      return new TracedConsumerRecord(consumerRecord, tracer, consumerGroup);
    }
  }

  /**
   * This method inject message transfer span into tracing graph, it - Extract span context from
   * carrier - Create message transfer span as child span - Inject the child span into carrier -
   * Finally returns a cleanup procedure to close the child span
   *
   * <p>if span context doesn't exist, skip emitting span and return NOOP if span context corrupt,
   * wrong version etc, create error span otherwise create new span as child of parent span finally,
   * return procedure to close span
   *
   * <p>Note, currently span activation is not in scope.
   *
   * @param tracer the tracer
   * @return Optional<Span>
   */
  protected Optional<Span> tryCreateSpan(Tracer tracer, String consumerGroup) {
    final String topic = topic();
    final Headers headers = headers();
    if (tracer == null || headers == null) {
      return Optional.empty();
    }

    Tracer.SpanBuilder spanBuilder =
        tracer
            .buildSpan(OPERATION_NAME)
            .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER)
            .withTag(TAG_CONSUMER_GROUP, consumerGroup)
            .withTag(TAG_TOPIC, topic);
    Map<String, Object> fields = Collections.emptyMap();
    try {
      SpanContext spanContext =
          tracer.extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(headers));
      if (spanContext != null) {
        spanBuilder.asChildOf(spanContext);
      }
    } catch (Throwable throwable) {
      spanBuilder = spanBuilder.withTag(Tags.ERROR, Boolean.TRUE);
      fields =
          ImmutableMap.of(
              Fields.ERROR_OBJECT,
              new RuntimeException("parent span context extract failed", throwable));
    }

    Span span = spanBuilder.start();
    if (!fields.isEmpty()) {
      span.log(fields);
    }

    SpanContext spanContext = span.context();
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new HeadersMapInjectAdapter(headers));
    return Optional.of(span);
  }

  public Optional<Span> span() {
    return span;
  }

  public void complete(Object result, Object error) {
    if (!span.isPresent()) {
      return;
    }
    synchronized (span) {
      if (result != null) {
        span.get().log(ImmutableMap.of(Fields.MESSAGE, result));
      }
      if (error != null) {
        span.get().log(ImmutableMap.of(Fields.ERROR_OBJECT, error));
      }

      span.get().finish();
    }
  }

  /** Inject spanContext and update trace info in carrier */
  static class HeadersMapInjectAdapter implements TextMap {
    private final Set<String> keys = new HashSet<>();
    private final Headers headers;

    HeadersMapInjectAdapter(Headers headers) {
      this.headers = headers;
      for (Header header : headers) {
        keys.add(header.key());
      }
    }

    @Override
    public void put(String key, String value) {
      if (!keys.contains(key)) {
        return;
      }

      headers.remove(key);
      headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      throw new UnsupportedOperationException(
          "HeadersMapInjectAdapter should only be used with Tracer.inject()");
    }
  }

  /**
   * Duplicated from
   * opentracing-kafka-client/src/main/java/io/opentracing/contrib/kafka/HeadersMapExtractAdapter.java
   */
  static final class HeadersMapExtractAdapter implements TextMap {
    private final Map<String, String> map = new HashMap<>();

    HeadersMapExtractAdapter(Iterable<Header> headers) {
      for (Header header : headers) {
        map.put(header.key(), decodeHeaderValue(header.key(), header.value()));
      }
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      return map.entrySet().iterator();
    }

    @Override
    public void put(String key, String value) {
      throw new UnsupportedOperationException(
          "HeadersMapExtractAdapter should only be used with Tracer.extract()");
    }

    @Nullable
    private String decodeHeaderValue(String headerKey, byte[] headerValue) {
      if (headerValue == null) {
        return null;
      }
      // We have to decode explicitly for Jaeger baggage items because if we don't do that, Jaeger
      // library will do it again and cause double URL encoding, failing everything relies on it.
      // More details can be found at https://t3.uberinternal.com/browse/QRK-2120
      try {
        return new String(URLCodec.decodeUrl(headerValue), StandardCharsets.UTF_8);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to decode header value",
            e,
            StructuredLogging.headerKey(headerKey),
            StructuredLogging.headerValue(headerValue));
      }
      return new String(headerValue, StandardCharsets.UTF_8);
    }
  }
}
