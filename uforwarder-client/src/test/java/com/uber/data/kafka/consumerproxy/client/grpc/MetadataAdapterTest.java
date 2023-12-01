package com.uber.data.kafka.consumerproxy.client.grpc;

import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.Metadata;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MetadataAdapterTest extends FievelTestBase {
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_GROUP = "test-group";
  private static final String TEST_PARTITION = "3";
  private static final String TEST_OFFSET = "1001";
  private static final String TEST_RETRYCOUNT = "4";
  private static final String TEST_TRACING_ID = "trace-01";
  private static final String TEST_CUSTOM_HEADER = "test01";

  private Supplier<Metadata> mockMetadataSupplier;
  private MetadataAdapter metadataAdapter;
  private Metadata metadata;

  @Before
  public void setup() {
    metadata = Mockito.mock(Metadata.class);
    Mockito.when(metadata.get(Metadata.Key.of("kafka-topic", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_TOPIC);
    Mockito.when(
            metadata.get(Metadata.Key.of("kafka-consumergroup", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_GROUP);
    Mockito.when(metadata.get(Metadata.Key.of("kafka-partition", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_PARTITION);
    Mockito.when(metadata.get(Metadata.Key.of("kafka-offset", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_OFFSET);
    Mockito.when(
            metadata.get(Metadata.Key.of("kafka-retrycount", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_RETRYCOUNT);
    Mockito.when(
            metadata.get(Metadata.Key.of("kafka-tracing-info", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_TRACING_ID);
    Mockito.when(metadata.get(Metadata.Key.of("my-header", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_CUSTOM_HEADER);
    mockMetadataSupplier = Mockito.mock(Supplier.class);
    Mockito.when(mockMetadataSupplier.get()).thenReturn(metadata);
    metadataAdapter = new MetadataAdapter(mockMetadataSupplier);
  }

  @Test
  public void testGetTopic() {
    String topic = metadataAdapter.getTopic();
    Assert.assertEquals(TEST_TOPIC, topic);
  }

  @Test
  public void testGetGroup() {
    String group = metadataAdapter.getConsumerGroup();
    Assert.assertEquals(TEST_GROUP, group);
  }

  @Test
  public void testGetPartition() {
    int partition = metadataAdapter.getPartition();
    Assert.assertEquals(3, partition);
  }

  @Test
  public void testGetOffset() {
    long offset = metadataAdapter.getOffset();
    Assert.assertEquals(1001, offset);
  }

  @Test
  public void testGetRetryCount() {
    long retryCount = metadataAdapter.getRetryCount();
    Assert.assertEquals(4, retryCount);
  }

  @Test
  public void testGetTraceInfo() {
    String value = metadataAdapter.getTracingInfo();
    Assert.assertEquals(TEST_TRACING_ID, value);
  }

  @Test
  public void testGetCustomHeader() {
    String value = metadataAdapter.getHeader("my-header");
    Assert.assertEquals(TEST_CUSTOM_HEADER, value);
  }
}
