package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.collect.ImmutableList;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class GrpcManagedChannelPoolTest extends FievelTestBase {
  private ManagedChannel channelOne;
  private ManagedChannel channelTwo;

  private GrpcManagedChannelPool poolWithTwoChannels;

  private MethodDescriptor methodDescriptor;
  private CallOptions callOptions;
  private ClientCall clientCall;

  @Before
  public void setup() {
    channelOne = Mockito.mock(ManagedChannel.class);
    channelTwo = Mockito.mock(ManagedChannel.class);
    Supplier<ManagedChannel> channelProvider = Mockito.mock(Supplier.class);
    Mockito.when(channelProvider.get()).thenReturn(channelOne).thenReturn(channelTwo);
    poolWithTwoChannels = new GrpcManagedChannelPool(channelProvider, 2, 10);

    methodDescriptor = Mockito.mock(MethodDescriptor.class);
    callOptions = CallOptions.DEFAULT;
    clientCall = Mockito.mock(ClientCall.class);

    Mockito.doReturn(clientCall).when(channelOne).newCall(Mockito.any(), Mockito.any());
    Mockito.doReturn(clientCall).when(channelTwo).newCall(Mockito.any(), Mockito.any());

    Mockito.doReturn("authority").when(channelOne).authority();
    Mockito.doReturn("authority-two").when(channelTwo).authority();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyPool() {
    new GrpcManagedChannelPool(Mockito.mock(Supplier.class), 0, 10);
  }

  @Test
  public void testNewCallOnNonEmptyPool() {
    poolWithTwoChannels.newCall(methodDescriptor, callOptions);
    poolWithTwoChannels.newCall(methodDescriptor, callOptions);
    Mockito.verify(channelOne, Mockito.times(1)).newCall(Mockito.any(), Mockito.any());
    Mockito.verify(channelTwo, Mockito.times(1)).newCall(Mockito.any(), Mockito.any());
  }

  @Test
  public void testAuthority() {
    Assert.assertEquals("authority", poolWithTwoChannels.authority());
  }

  @Test
  public void testShutdown() {
    poolWithTwoChannels.shutdown();
  }

  @Test
  public void testIsShutdownAllShutdown() {
    poolWithTwoChannels.shutdown();
    Assert.assertTrue(poolWithTwoChannels.isShutdown());
  }

  @Test
  public void testIsShutdownOneNotShutdown() {
    Mockito.doReturn(true).when(channelOne).isShutdown();
    Mockito.doReturn(false).when(channelTwo).isShutdown();
    Assert.assertFalse(poolWithTwoChannels.isShutdown());
  }

  @Test
  public void testIsTerminatedAllTerminated() {
    Mockito.doReturn(true).when(channelOne).isTerminated();
    Mockito.doReturn(true).when(channelTwo).isTerminated();
    Assert.assertTrue(poolWithTwoChannels.isTerminated());
  }

  @Test
  public void testIsTerminatedAllNotTerminated() {
    Mockito.doReturn(false).when(channelOne).isTerminated();
    Mockito.doReturn(false).when(channelTwo).isTerminated();
    Assert.assertFalse(poolWithTwoChannels.isTerminated());
  }

  @Test
  public void testIsTerminatedOneNotTerminated() {
    Mockito.doReturn(false).when(channelOne).isTerminated();
    Mockito.doReturn(true).when(channelTwo).isTerminated();
    Assert.assertFalse(poolWithTwoChannels.isTerminated());
  }

  @Test
  public void testShutdownNow() {
    poolWithTwoChannels.shutdownNow();
  }

  @Test
  public void testAwaitTerminationAllTermianted() throws Exception {
    Mockito.doReturn(true).when(channelOne).awaitTermination(Mockito.anyLong(), Mockito.any());
    Mockito.doReturn(true).when(channelTwo).awaitTermination(Mockito.anyLong(), Mockito.any());
    Assert.assertTrue(poolWithTwoChannels.awaitTermination(1, TimeUnit.SECONDS));
  }

  @Test
  public void testAwaitTerminationOneNotTerminated() throws Exception {
    Mockito.doReturn(true).when(channelOne).awaitTermination(Mockito.anyLong(), Mockito.any());
    Mockito.doReturn(false).when(channelTwo).awaitTermination(Mockito.anyLong(), Mockito.any());
    Assert.assertFalse(poolWithTwoChannels.awaitTermination(1, TimeUnit.SECONDS));
  }

  @Test
  public void testMetrics() {
    ClientCall poolCall = poolWithTwoChannels.newCall(methodDescriptor, callOptions);
    poolCall.start(Mockito.mock(ClientCall.Listener.class), Mockito.mock(Metadata.class));
    double usage = poolWithTwoChannels.getMetrics().usage();
    Assert.assertEquals(0.05, usage, 0.0001);
    ArgumentCaptor<ClientCall.Listener> listenerArgumentCaptor =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    Mockito.verify(clientCall).start(listenerArgumentCaptor.capture(), Mockito.any());
    ClientCall.Listener listener = listenerArgumentCaptor.getValue();
    listener.onClose(Status.OK, Mockito.mock(Metadata.class));
    usage = poolWithTwoChannels.getMetrics().usage();
    Assert.assertEquals(0.00, usage, 0.0001);
  }

  @Test
  public void testConnectionScaleOut() {
    for (int i = 0; i < 18; ++i) {
      ClientCall poolCall = poolWithTwoChannels.newCall(methodDescriptor, callOptions);
      poolCall.start(Mockito.mock(ClientCall.Listener.class), Mockito.mock(Metadata.class));
    }

    double usage = poolWithTwoChannels.getMetrics().usage();
    Assert.assertEquals(0.9, usage, 0.0001);

    // over usage limit trigger connection pool scaling
    for (int i = 0; i < 2; ++i) {
      ClientCall poolCall = poolWithTwoChannels.newCall(methodDescriptor, callOptions);
      poolCall.start(Mockito.mock(ClientCall.Listener.class), Mockito.mock(Metadata.class));
    }

    usage = poolWithTwoChannels.getMetrics().usage();
    Assert.assertEquals(0.66666, usage, 0.0001);
  }

  @Test(expected = IllegalStateException.class)
  public void testStartCallThrowException() {
    Assert.assertEquals(0, poolWithTwoChannels.getMetrics().inflight());
    Mockito.doThrow(new IllegalStateException())
        .when(clientCall)
        .start(Mockito.any(), Mockito.any());
    ClientCall poolCall = poolWithTwoChannels.newCall(methodDescriptor, callOptions);
    // Assert.assertEquals(1, poolWithTwoChannels.getMetrics().inflight());
    try {
      poolCall.start(Mockito.mock(ClientCall.Listener.class), new Metadata());
    } catch (Exception e) {
      Assert.assertEquals(0, poolWithTwoChannels.getMetrics().inflight());
      throw e;
    }
  }

  @Test
  public void testNextChannelIndexOverflow() {
    ImmutableList.Builder<ManagedChannel> poolBuilder = ImmutableList.builder();
    Supplier<ManagedChannel> channelProvider = Mockito.mock(Supplier.class);
    Mockito.when(channelProvider.get()).thenReturn(channelOne).thenReturn(channelTwo);
    for (int i = 0; i < 5; i++) {
      poolBuilder.add(channelProvider.get());
    }
    GrpcManagedChannelPool.ImmutableChannelPool pool =
        poolWithTwoChannels.new ImmutableChannelPool(poolBuilder.build());

    pool.setIndex(Integer.MAX_VALUE);
    Assert.assertNotNull(pool.next());
    // this should not overflow
    Assert.assertNotNull(pool.next());
  }
}
