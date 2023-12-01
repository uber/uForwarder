package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.collect.ImmutableList;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
    poolWithTwoChannels =
        new GrpcManagedChannelPool(ImmutableList.of(channelOne, channelTwo), new AtomicLong(0));

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
    new GrpcManagedChannelPool(ImmutableList.of(), new AtomicLong(0));
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
    Mockito.doReturn(true).when(channelOne).isShutdown();
    Mockito.doReturn(true).when(channelTwo).isShutdown();
    Assert.assertTrue(poolWithTwoChannels.isShutdown());
  }

  @Test
  public void testIsShutdownAllNotShutdown() {
    Mockito.doReturn(false).when(channelOne).isShutdown();
    Mockito.doReturn(false).when(channelTwo).isShutdown();
    Assert.assertFalse(poolWithTwoChannels.isShutdown());
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
}
