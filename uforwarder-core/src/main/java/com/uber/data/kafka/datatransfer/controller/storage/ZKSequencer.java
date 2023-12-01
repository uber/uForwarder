package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryOneTime;

/**
 * ZKSequencer is a sequencer that returns a distributed atomic long via Curator
 * DistributedAtomicLong. https://curator.apache.org/curator-recipes/distributed-atomic-long.html
 *
 * <p>The long is atomically issued by zookeeper so it is persistent across process retarts.
 */
@InternalApi
public final class ZKSequencer<V> implements IdProvider<Long, V> {
  private final CuratorFramework curatorFramework;
  private final AtomicBoolean running;
  private final DistributedAtomicLong sequencer;

  public ZKSequencer(CuratorFramework client, String zkPath) {
    this.sequencer = new DistributedAtomicLong(client, zkPath, new RetryOneTime(0));
    this.curatorFramework = client;
    this.running = new AtomicBoolean(false);
  }

  @Override
  public Long getId(V item) throws Exception {
    AtomicValue<Long> id;
    do {
      id = sequencer.increment();
    } while (!id.succeeded());
    return id.postValue();
  }

  @Override
  public void start() {
    if (curatorFramework.getState() == CuratorFrameworkState.LATENT) {
      curatorFramework.start();
    }
    running.set(true);
  }

  @Override
  public void stop() {
    if (curatorFramework.getState() == CuratorFrameworkState.STARTED) {
      curatorFramework.close();
    }
    running.set(false);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }
}
