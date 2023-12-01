package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.common.ZKUtils;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

/**
 * ZKStore is an implementation of the Store interface that uses zookeeper as persistent storage.
 *
 * @param <V> is any protobuf generated Message type.
 */
public class ZKStore<K, V extends Message> implements Store<K, V> {
  protected final Logger logger;
  protected final CoreInfra infra;
  // basePath is the base zk path.
  // All data is stored as children of this path.
  protected final ZPath basePath;
  // Client to access zookeeper.
  // We use Apache Curator Async client: https://curator.apache.org/curator-x-async/index.html.
  protected final CachedModeledFramework<V> client;
  // IdProvider atomically distributes long id for use as a primary key.
  protected final IdProvider<K, V> idProvider;
  // KeyFn extracts the key from the item.
  protected final Function<V, K> keyFn;

  protected final AtomicBoolean running;

  public ZKStore(
      Logger logger,
      CoreInfra infra,
      AsyncCuratorFramework client,
      ModelSerializer<V> serializationFactory,
      IdProvider<K, V> idProvider,
      Function<V, K> keyFn,
      String basePath) {
    this(
        logger,
        infra,
        // Convert the low level async curator api into a high level cached typed curator API.
        // The high level API offers the following key features:
        // 1. Read cache that uses Curator TreeCache as the underlying implementation.
        // 2. Versioned API that uses optimistic concurrency control to prevent read-update-write
        // conflicts.
        // 3. Automatic (de)serialization of typed object to bytes.
        ModeledFramework.builder(
                client,
                ModelSpec.builder(ZPath.parseWithIds(basePath), serializationFactory)
                    .withCreateOptions(ImmutableSet.of(CreateOption.createParentsIfNeeded))
                    // DeleteOption.quietly ignores NONODE exception on deletion
                    .withDeleteOptions(ImmutableSet.of(DeleteOption.quietly))
                    .build())
            .withUnhandledErrorListener(
                new UnhandledErrorListener() {
                  @Override
                  public void unhandledError(String message, Throwable e) {
                    infra
                        .scope()
                        .tagged(ImmutableMap.of("reason", message))
                        .counter("zk.failure")
                        .inc(1);
                    logger.error("zk failure", StructuredLogging.reason(message), e);
                  }
                })
            .build()
            .cached(),
        idProvider,
        keyFn,
        ZPath.parseWithIds(basePath));
  }

  @VisibleForTesting
  protected ZKStore(
      Logger logger,
      CoreInfra infra,
      CachedModeledFramework<V> client,
      IdProvider<K, V> idProvider,
      Function<V, K> keyFn,
      ZPath basePath) {
    this.logger = logger;
    this.infra = infra;
    this.client = client;
    this.basePath = basePath;
    this.idProvider = idProvider;
    this.keyFn = keyFn;
    this.running = new AtomicBoolean(false);
  }

  @Override
  public void start() {
    CuratorFramework curatorFramework = client.unwrap().unwrap();
    if (curatorFramework.getState() == CuratorFrameworkState.LATENT) {
      curatorFramework.start();
    }
    client.start();
    logger.info("zk store started");
    running.set(true);
  }

  @Override
  public void stop() {
    try {
      client.close();
    } catch (Exception e) {
      // swallow error because cache is already closed.
      logger.warn("trying to close already closed client", e);
    }
    if (client.unwrap().unwrap().getState() == CuratorFrameworkState.STARTED) {
      client.unwrap().unwrap().close();
    }
    logger.info("zk store stopped");
    running.set(false);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> initialized() throws Exception {
    return initialized(new CompletableFuture<>());
  }

  protected CompletableFuture<Map<K, Versioned<V>>> initialized(
      CompletableFuture<Map<K, Versioned<V>>> future) {
    client
        .listenable()
        .addListener(
            new ModeledCacheListener<V>() {
              @Override
              public void initialized() {
                try {
                  ImmutableMap.Builder<K, Versioned<V>> builder = ImmutableMap.builder();
                  for (ZNode<V> znode : client.cache().currentChildren(ZPath.root).values()) {
                    builder.put(
                        keyFn.apply(znode.model()),
                        VersionedProto.from(znode.model(), znode.stat().getVersion()));
                  }
                  Map<K, Versioned<V>> map = builder.build();
                  logger.info("store.initialized", StructuredLogging.count(map.size()));
                  future.complete(map);
                } catch (Exception e) {
                  future.completeExceptionally(e);
                }
              }

              @Override
              public void accept(Type type, ZPath path, Stat stat, V model) {
                // Noop since this listener exists for signaling cache load.
              }
            });
    return future;
  }

  /**
   * GetAll loads the data on all of the children of the base path.
   *
   * @return a list of data stored in each child node.
   * @throws Exception if this operation fails to complete.
   */
  @Override
  public Map<K, Versioned<V>> getAll() throws Exception {
    return getAll(m -> true);
  }

  /**
   * GetAll returns all elements that are currently stored that pass the {@code selector} check.
   *
   * @param selector is a filter that is used to check whether an element should be returned. Only
   *     elements that return true are returned in the output list.
   * @return list of elements that pass the {@code toReturn} check.
   */
  @Override
  public Map<K, Versioned<V>> getAll(Function<V, Boolean> selector) throws Exception {
    return getAllCache()
        .values()
        .stream()
        .filter(z -> selector.apply(z.model()))
        .collect(Collectors.toMap(k -> keyFn.apply(k.model()), v -> v));
  }

  /**
   * Returns all the cached elements. There are no guarantees of accuracy; this is merely the most
   * recent view of the data.
   *
   * @return all the cached elements.
   */
  private Map<ZPath, Versioned<V>> getAllCache() {
    return client
        .cache()
        .currentChildren(ZPath.root)
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> VersionedProto.from(e.getValue().model(), e.getValue().stat().getVersion())));
  }

  @Override
  public Versioned<V> get(K id) throws Exception {
    ZPath zpath = basePath.resolved(id.toString());
    Optional<ZNode<V>> znodeOpt = client.cache().currentData(zpath);
    if (!znodeOpt.isPresent()) {
      throw new NoSuchElementException(String.format("item %s does not exist in cache", id));
    }
    return VersionedProto.from(znodeOpt.get().model(), znodeOpt.get().stat().getVersion());
  }

  @Override
  public Versioned<V> getThrough(K id) throws Exception {
    ZPath zpath = basePath.resolved(id.toString());
    ZNode<V> znode = client.withPath(zpath).readThroughAsZNode().toCompletableFuture().get();
    return VersionedProto.from(znode.model(), znode.stat().getVersion());
  }

  /**
   * Get or create performs a point lookup of the Store based on the {@code id}. The provided {@code
   * creator} is lazily invoked to create an element if the no element can be found for {@code id}.
   *
   * @param item to insert.
   * @param creator is called to assign key. The returned item is the one that is actually inserted.
   */
  @Override
  public Versioned<V> create(V item, BiFunction<K, V, V> creator) throws Exception {
    K id = idProvider.getId(item);
    V itemWithKey = creator.apply(id, item);
    client
        .withPath(basePath.resolved(id))
        .set(itemWithKey, ZKUtils.NOOP_VERSION)
        .toCompletableFuture()
        .get();
    return getThrough(id);
  }

  /**
   * Put inserts a single element into the zookeeper.
   *
   * @param id for this item. The data is inserted at /basePath/id.
   * @param item contains the object that will be set at /basePath/id. The data is serialized using
   *     the provided serializationFactory.
   * @throws Exception if this operation fails to complete.
   */
  @Override
  public void put(K id, Versioned<V> item) throws Exception {
    putAsync(id, item).toCompletableFuture().get();
  }

  /**
   * putAsync asynchronously inserts a single element into the zookeeper.
   *
   * @param id for this item. The data is inserted at /basePath/id.
   * @param item contains the object that will be set at /basePath/id. The data is serialized using
   *     the provided serializationFactory.
   * @return an AsyncStage which returns the {@link Stat} when the put operation finishes.
   */
  @Override
  public CompletionStage<Void> putAsync(K id, Versioned<V> item) {
    return client
        .withPath(basePath.resolved(id.toString()))
        .update(item.model(), item.version())
        .thenApply(
            stat -> null // swallow stat b/c it isn't necessary
            );
  }

  /**
   * PutThrough inserts a single element into zookeeper.
   *
   * @param id for this item. The data is inserted at /basePath/id.
   * @param item contains the object that will be set at /basePath/id. The data is serialized using
   *     the provided serializationFactory.
   * @throws Exception if this operation fails to complete.
   */
  @Override
  public void putThrough(K id, Versioned<V> item) throws Exception {
    putAsync(id, item).toCompletableFuture().get();
  }

  /**
   * Remove this znode from zookeeper.
   *
   * @param id for this item to remove.
   * @throws Exception if deleting the znode failed or timeout. The implementation swallows
   *     NoNodeException.
   */
  @Override
  public void remove(K id) throws Exception {
    try {
      client.withPath(basePath.resolved(id.toString())).delete().toCompletableFuture().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        logger.debug("failed to remove non-existent entry", e);
        return;
      }
      // only catch NoNodeException
      throw e;
    }
  }
}
