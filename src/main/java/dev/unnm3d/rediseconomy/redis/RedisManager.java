package dev.unnm3d.rediseconomy.redis;

import dev.unnm3d.rediseconomy.RedisEconomyPlugin;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public class RedisManager {

    protected static final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final RoundRobinConnectionPool<String, String> roundRobinConnectionPool;
    private final List<StatefulRedisPubSubConnection<String, String>> pubSubConnections;
    protected RedisClient lettuceRedisClient;
    /**
     * How long debits are refused without asking Redis after one got no answer. A hung Redis does not drop
     * the connections, and waiting for each debit there would stall the main thread again and again
     */
    private static final long DEBIT_BACKOFF_MILLIS = 2000;
    /**
     * Pub/sub subscriptions are restored by Lettuce a moment after the connection is back
     */
    private static final long RESYNC_DELAY_MILLIS = 1000;
    private volatile long debitsRefusedUntil = 0;
    private volatile Runnable reconnectTask = () -> {
    };
    private final Set<RedisChannelHandler<?, ?>> disconnectedPubSubs = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean reconnectTaskScheduled = new AtomicBoolean(false);

    public RedisManager(RedisClient lettuceRedisClient, int poolSize) {
        this.lettuceRedisClient = lettuceRedisClient;
        lettuceRedisClient.addListener(new RedisConnectionStateListener() {
            @Override
            public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
                if (connection instanceof StatefulRedisPubSubConnection) disconnectedPubSubs.add(connection);
            }

            @Override
            public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
                //While a pub/sub connection was down this instance missed the updates of the others
                if (disconnectedPubSubs.remove(connection)) scheduleReconnectTask(RESYNC_DELAY_MILLIS);
            }

            @Override
            public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
            }
        });
        this.roundRobinConnectionPool = new RoundRobinConnectionPool<>(lettuceRedisClient::connect, poolSize);
        pubSubConnections = new CopyOnWriteArrayList<>();
    }

    public <T> CompletionStage<T> getConnectionAsync(Function<RedisAsyncCommands<String, String>, CompletionStage<T>> redisCallBack) {
        return redisCallBack.apply(roundRobinConnectionPool.get().async());
    }

    public <T> T getConnectionSync(Function<RedisCommands<String, String>, T> redisCallBack) {
        return redisCallBack.apply(roundRobinConnectionPool.get().sync());
    }

    /**
     * Runs the callback commands in order on a single connection.
     * Connections are shared between threads, so setAutoFlushCommands must not be toggled here: it would
     * leave other threads' commands unflushed until they time out, and forever if the callback throws.
     */
    public <T> CompletionStage<T> getConnectionPipeline(Function<RedisAsyncCommands<String, String>, CompletionStage<T>> redisCallBack) {
        return redisCallBack.apply(roundRobinConnectionPool.get().async());
    }

    /**
     * Executes a transaction with the given Redis commands consumer.
     * If there is an exception during the transaction, it will be discarded.
     * It runs on its own connection: MULTI is per connection state, and pooled connections are shared with other threads.
     *
     * @param redisCommandsConsumer the consumer that will execute the Redis commands
     * @return an Optional containing the result of the redis transaction, or an empty Optional if the transaction was discarded
     */
    public Optional<List<Object>> executeTransaction(Consumer<RedisCommands<String, String>> redisCommandsConsumer) {
        try (StatefulRedisConnection<String, String> connection = lettuceRedisClient.connect()) {
            final RedisCommands<String, String> syncCommands = connection.sync();
            syncCommands.multi();
            redisCommandsConsumer.accept(syncCommands);
            return Optional.of(syncCommands.exec())
                    .filter(result -> !result.wasDiscarded())
                    .map(result -> result.stream().toList());
        }
    }

    public StatefulRedisPubSubConnection<String, String> getPubSubConnection() {
        StatefulRedisPubSubConnection<String, String> pubSubConnection = lettuceRedisClient.connectPubSub();
        pubSubConnections.add(pubSubConnection);
        return pubSubConnection;
    }

    public void expandPool(int expandBy) {
        roundRobinConnectionPool.expandPool(expandBy);
    }

    public void printPool() {
        RedisEconomyPlugin.getInstance().getLogger().warning(roundRobinConnectionPool.printPool());
    }

    /**
     * @param task runs off the main thread after a pub/sub connection reconnected
     */
    public void setReconnectTask(Runnable task) {
        this.reconnectTask = task;
    }

    /**
     * Runs the reconnect task after the delay, once even if more connections come back in the meantime
     */
    public void scheduleReconnectTask(long delayMillis) {
        if (!reconnectTaskScheduled.compareAndSet(false, true)) return;
        try {
            executorService.schedule(() -> {
                reconnectTaskScheduled.set(false);
                reconnectTask.run();
            }, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            reconnectTaskScheduled.set(false); //Shutting down
        }
    }

    /**
     * @return false if asking Redis for a debit would only stall the caller
     */
    public boolean canAttemptDebit() {
        return System.currentTimeMillis() >= debitsRefusedUntil && roundRobinConnectionPool.anyOpen();
    }

    /**
     * Runs the task off the main thread, again and again until the plugin stops
     */
    public void scheduleRepeating(Runnable task, long periodMillis) {
        executorService.scheduleWithFixedDelay(() -> {
            try {
                task.run();
            } catch (RuntimeException e) { //An exception would cancel the next runs
                RedisEconomyPlugin.getInstance().getLogger().warning("Periodic Redis check failed: " + e);
            }
        }, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    public void scheduleOnce(Runnable task, long delayMillis) {
        try {
            executorService.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) { //Shutting down
        }
    }

    public void debitNotAnswered() {
        debitsRefusedUntil = System.currentTimeMillis() + DEBIT_BACKOFF_MILLIS;
    }

    public void debitAnswered() {
        debitsRefusedUntil = 0;
    }

    public void close() {
        pubSubConnections.forEach(StatefulRedisPubSubConnection::close);
        lettuceRedisClient.shutdown(Duration.ofSeconds(1), Duration.ofSeconds(1));
        lettuceRedisClient.getResources().shutdown(1, 1, TimeUnit.SECONDS);
        executorService.shutdown();
    }

    public RedisFuture<String> isConnected() {
        return roundRobinConnectionPool.get().async().get("test");
    }

}
