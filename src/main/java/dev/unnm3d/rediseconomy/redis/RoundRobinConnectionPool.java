package dev.unnm3d.rediseconomy.redis;


import io.lettuce.core.api.StatefulRedisConnection;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class RoundRobinConnectionPool<K, V> {
    private final AtomicInteger next = new AtomicInteger(0);
    private volatile StatefulRedisConnection<K, V>[] elements;
    private final Supplier<StatefulRedisConnection<K, V>> statefulRedisConnectionSupplier;

    public RoundRobinConnectionPool(Supplier<StatefulRedisConnection<K, V>> statefulRedisConnectionSupplier, int poolSize) {
        this.statefulRedisConnectionSupplier = statefulRedisConnectionSupplier;
        this.elements = new StatefulRedisConnection[poolSize];
        for (int i = 0; i < poolSize; i++) {
            elements[i] = statefulRedisConnectionSupplier.get();
        }
    }

    public void expandPool(int expandBy) {
        if (expandBy <= 0)
            throw new IllegalArgumentException("expandBy must be greater than 0");
        final StatefulRedisConnection<K, V>[] expanded = Arrays.copyOf(elements, elements.length + expandBy);
        for (int i = elements.length; i < expanded.length; i++) {
            expanded[i] = statefulRedisConnectionSupplier.get();
        }
        this.elements = expanded;
    }

    public StatefulRedisConnection<K, V> get() {
        final StatefulRedisConnection<K, V>[] current = elements;
        int index = Math.floorMod(next.getAndIncrement(), current.length);
        StatefulRedisConnection<K, V> disconnected = null;
        for (int i = 0; i < current.length; i++) {
            final StatefulRedisConnection<K, V> connection = current[Math.floorMod(index + i, current.length)];
            if (connection.isMulti()) continue;
            if (connection.isOpen()) return connection;
            if (disconnected == null) disconnected = connection;
        }
        //Lettuce reconnects it and sends the command then. Opening a new connection here would block the
        //caller, usually the main thread, throw while Redis is down and leak the connection it replaces
        if (disconnected != null) return disconnected;

        final StatefulRedisConnection<K, V> connection = statefulRedisConnectionSupplier.get();
        current[index] = connection;
        return connection;
    }

    /**
     * @return true if at least one connection is up right now
     */
    public boolean anyOpen() {
        for (StatefulRedisConnection<K, V> element : elements) {
            if (element.isOpen()) return true;
        }
        return false;
    }

    public String printPool() {
        StringBuilder sb = new StringBuilder();
        for (StatefulRedisConnection<K, V> element : elements) {
            sb.append("Open: ")
                    .append(element.isOpen())
                    .append(", multi: ")
                    .append(element.isMulti())
                    .append(", hash")
                    .append(element.hashCode())
                    .append("\n");
        }
        return sb.toString();
    }


    public void close() {
        for (StatefulRedisConnection<K, V> element : elements) {
            element.closeAsync();
        }
    }

}