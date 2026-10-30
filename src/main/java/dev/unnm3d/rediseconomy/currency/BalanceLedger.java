package dev.unnm3d.rediseconomy.currency;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the local balance cache consistent with Redis while writes are still in flight.
 * <p>
 * Redis owns the balance: writes are sent as deltas and every commit gets a sequence number. The local value shown
 * to callers is the newest committed balance plus the local writes Redis has not acknowledged yet. Acknowledgements
 * and pub/sub updates arrive on different connections, so the sequence number decides which one is newer.
 * <p>
 * Hold this object's monitor to make a check and the following write one atomic step.
 */
final class BalanceLedger<K> {
    /**
     * Sequence number for updates that do not carry one
     */
    static final long UNSEQUENCED = -1;

    private final ConcurrentHashMap<K, Double> balances;
    private final Map<K, Entry> entries = new HashMap<>();

    BalanceLedger(ConcurrentHashMap<K, Double> balances) {
        this.balances = balances;
    }

    private static final class Entry {
        private double committed;
        private long sequence;
        private double pendingDelta;
        private int pendingWrites;
        private int pendingSets;
    }

    private Entry entry(K key) {
        Entry entry = entries.get(key);
        if (entry == null) {
            entry = new Entry();
            entry.committed = balances.getOrDefault(key, 0.0D);
            entries.put(key, entry);
        }
        return entry;
    }

    private void publishLocal(K key, Entry entry) {
        //While a set is in flight the local value already is the result of the local writes
        if (entry.pendingSets == 0) balances.put(key, entry.committed + entry.pendingDelta);
    }

    /**
     * Applies a local relative write. It has to be acknowledged or abandoned later, in the same order
     */
    synchronized void applyDelta(K key, double delta) {
        final Entry entry = entry(key);
        entry.pendingDelta += delta;
        entry.pendingWrites++;
        balances.put(key, balances.getOrDefault(key, 0.0D) + delta);
    }

    /**
     * Applies a local absolute write. It has to be acknowledged or abandoned later, in the same order
     */
    synchronized void applySet(K key, double balance) {
        final Entry entry = entry(key);
        entry.pendingWrites++;
        entry.pendingSets++;
        balances.put(key, balance);
    }

    /**
     * Redis committed a local write
     *
     * @param absolute true if it was written with {@link #applySet}
     * @param delta    the delta given to {@link #applyDelta}, ignored for absolute writes
     * @param balance  the balance Redis stored
     * @param sequence the sequence number of the balance
     */
    synchronized void acknowledge(K key, boolean absolute, double delta, double balance, long sequence) {
        final Entry entry = entries.get(key);
        if (entry == null) return; //Account deleted while the write was in flight
        finishWrite(entry, absolute, delta);
        if (sequence > entry.sequence) {
            entry.sequence = sequence;
            entry.committed = balance;
        }
        publishLocal(key, entry);
    }

    /**
     * A local write will never reach Redis: only what Redis committed remains
     */
    synchronized void abandon(K key, boolean absolute, double delta) {
        final Entry entry = entries.get(key);
        if (entry == null) return;
        finishWrite(entry, absolute, delta);
        publishLocal(key, entry);
    }

    private void finishWrite(Entry entry, boolean absolute, double delta) {
        entry.pendingWrites--;
        if (absolute) entry.pendingSets--;
        else entry.pendingDelta -= delta;
        if (entry.pendingWrites == 0) entry.pendingDelta = 0; //Do not keep floating point residue
    }

    /**
     * A balance committed on Redis by someone else, or read back from Redis
     *
     * @param sequence the sequence number of the balance or {@link #UNSEQUENCED} to always apply it
     */
    synchronized void update(K key, double balance, long sequence) {
        final Entry entry = entry(key);
        if (sequence != UNSEQUENCED) {
            //An older update, delivered late. Equal is a read of the current state, also after a change without sequence (restore)
            if (sequence < entry.sequence) return;
            entry.sequence = sequence;
        }
        entry.committed = balance;
        publishLocal(key, entry);
    }

    /**
     * Drops the account. Acknowledgements of writes still in flight are ignored
     */
    synchronized void remove(K key) {
        entries.remove(key);
        balances.remove(key);
    }
}
