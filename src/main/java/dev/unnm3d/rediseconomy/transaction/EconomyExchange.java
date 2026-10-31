package dev.unnm3d.rediseconomy.transaction;

import dev.unnm3d.rediseconomy.RedisEconomyPlugin;
import dev.unnm3d.rediseconomy.api.TransactionEvent;
import dev.unnm3d.rediseconomy.currency.Currency;
import dev.unnm3d.rediseconomy.redis.RedisKeys;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import org.bukkit.command.CommandSender;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class EconomyExchange {

    private static final String REVERT_IN_PROGRESS = "-1";

    private final RedisEconomyPlugin plugin;
    private final ExecutorService executorService;
    private long updateTIDTimestamp = System.currentTimeMillis();
    private int lastTID = 0;

    /**
     * Constructor for EconomyExchange
     *
     * @param plugin The RedisEconomyPlugin instance
     */
    public EconomyExchange(final RedisEconomyPlugin plugin) {
        this.plugin = plugin;
        this.executorService = Executors.newFixedThreadPool(plugin.getConfigManager().getSettings().transactionExecutorThreads,
                Thread.ofVirtual().factory());
    }

    /**
     * Get transactions from an account id
     *
     * @param accountId Account id
     * @param limit     Maximum number of transactions to return
     * @return Map of transaction ids and transactions
     */
    public CompletionStage<TreeMap<Long, Transaction>> getTransactions(AccountID accountId, int limit) {
        return CompletableFuture.supplyAsync(() -> readTransactions(accountId, limit), executorService)
                .exceptionally(exc -> {
                    exc.printStackTrace();
                    return new TreeMap<>(); // Return empty map instead of null for better error handling
                });
    }

    private TreeMap<Long, Transaction> readTransactions(AccountID accountId, int limit) {
        final Map<String, String> transactions = plugin.getCurrenciesManager().getRedisManager().getConnectionSync(connection ->
                connection.hgetall(RedisKeys.TRANSACTIONS + accountId.toString()));
        if (transactions == null || transactions.isEmpty()) {
            return new TreeMap<>();
        }

        final TreeMap<Long, Transaction> transactionsMap = new TreeMap<>();
        transactions.entrySet().stream().toList().stream()
                .sorted(Comparator.<Map.Entry<String, String>>comparingLong(entry ->
                        Long.parseLong(entry.getKey())).reversed())
                .limit(limit)
                .forEach(entry -> transactionsMap.put(
                        Long.parseLong(entry.getKey()),
                        Transaction.fromString(entry.getValue())
                ));
        return transactionsMap;
    }

    public int getCurrentTransactionID() {
        if (System.currentTimeMillis() - this.updateTIDTimestamp > 10000 || this.lastTID == 0) {
            plugin.getCurrenciesManager().getRedisManager()
                    .getConnectionAsync(connection ->
                            connection.get(RedisKeys.TRANSACTIONS_COUNTER.toString()))
                    .thenAccept(s -> this.lastTID = Integer.parseInt(Optional.ofNullable(s).orElse("0")));
            this.updateTIDTimestamp = System.currentTimeMillis();
        }
        return this.lastTID;
    }

    /**
     * Remove all transactions from Redis
     *
     * @return How many transaction accounts were removed
     */
    public CompletionStage<Long> removeAllTransactions() {
        return plugin.getCurrenciesManager().getRedisManager().getConnectionAsync(connection -> {
                    try {
                        List<String> keys = connection.keys(RedisKeys.TRANSACTIONS + "*").get();
                        if (keys.isEmpty()) {
                            return CompletableFuture.completedFuture(0L);
                        }
                        return connection.del(keys.toArray(new String[0]));
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    /**
     * Get transactions from an account id and transaction id
     *
     * @param accountId Account id
     * @param id        Transaction id
     * @return Transaction
     */
    public CompletionStage<Transaction> getTransaction(@NotNull AccountID accountId, long id) {
        return CompletableFuture.supplyAsync(() -> {
            final String serialized = plugin.getCurrenciesManager().getRedisManager().getConnectionSync(connection ->
                    connection.hget(RedisKeys.TRANSACTIONS + accountId.toString(), String.valueOf(id)));
            return serialized == null ? null : Transaction.fromString(serialized);
        }, executorService).orTimeout(plugin.getConfigManager().getSettings().redis.timeout(), TimeUnit.MILLISECONDS).exceptionally(exc -> {
            exc.printStackTrace();
            return null;
        });
    }

    /**
     * Save payment transaction
     *
     * @param sender   Sender of the transaction
     * @param target   Target of the transaction
     * @param amount   Amount of the transaction
     * @param currency Currency of the transaction
     * @param reason   Reason of the transaction
     * @return List of ids: the first one is the id of the transaction on the sender side, the second one is the id of the transaction on the target side
     */
    public CompletionStage<List<Long>> savePaymentTransaction(@NotNull UUID sender, @NotNull UUID target, double amount, @NotNull Currency currency, @NotNull String reason) {
        if (!currency.shouldSaveTransactions()) return CompletableFuture.completedStage(List.of((long) -1, (long) -1));

        long init = System.currentTimeMillis();
        final String stackTrace = getCallerPluginString();
        return CompletableFuture.supplyAsync(() -> {
                    TransactionEvent transactionSenderEvent = new TransactionEvent(new Transaction(
                            new AccountID(sender),
                            new AccountID(target), currency.getCurrencyName(), System.currentTimeMillis(),
                            -amount,
                            null, reason + stackTrace
                    ));
                    TransactionEvent transactionReceiverEvent = new TransactionEvent(new Transaction(
                            new AccountID(target),
                            new AccountID(sender), currency.getCurrencyName(), System.currentTimeMillis(),
                            amount,
                            null, reason + stackTrace
                    ));

                    callEvents(transactionSenderEvent, transactionReceiverEvent);

                    //noinspection unchecked
                    return untilRedisAnswers("payment of " + sender + " to " + target, () -> plugin.getCurrenciesManager().getRedisManager().getConnectionSync(connection ->
                            (List<Long>) connection.eval(
                                    "local a=redis.call('incr',KEYS[1])local b=redis.call('incr',KEYS[1])redis.call('hset',KEYS[2],a,ARGV[1])redis.call('hset',KEYS[3],b,ARGV[2])if tonumber(ARGV[3])>0 then redis.call('hexpire',KEYS[2],ARGV[3],'FIELDS',1,a)redis.call('hexpire',KEYS[3],ARGV[3],'FIELDS',1,b)end;return{a,b}",
                                    ScriptOutputType.MULTI,
                                    new String[]{
                                            RedisKeys.TRANSACTIONS_COUNTER.toString(),
                                            RedisKeys.TRANSACTIONS + sender.toString(),
                                            RedisKeys.TRANSACTIONS + target.toString()}, //Key rediseco:transactions:playerUUID
                                    transactionSenderEvent.getTransaction().toString(),
                                    transactionReceiverEvent.getTransaction().toString(),
                                    String.valueOf(currency.getTransactionsTTL()))));
                }, executorService).orTimeout(plugin.getConfigManager().getSettings().redis.timeout(), TimeUnit.MILLISECONDS)
                .exceptionally(exc -> {
                    RedisEconomyPlugin.debug("ERROR!!!!! 03payment Exception while saving transaction for " + sender + " and " + target + ": " + exc.getMessage());
                    return List.of((long) -1, (long) -1);
                })
                .thenApply(response -> {
                    RedisEconomyPlugin.debug("03payment Transaction for " + sender + " saved in " + (System.currentTimeMillis() - init) + " ms with id " + response.get(0) + " !");
                    RedisEconomyPlugin.debug("03payment Transaction for " + target + " saved in " + (System.currentTimeMillis() - init) + " ms with id " + response.get(1) + " !");

                    return response;
                });
    }


    /**
     * Saves a transaction
     *
     * @param accountOwner The id of the account, could be a UUID or a bank id (string)
     * @param target       The id of the target account, could be a UUID or a bank id (string)
     * @param amount       The amount of money transferred
     * @param currency     The currency of the transaction
     * @param reason       The reason of the transaction
     * @return The transaction id
     */
    public CompletionStage<Long> saveTransaction(@NotNull AccountID accountOwner, @NotNull AccountID target, double amount, @NotNull Currency currency, @NotNull String reason) {
        if (!currency.shouldSaveTransactions()) return CompletableFuture.completedStage((long) -1);
        long init = System.currentTimeMillis();

        final String stackTrace = getCallerPluginString();

        return CompletableFuture.supplyAsync(() -> {
                    TransactionEvent transactionEvent = new TransactionEvent(new Transaction(
                            accountOwner,
                            target, currency.getCurrencyName(), System.currentTimeMillis(),
                            //If target is null, it has been sent from the server
                            amount, null, reason + stackTrace));
                    callEvents(transactionEvent);
                    return untilRedisAnswers("transaction of " + accountOwner, () -> (Long) plugin.getCurrenciesManager().getRedisManager().getConnectionSync(commands -> commands.eval(
                            "local a=redis.call('incr',KEYS[1])redis.call('hset',KEYS[2],a,ARGV[1])if tonumber(ARGV[2])>0 then redis.call('hexpire',KEYS[2],ARGV[2],'FIELDS',1,a)end;return a",
                            ScriptOutputType.INTEGER,
                            new String[]{
                                    RedisKeys.TRANSACTIONS_COUNTER.toString(),
                                    RedisKeys.TRANSACTIONS + accountOwner.toString() //Key rediseco:transactions:playerUUID
                            },
                            transactionEvent.getTransaction().toString(),
                            String.valueOf(currency.getTransactionsTTL()))));
                }, executorService).orTimeout(plugin.getConfigManager().getSettings().redis.timeout(), TimeUnit.MILLISECONDS)
                .exceptionally(exc -> {
                    //The save goes on until Redis is back. The caller only stops waiting for its id
                    if (!(exc instanceof TimeoutException)) exc.printStackTrace();
                    RedisEconomyPlugin.debug("ERROR!!!!! 03 Transaction Exception while saving transaction for " + accountOwner + " and " + target + ": " + exc.getMessage());
                    return (long) -1;
                })
                .thenApply(response -> {
                    RedisEconomyPlugin.debug("03 Transaction for " + accountOwner + " saved in " + (System.currentTimeMillis() - init) + " ms with id " + response + " !");
                    return response;
                });
    }

    /**
     * Revert a transaction creating a new transaction with the opposite amount
     *
     * @param accountOwner  The id of the account
     * @param transactionId The id of the transaction to revert
     * @return The id of the new transaction that reverts the old one or the id of the already existing transaction that reverts the old one
     */
    public CompletionStage<Long> revertTransaction(AccountID accountOwner, long transactionId) {
        return getTransaction(accountOwner, transactionId)
                .thenCompose(transaction -> {
                    // Early return for null transaction
                    if (transaction == null) {
                        return CompletableFuture.completedFuture((long) -1);
                    }

                    // Get currency - this should be fast since it's likely cached
                    Currency currency = plugin.getCurrenciesManager().getCurrencyByName(transaction.getCurrencyName());
                    if (currency == null) {
                        return CompletableFuture.completedFuture((long) -1);
                    }

                    // Check if already reverted
                    if (transaction.getRevertedWith() != null) {
                        RedisEconomyPlugin.debug("revert01b Transaction " + transactionId +
                                " already reverted with " + transaction.getRevertedWith());
                        return CompletableFuture.completedFuture(Long.valueOf(transaction.getRevertedWith()));
                    }

                    // Claim the transaction, so that a concurrent revert can't apply it twice
                    final String unrevertedTransaction = transaction.toString();
                    transaction.setRevertedWith(REVERT_IN_PROGRESS);
                    final String claimedTransaction = transaction.toString();
                    final Long claimed = plugin.getCurrenciesManager().getRedisManager().getConnectionSync(connection -> connection.eval(
                            "if redis.call('HGET',KEYS[1],ARGV[1])==ARGV[2] then redis.call('HSET',KEYS[1],ARGV[1],ARGV[3]) return 1 end return 0",
                            ScriptOutputType.INTEGER,
                            new String[]{RedisKeys.TRANSACTIONS + accountOwner.toString()},
                            String.valueOf(transactionId), unrevertedTransaction, claimedTransaction));
                    if (claimed == null || claimed != 1) {
                        RedisEconomyPlugin.debug("revert01b Transaction " + transactionId + " is being reverted by someone else");
                        return CompletableFuture.completedFuture((long) -1);
                    }

                    // Fire event asynchronously (non-blocking)
                    final TransactionEvent revertTransactionEvent = new TransactionEvent(transaction);
                    callEvents(revertTransactionEvent);

                    // Proceed with revert operation
                    return currency.revertTransaction(transactionId, revertTransactionEvent.getTransaction())
                            .thenCompose(newId -> {
                                if (newId == null) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                // Update the transaction with revert info
                                revertTransactionEvent.getTransaction().setRevertedWith(String.valueOf(newId));

                                // Update Redis asynchronously and return the newId
                                return plugin.getCurrenciesManager().getRedisManager()
                                        .getConnectionAsync(connection ->
                                                connection.hset(
                                                        RedisKeys.TRANSACTIONS + accountOwner.toString(),
                                                        String.valueOf(transactionId),
                                                        revertTransactionEvent.getTransaction().toString()
                                                )
                                        )
                                        .thenApply(updateResult -> {
                                            RedisEconomyPlugin.debug("revert02 Replace transaction " + transactionId +
                                                    " with a new revertedWith id on Redis: " + updateResult);
                                            return newId; // Return the new transaction ID, not the Redis update result
                                        })
                                        .exceptionally(throwable -> {
                                            RedisEconomyPlugin.debug("Failed to update transaction in Redis: " + throwable.getMessage());
                                            return newId; // Still return newId even if Redis update fails
                                        });
                            });
                });
    }

    /**
     * Archives all transactions to a file
     *
     * @param sender      Command sender, usually a player or console
     * @param archivePath Path to the archive file where transactions will be saved
     * @return A CompletionStage that completes with the number of archived accounts
     */
    public CompletionStage<Integer> archiveTransactions(CommandSender sender, Path archivePath) {
        // Create parent directories if they don't exist
        try {
            Files.createDirectories(archivePath.getParent());
        } catch (IOException e) {
            return CompletableFuture.failedFuture(new IOException("Failed to create archive directory", e));
        }

        return CompletableFuture.supplyAsync(() -> {
            int totalAccounts = plugin.getCurrenciesManager().getNameUniqueIds().size();
            int archivedCount = 0;
            //What reached the file, the only transactions that can be removed from Redis
            final Map<AccountID, String[]> archived = new HashMap<>();

            try (BufferedWriter writer = Files.newBufferedWriter(archivePath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                for (Map.Entry<String, UUID> entry : plugin.getCurrenciesManager().getNameUniqueIds().entrySet()) {

                    String username = entry.getKey();
                    UUID uuid = entry.getValue();

                    try {
                        AccountID accountID = new AccountID(uuid);
                        // Read directly: getTransactions turns a Redis error into an empty result
                        final Map<Long, Transaction> transactionsMap = readTransactions(accountID, Integer.MAX_VALUE);

                        if (transactionsMap.isEmpty()) {
                            continue;
                        }

                        // Write account header
                        writer.write(username);
                        writer.write(';');
                        writer.write(uuid.toString());
                        writer.newLine();

                        // Write transactions
                        for (Transaction transaction : transactionsMap.values()) {
                            writer.write(transaction.toString());
                            writer.newLine();
                        }

                        // Add separator between accounts
                        writer.newLine();
                        archived.put(accountID, transactionsMap.keySet().stream().map(String::valueOf).toArray(String[]::new));

                        // Report progress at regular intervals
                        archivedCount++;
                        if (archivedCount % 100 == 0 || archivedCount == totalAccounts) {
                            int progressPercentage = (archivedCount * 100) / totalAccounts;
                            plugin.getScheduler().runTask(() ->
                                    plugin.langs().send(sender, plugin.langs().transactionsArchiveProgress
                                            .replace("%progress%", String.valueOf(progressPercentage))));
                        }
                    } catch (Exception e) {
                        plugin.getLogger().warning("Failed to archive transactions for " + username + ": " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                plugin.getLogger().severe("Failed to write transaction archive: " + e.getMessage());
                return 0;
            }

            // Remove only what has been written and closed successfully: accounts without a known name
            // (banks too) and accounts that failed are not in the file, and new transactions may have arrived
            archived.forEach((accountID, ids) -> plugin.getCurrenciesManager().getRedisManager().getConnectionSync(connection ->
                    connection.hdel(RedisKeys.TRANSACTIONS + accountID.toString(), ids)));
            final int removedAccounts = archived.size();
            plugin.getScheduler().runTask(() ->
                    plugin.langs().send(sender, plugin.langs().transactionsArchiveCompleted
                            .replace("%size%", String.valueOf(removedAccounts))
                            .replace("%file%", archivePath.getFileName().toString())));
            return archivedCount;
        });
    }
    
    /**
     * Deserializes a string into an array of transactions
     *
     * @param serialized The serialized transactions
     * @return The deserialized transactions
     */
    private @NotNull Map<Integer, Transaction> getTransactionsFromSerialized(@Nullable Map<String, String> serialized) {
        if (serialized == null)
            return new HashMap<>();
        Map<Integer, Transaction> transactions = new HashMap<>();
        serialized.forEach((k, v) -> transactions.put(Integer.parseInt(k), Transaction.fromString(v)));
        return transactions;
    }

    public String getCallerPluginString() {
        if (plugin.settings().registerCallsVerbosity == 0) return "";
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .skip(3)
                .filter(s -> plugin.settings().callBlacklistRegex.stream().noneMatch(blRegex -> s.getClassName().matches(blRegex)))
                .findFirst()
                .map(ste -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append("\nCall: ").append(ste.getClassName());
                    if (plugin.settings().registerCallsVerbosity > 1) {
                        sb.append(":").append(ste.getMethodName());
                    }
                    if (plugin.settings().registerCallsVerbosity > 2) {
                        sb.append(":").append(ste.getLineNumber());
                    }
                    return sb.toString();
                })
                .orElse("");
    }

    public void terminate() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    /**
     * Saves a transaction record even if Redis is down for a while: the money has already moved.
     * Runs on the executor, so records stay queued in memory like the balance writes.
     * A reply lost after the commit can leave a duplicate record, never a wrong balance.
     */
    private <T> T untilRedisAnswers(String description, Supplier<T> save) {
        long waitMillis = 100;
        while (true) {
            try {
                return save.get();
            } catch (RedisCommandExecutionException e) {
                final String message = String.valueOf(e.getMessage());
                if (!message.startsWith("LOADING") && !message.startsWith("BUSY")) throw e; //Redis refused it: it won't change
                retryOrGiveUp(description, e, waitMillis);
            } catch (RedisException e) { //Timeouts and lost connections: a reset comes as a plain RedisException
                retryOrGiveUp(description, e, waitMillis);
            }
            waitMillis = Math.min(waitMillis * 2, 2000);
        }
    }

    private void retryOrGiveUp(String description, RedisException cause, long waitMillis) {
        try {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            Thread.sleep(waitMillis);
        } catch (InterruptedException e) { //shutdownNow from terminate()
            plugin.getLogger().severe("LOST " + description + ": Redis did not answer before the server stopped");
            Thread.currentThread().interrupt();
            throw cause;
        }
    }

    /**
     * Fire a transaction event on the main thread. The scheduler refuses tasks while the plugin is disabling
     */
    private void callEvents(TransactionEvent... events) {
        if (!plugin.isEnabled()) return;
        plugin.getScheduler().runTask(() -> {
            for (TransactionEvent event : events) {
                plugin.getServer().getPluginManager().callEvent(event);
            }
        });
    }

}
