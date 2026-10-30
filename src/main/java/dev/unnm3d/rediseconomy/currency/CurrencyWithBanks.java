package dev.unnm3d.rediseconomy.currency;

import dev.unnm3d.rediseconomy.RedisEconomyPlugin;
import dev.unnm3d.rediseconomy.config.CurrencySettings;
import dev.unnm3d.rediseconomy.redis.RedisKeys;
import dev.unnm3d.rediseconomy.transaction.AccountID;
import dev.unnm3d.rediseconomy.transaction.Transaction;
import io.lettuce.core.ScoredValue;
import net.milkbowl.vault.economy.EconomyResponse;
import org.bukkit.OfflinePlayer;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import static dev.unnm3d.rediseconomy.redis.RedisKeys.*;

public class CurrencyWithBanks extends Currency {
    private static final String BANK_DELETED_MESSAGE = "DELETED";

    private final ConcurrentHashMap<String, Double> bankAccounts;
    /**
     * Lock it to make a bank balance check and the following write one atomic step
     */
    private final BalanceLedger<String> bankLedger;
    /**
     * Account ID -> Owner UUID
     */
    private final ConcurrentHashMap<String, UUID> bankOwners;

    public CurrencyWithBanks(CurrenciesManager currenciesManager, CurrencySettings currencySettings) {
        super(currenciesManager, currencySettings);
        bankAccounts = new ConcurrentHashMap<>();
        bankLedger = new BalanceLedger<>(bankAccounts);
        bankOwners = new ConcurrentHashMap<>();
        getOrderedBankAccounts().thenApply(list -> {
            for (ScoredValue<String> scoredValue : list) {
                bankAccounts.put(scoredValue.getValue(), scoredValue.getScore());
            }
            if (!bankAccounts.isEmpty()) {
                RedisEconomyPlugin.debug("start1bank Loaded " + bankAccounts.size() + " accounts for currency " + currencyName);
            }
            return list;
        }).toCompletableFuture().join();
        getRedisBankOwners().thenApply(map -> {
            map.forEach((k, v) -> bankOwners.put(k, UUID.fromString(v)));
            if (!bankOwners.isEmpty()) {
                RedisEconomyPlugin.debug("start1bankb Loaded " + bankOwners.size() + " accounts owners for currency " + currencyName);
            }
            return map;
        }).toCompletableFuture().join();

    }

    @Override
    public void processUpdateMessage(String channel, String[] arguments) {
        if (channel.equals(RedisKeys.UPDATE_BANK_OWNER_CHANNEL_PREFIX.toString())) {
            String accountId = arguments[0];
            UUID owner = UUID.fromString(arguments[1]);
            bankOwners.put(accountId, owner);
            RedisEconomyPlugin.debug("01d Received bank owner update " + accountId + " to " + owner);
        } else if (channel.equals(RedisKeys.UPDATE_BANK_CHANNEL_PREFIX.toString())) {
            String accountId = arguments[0];
            if (arguments[1].equals(BANK_DELETED_MESSAGE)) {
                bankLedger.remove(accountId);
                bankOwners.remove(accountId);
                RedisEconomyPlugin.debug("01c Received bank deletion " + accountId);
                return;
            }
            double balance = Double.parseDouble(arguments[1]);
            bankLedger.update(accountId, balance, arguments.length > 2 ? Long.parseLong(arguments[2]) : BalanceLedger.UNSEQUENCED);
            RedisEconomyPlugin.debug("01c Received bank balance update " + accountId + " to " + balance);
        } else super.processUpdateMessage(channel, arguments);
    }

    @Override
    public boolean hasBankSupport() {
        return true;
    }

    public EconomyResponse createBank(@NotNull String accountId) {
        return createBank(accountId, RedisKeys.getServerUUID(), "Bank account creation");
    }

    @Override
    public EconomyResponse createBank(@NotNull String accountId, String player) {
        return createBank(accountId, currenciesManager.getUUIDFromUsernameCache(player), "Bank account creation");
    }

    /**
     * Creates a bank account
     *
     * @param accountId   Account ID
     * @param playerOwner Owner UUID
     * @param reason      Reason for the creation
     * @return EconomyResponse with the result of the operation (SUCCESS)
     */
    public EconomyResponse createBank(@NotNull String accountId, UUID playerOwner, String reason) {
        if (playerOwner == null)
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Player not found");
        synchronized (bankLedger) {
            if (bankAccounts.containsKey(accountId))
                return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Bank account already exists");
            setOwner(accountId, playerOwner);
            writeBankAccount(accountId, WriteMode.CREATE, 0);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(accountId), new AccountID(playerOwner), 0, this, reason);
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.SUCCESS, null);
    }

    @Override
    public EconomyResponse createBank(@NotNull String accountId, OfflinePlayer player) {
        return createBank(accountId, player.getUniqueId(), "Bank account creation");
    }

    public boolean existsBank(@NotNull String accountId) {
        return bankAccounts.containsKey(accountId);
    }

    @Override
    public EconomyResponse deleteBank(@NotNull String accountId) {
        synchronized (bankLedger) {
            bankLedger.remove(accountId);
            bankOwners.remove(accountId);
            //After the writes already queued for this account, or they would create it again
            runOnExecutor(Math.floorMod(accountId.hashCode(), updateExecutors.size()), () -> {
                final long result = currenciesManager.getRedisManager().getConnectionSync(connection -> {
                    connection.zrem(BALANCE_BANK_PREFIX + currencyName, accountId);
                    connection.publish(UPDATE_BANK_CHANNEL_PREFIX + currencyName,
                            RedisEconomyPlugin.getInstanceUUID().toString() + ";;" + accountId + ";;" + BANK_DELETED_MESSAGE);
                    return connection.hdel(BANK_OWNERS.toString(), accountId);
                });
                RedisEconomyPlugin.debug("Deleted bank account " + accountId + " with result " + result);

                currenciesManager.getExchange().saveTransaction(new AccountID(accountId), new AccountID(), 0, this, "Bank account deletion");
            });
        }
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.SUCCESS, null);
    }

    @Override
    public EconomyResponse bankBalance(@NotNull String accountId) {
        return new EconomyResponse(0, bankAccounts.getOrDefault(accountId, 0.0D), EconomyResponse.ResponseType.SUCCESS, null);
    }

    @Override
    public EconomyResponse bankHas(@NotNull String accountId, double amount) {
        if (!existsBank(accountId)) {
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Bank account does not exist");
        }
        double bankBalance = bankAccounts.getOrDefault(accountId, 0.0D);
        if (bankBalance - amount < 0) {
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Not enough money");
        }
        return new EconomyResponse(amount, bankBalance - amount, EconomyResponse.ResponseType.SUCCESS, null);
    }

    @Override
    public EconomyResponse bankWithdraw(@NotNull String accountId, double amount) {
        return bankWithdraw(accountId, amount, "Bank withdraw");
    }

    /**
     * Withdraws money from a bank account with a reason
     *
     * @param accountId Account ID
     * @param amount    Amount to withdraw
     * @param reason    Reason for the withdraw
     * @return EconomyResponse with the result of the operation SUCCESS or FAILURE if there is not enough money
     */
    public EconomyResponse bankWithdraw(@NotNull String accountId, double amount, String reason) {
        if (!isValidAmount(amount))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");
        final EconomyResponse hasAmountResponse;
        synchronized (bankLedger) {
            hasAmountResponse = bankHas(accountId, amount);
            if (hasAmountResponse.type.equals(EconomyResponse.ResponseType.FAILURE)) {
                return hasAmountResponse;
            }
            writeBankAccount(accountId, WriteMode.DELTA, -amount);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(accountId), new AccountID(), -amount, this, reason);
        return new EconomyResponse(amount, hasAmountResponse.balance, EconomyResponse.ResponseType.SUCCESS, null); //Balance is the new balance with subtracted amount

    }

    @Override
    public EconomyResponse bankDeposit(@NotNull String accountId, double amount) {
        return bankDeposit(accountId, amount, "Bank deposit");
    }

    /**
     * Deposits money to a bank account with a reason
     *
     * @param accountId Account ID
     * @param amount    Amount to deposit
     * @param reason    Reason for the deposit
     * @return EconomyResponse with the result of the operation SUCCESS or FAILURE if the bank account does not exist
     */
    public EconomyResponse bankDeposit(@NotNull String accountId, double amount, String reason) {
        if (!isValidAmount(amount))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");
        final double newBalance;
        synchronized (bankLedger) {
            if (!existsBank(accountId)) {
                return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Bank account does not exist");
            }
            newBalance = bankAccounts.getOrDefault(accountId, 0.0D) + amount;
            writeBankAccount(accountId, WriteMode.DELTA, amount);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(accountId), new AccountID(), amount, this, reason);
        return new EconomyResponse(amount, newBalance, EconomyResponse.ResponseType.SUCCESS, null);
    }

    /**
     * Set the balance of a bank account.
     *
     * @param accountId The account ID of the bank
     * @param amount    The amount to set the balance to
     * @return The result of the operation
     */
    public EconomyResponse setBankBalance(@NotNull String accountId, double amount) {
        if (amount == Double.POSITIVE_INFINITY || amount == Double.NEGATIVE_INFINITY || Double.isNaN(amount))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");
        final double previousBalance;
        synchronized (bankLedger) {
            previousBalance = bankAccounts.getOrDefault(accountId, 0.0D);
            writeBankAccount(accountId, WriteMode.SET, amount);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(accountId), new AccountID(), -previousBalance, this, "Reset balance");
        currenciesManager.getExchange().saveTransaction(new AccountID(accountId), new AccountID(), amount, this, "Set balance");
        return new EconomyResponse(amount, amount, EconomyResponse.ResponseType.SUCCESS, null);
    }

    @Override
    public EconomyResponse isBankOwner(@NotNull String accountId, String playerName) {
        final UUID owner = bankOwners.get(accountId);
        if (owner != null && owner.equals(currenciesManager.getUUIDFromUsernameCache(playerName))) {
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.SUCCESS, null);
        }
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Not owner");
    }

    @Override
    public EconomyResponse isBankOwner(@NotNull String accountId, OfflinePlayer player) {
        final UUID owner = bankOwners.get(accountId);
        if (owner != null && owner.equals(player.getUniqueId())) {
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.SUCCESS, null);
        }
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Not owner");
    }

    @Override
    public EconomyResponse isBankMember(@NotNull String accountId, String playerName) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse isBankMember(@NotNull String accountId, OfflinePlayer player) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public List<String> getBanks() {
        return bankAccounts.keySet().stream().toList();
    }

    /**
     * Revert a transaction
     *
     * @param transactionId The transaction id
     * @param transaction   The transaction to revert
     * @return The transaction id that reverted the initial transaction
     */
    @Override
    public CompletionStage<Long> revertTransaction(long transactionId, @NotNull Transaction transaction) {
        if (transaction.getAccountIdentifier().isPlayer()) {//Update player account
            return super.revertTransaction(transactionId, transaction);
        }
        //Update bank account
        writeBankAccount(transaction.getAccountIdentifier().toString(), WriteMode.DELTA, -transaction.getAmount());
        RedisEconomyPlugin.debug("revert01a reverted on account " + transaction.getAccountIdentifier() + " amount " + transaction.getAmount());

        return currenciesManager.getExchange().saveTransaction(transaction.getAccountIdentifier(), transaction.getActor(), -transaction.getAmount(), this, "Revert #" + transactionId + ": " + transaction.getReason());
    }

    private void setOwner(@NotNull String accountId, UUID ownerUUID) {
        bankOwners.put(accountId, ownerUUID);
        currenciesManager.getRedisManager().getConnectionPipeline(connection -> {
            connection.hset(BANK_OWNERS.toString(), accountId, ownerUUID.toString());
            return connection.publish(UPDATE_BANK_OWNER_CHANNEL_PREFIX + currencyName, RedisEconomyPlugin.getInstanceUUID().toString() + ";;" + accountId + ";;" + ownerUUID);
        }).thenAccept((result) -> {
            RedisEconomyPlugin.debug("Set owner of bank " + accountId + " to " + ownerUUID + " with result " + result);
        });
    }

    /**
     * Applies a write on the local cache and queues it for Redis
     */
    private void writeBankAccount(@NotNull String accountId, @NotNull WriteMode mode, double value) {
        synchronized (bankLedger) {
            final double seed = bankAccounts.getOrDefault(accountId, 0.0D);
            if (mode == WriteMode.DELTA) bankLedger.applyDelta(accountId, value);
            else bankLedger.applySet(accountId, value);

            enqueueBalanceWrite(new BalanceWrite<>(bankLedger, accountId, accountId,
                    BALANCE_BANK_PREFIX + currencyName,
                    UPDATE_BANK_CHANNEL_PREFIX + currencyName,
                    RedisEconomyPlugin.getInstanceUUID() + ";;" + accountId + ";;",
                    "", mode, value, seed
            ), accountId.hashCode());
        }
    }

    /**
     * Get ordered accounts from Redis
     * Redis uses ordered sets as data structures
     * This method has a time complexity of O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
     *
     * @return A list of accounts ordered by balance in Tuples of UUID and balance (UUID is stringified)
     */
    public CompletionStage<List<ScoredValue<String>>> getOrderedBankAccounts() {
        return currenciesManager.getRedisManager().getConnectionAsync(connection ->
                connection.zrevrangeWithScores(BALANCE_BANK_PREFIX + currencyName, 0, -1));
    }

    /**
     * Get bank owners from Redis. Use with caution, this method is not cached
     *
     * @return A map of bank owners
     */
    public CompletionStage<Map<String, String>> getRedisBankOwners() {
        return currenciesManager.getRedisManager().getConnectionAsync(connection ->
                connection.hgetall(BANK_OWNERS.toString()));
    }


}
