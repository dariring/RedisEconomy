package dev.unnm3d.rediseconomy.currency;

import dev.unnm3d.rediseconomy.RedisEconomyPlugin;
import dev.unnm3d.rediseconomy.config.CurrencySettings;
import dev.unnm3d.rediseconomy.redis.RedisKeys;
import dev.unnm3d.rediseconomy.transaction.AccountID;
import dev.unnm3d.rediseconomy.transaction.Transaction;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.milkbowl.vault.economy.Economy;
import net.milkbowl.vault.economy.EconomyResponse;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


@AllArgsConstructor
public class Currency implements Economy {
    protected final CurrenciesManager currenciesManager;

    @Getter
    protected final String currencyName;
    private final ConcurrentHashMap<UUID, Double> accounts;
    private final ConcurrentHashMap<UUID, Double> maxPlayerBalances;

    private boolean enabled;
    @Getter
    private String currencySingular;
    @Getter
    private String currencyPlural;
    @Getter
    private final DecimalFormat decimalFormat;
    @Getter
    private final double startingBalance;
    @Getter
    private final double maxBalance;
    @Getter
    private boolean saveTransactions;
    @Getter
    private int transactionsTTL;
    @Getter
    private double transactionTax;
    @Getter
    private final boolean taxOnlyPay;
    protected final List<ExecutorService> updateExecutors;
    /**
     * Lock it to make a balance check and the following write one atomic step
     */
    protected final BalanceLedger<UUID> playerLedger;
    private final AtomicLong lastWriteId = new AtomicLong();
    private volatile boolean terminated = false;
    private static final long TERMINATION_TIMEOUT_MILLIS = 10_000;

    /**
     * Commits a balance write and publishes the result. Being a script, nothing can run in between.
     * KEYS: balances, name-uuids, sequence, last applied write of this executor
     * ARGV: account, D(elta) S(et) C(reate), value, balance if the account is not on Redis, write id, channel, message prefix, name
     * A retry after a timeout that actually committed is detected with the write id and not applied twice.
     */
    private static final String BALANCE_WRITE_SCRIPT = """
            if tonumber(redis.call('GET',KEYS[4]) or '0')>=tonumber(ARGV[5]) then
              return {redis.call('ZSCORE',KEYS[1],ARGV[1]) or '0',redis.call('GET',KEYS[3]) or '0'}
            end
            if ARGV[2]=='D' then
              redis.call('ZADD',KEYS[1],'NX',ARGV[4],ARGV[1])
              redis.call('ZINCRBY',KEYS[1],ARGV[3],ARGV[1])
            elseif ARGV[2]=='S' then
              redis.call('ZADD',KEYS[1],ARGV[3],ARGV[1])
            else
              redis.call('ZADD',KEYS[1],'NX',ARGV[3],ARGV[1])
            end
            local b=redis.call('ZSCORE',KEYS[1],ARGV[1])
            if ARGV[8]~='' then redis.call('HSET',KEYS[2],ARGV[8],ARGV[1]) end
            redis.call('INCR',KEYS[3])
            local s=redis.call('GET',KEYS[3])
            redis.call('PUBLISH',ARGV[6],ARGV[7]..b..';;'..s)
            redis.call('SET',KEYS[4],ARGV[5],'EX',86400)
            return {b,s}""";

    protected enum WriteMode {
        DELTA("D"), SET("S"), CREATE("C");

        private final String scriptArgument;

        WriteMode(String scriptArgument) {
            this.scriptArgument = scriptArgument;
        }
    }

    /**
     * A write to send to Redis
     *
     * @param member        the account in the balances sorted set
     * @param balancesKey   the balances sorted set
     * @param channel       the channel where to publish the result
     * @param messagePrefix the message up to the balance
     * @param playerName    the name to store for the account, empty for none
     * @param value         the delta or the balance to set, depending on the mode
     * @param seed          the balance to start from if Redis doesn't know the account
     */
    protected record BalanceWrite<K>(BalanceLedger<K> ledger, K key, String member, String balancesKey, String channel,
                                     String messagePrefix, String playerName, WriteMode mode, double value,
                                     double seed) {
    }


    /**
     * Creates a new currency.
     * Currency implements Economy from Vault, so it's the same as using any other Vault Economy plugin
     *
     * @param currenciesManager The CurrenciesManager instance
     * @param currencySettings  The currency settings
     */
    public Currency(CurrenciesManager currenciesManager, CurrencySettings currencySettings) {
        this.currenciesManager = currenciesManager;
        this.enabled = true;
        this.updateExecutors = generateExecutors(currencySettings.getExecutorThreads());
        this.currencyName = currencySettings.getCurrencyName();
        this.currencySingular = currencySettings.getCurrencySingle();
        this.currencyPlural = currencySettings.getCurrencyPlural();
        this.startingBalance = currencySettings.getStartingBalance();
        this.maxBalance = currencySettings.getMaxBalance() == 0.0d ? Double.POSITIVE_INFINITY : currencySettings.getMaxBalance();
        this.saveTransactions = currencySettings.isSaveTransactions();
        this.transactionsTTL = currencySettings.getTransactionsTTL();
        this.transactionTax = currencySettings.getPayTax();
        this.taxOnlyPay = currencySettings.isTaxOnlyPay();
        this.accounts = new ConcurrentHashMap<>();
        this.playerLedger = new BalanceLedger<>(accounts);
        this.maxPlayerBalances = new ConcurrentHashMap<>();
        this.decimalFormat = new DecimalFormat(
                currencySettings.getDecimalFormat() != null ? currencySettings.getDecimalFormat() : "#.##",
                new DecimalFormatSymbols(Locale.forLanguageTag(currencySettings.getLanguageTag() != null ? currencySettings.getLanguageTag() : "en-US"))
        );

        getOrderedAccounts(-1).thenApply(result -> {
            result.forEach(t ->
                    accounts.put(UUID.fromString(t.getValue()), t.getScore()));
            if (!accounts.isEmpty()) {
                RedisEconomyPlugin.debug("start1 Loaded " + accounts.size() + " accounts for currency " + currencyName);
            }
            return result;
        }).toCompletableFuture().join(); //Wait to avoid API calls before accounts are loaded

        getPlayerMaxBalances().thenApply(result -> {
            maxPlayerBalances.putAll(result);
            if (!maxPlayerBalances.isEmpty()) {
                RedisEconomyPlugin.debug("start1 Loaded " + maxPlayerBalances.size() + " max balances for currency " + currencyName);
            }
            return result;
        }); //Not as critical as accounts, so we don't wait


    }

    public void processUpdateMessage(String channel, String[] arguments) {
        final UUID uuid = UUID.fromString(arguments[0]);

        if (channel.equals(RedisKeys.UPDATE_PLAYER_CHANNEL_PREFIX.toString())) {
            String playerName = arguments[1].isEmpty() ? null : arguments[1];
            double balance = Double.parseDouble(arguments[2]);
            long sequence = arguments.length > 3 ? Long.parseLong(arguments[3]) : BalanceLedger.UNSEQUENCED;
            if (playerName != null)
                currenciesManager.updateNameUniqueId(playerName, uuid);
            playerLedger.update(uuid, balance, sequence);
            RedisEconomyPlugin.debug("01b Received balance update " + playerName + " to " + balance);

        } else if (channel.equals(RedisKeys.UPDATE_MAX_BAL_PREFIX.toString())) {
            double maxBal = Double.parseDouble(arguments[1]);
            setPlayerMaxBalanceLocal(uuid, maxBal);
            RedisEconomyPlugin.debug("01b Received max balance update " + uuid + " to " + maxBal);

        }
    }

    private List<ExecutorService> generateExecutors(int size) {
        if (size <= 0) return List.of(Executors.newSingleThreadExecutor(Thread.ofVirtual().factory()));
        List<ExecutorService> executors = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            executors.add(Executors.newSingleThreadExecutor(Thread.ofVirtual().factory()));
        }
        return executors;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    public boolean shouldSaveTransactions() {
        return saveTransactions;
    }

    public void setShouldSaveTransactions(boolean saveTransactions) {
        this.saveTransactions = saveTransactions;
    }

    @Override
    public String getName() {
        return "RedisEconomy";
    }

    @Override
    public boolean hasBankSupport() {
        return false;
    }

    @Override
    public int fractionalDigits() {
        return decimalFormat.getMaximumFractionDigits();
    }

    @Override
    public String format(double amount) {
        return decimalFormat.format(amount) + (amount == 1 ? currencySingular : currencyPlural);
    }

    @Override
    public String currencyNamePlural() {
        return currencyPlural;
    }

    @Override
    public String currencyNameSingular() {
        return currencySingular;
    }

    @Override
    public boolean hasAccount(@NotNull String playerName) {
        UUID playerUniqueId = currenciesManager.getUUIDFromUsernameCache(playerName);
        if (playerUniqueId == null) return false;
        return hasAccount(playerUniqueId);
    }

    @Override
    public boolean hasAccount(@NotNull OfflinePlayer player) {
        return hasAccount(player.getUniqueId());
    }

    @Override
    public boolean hasAccount(@NotNull String playerName, String worldName) {
        return hasAccount(playerName);
    }

    @Override
    public boolean hasAccount(@NotNull OfflinePlayer player, String worldName) {
        return hasAccount(player);
    }

    public boolean hasAccount(@NotNull UUID playerUUID) {
        return accounts.containsKey(playerUUID);
    }

    public double getBalance(@NotNull UUID playerUUID) {
        return accounts.getOrDefault(playerUUID, 0.0D);
    }

    @Override
    public double getBalance(@NotNull String playerName) {
        UUID playerUniqueId = currenciesManager.getUUIDFromUsernameCache(playerName);
        if (playerUniqueId == null) return 0.0D;
        return getBalance(playerUniqueId);
    }

    @Override
    public double getBalance(@NotNull OfflinePlayer player) {
        return getBalance(player.getUniqueId());
    }

    @Override
    public double getBalance(@NotNull String playerName, String world) {
        return getBalance(playerName);
    }

    @Override
    public double getBalance(@NotNull OfflinePlayer player, String world) {
        return getBalance(player);
    }

    @Override
    public boolean has(@NotNull String playerName, double amount) {
        return getBalance(playerName) >= amount;
    }

    @Override
    public boolean has(@NotNull OfflinePlayer player, double amount) {
        return has(player.getUniqueId(), amount);
    }

    public boolean has(@NotNull UUID playerUUID, double amount) {
        return getBalance(playerUUID) >= amount;
    }

    @Override
    public boolean has(@NotNull String playerName, String worldName, double amount) {
        return has(playerName, amount);
    }

    @Override
    public boolean has(@NotNull OfflinePlayer player, String worldName, double amount) {
        return has(player, amount);
    }

    @Override
    public EconomyResponse withdrawPlayer(@NotNull String playerName, double amount) {
        return withdrawPlayer(playerName, amount, "Withdraw");
    }

    @Override
    public EconomyResponse withdrawPlayer(@NotNull OfflinePlayer player, double amount) {
        return withdrawPlayer(player.getUniqueId(), player.getName(), amount, "Withdraw");
    }

    @Override
    public EconomyResponse withdrawPlayer(@NotNull String playerName, String worldName, double amount) {
        return withdrawPlayer(playerName, amount);
    }

    @Override
    public EconomyResponse withdrawPlayer(@NotNull OfflinePlayer player, String worldName, double amount) {
        return withdrawPlayer(player, amount);
    }

    @Override
    public EconomyResponse depositPlayer(@NotNull String playerName, double amount) {
        return depositPlayer(playerName, amount, "Deposit");
    }

    @Override
    public EconomyResponse depositPlayer(@NotNull OfflinePlayer player, double amount) {
        return depositPlayer(player.getUniqueId(), player.getName(), amount, "Deposit");
    }

    @Override
    public EconomyResponse depositPlayer(@NotNull String playerName, String worldName, double amount) {
        return depositPlayer(playerName, amount);
    }

    @Override
    public EconomyResponse depositPlayer(@NotNull OfflinePlayer player, String worldName, double amount) {
        return depositPlayer(player, amount);
    }

    @Override
    public EconomyResponse createBank(String name, String player) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse createBank(String name, OfflinePlayer player) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse deleteBank(String name) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse bankBalance(String name) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse bankHas(String name, double amount) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse bankWithdraw(String name, double amount) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse bankDeposit(String name, double amount) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse isBankOwner(String name, String playerName) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse isBankOwner(String name, OfflinePlayer player) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse isBankMember(String name, String playerName) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public EconomyResponse isBankMember(String name, OfflinePlayer player) {
        return new EconomyResponse(0, 0, EconomyResponse.ResponseType.NOT_IMPLEMENTED, "Not implemented");
    }

    @Override
    public List<String> getBanks() {
        return List.of();
    }

    @Override
    public boolean createPlayerAccount(@NotNull String playerName) {
        UUID playerUUID = currenciesManager.getUUIDFromUsernameCache(playerName);
        if (playerUUID == null)
            return false;
        return createPlayerAccount(playerUUID, playerName);
    }

    @Override
    public boolean createPlayerAccount(@NotNull OfflinePlayer player) {
        return createPlayerAccount(player.getUniqueId(), player.getName());
    }

    public boolean createPlayerAccount(@NotNull UUID playerUUID, @Nullable String playerName) {
        synchronized (playerLedger) {
            if (hasAccount(playerUUID))
                return false;
            writeAccount(playerUUID, playerName, WriteMode.CREATE, startingBalance);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(playerUUID), new AccountID(), startingBalance, this, "Account creation");
        return true;
    }

    @Override
    public boolean createPlayerAccount(@NotNull String playerName, @Nullable String worldName) {
        return createPlayerAccount(playerName);
    }

    @Override
    public boolean createPlayerAccount(@NotNull OfflinePlayer player, @Nullable String worldName) {
        return createPlayerAccount(player);
    }

    public EconomyResponse withdrawPlayer(@NotNull String playerName, double amount, @Nullable String reason) {
        UUID playerUniqueId = currenciesManager.getUUIDFromUsernameCache(playerName);
        if (playerUniqueId == null)
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Player not found");
        return withdrawPlayer(playerUniqueId, playerName, amount, reason);
    }


    public EconomyResponse withdrawPlayer(@NotNull UUID playerUUID, @Nullable String playerName, double amount, @Nullable String reason) {
        double amountToWithdraw = amount + (taxOnlyPay ? 0d : amount * transactionTax);
        if (!isValidAmount(amount) || !isValidAmount(amountToWithdraw))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");

        final double newBalance;
        synchronized (playerLedger) {
            if (!hasAccount(playerUUID))
                return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Account not found");

            if (!has(playerUUID, amountToWithdraw))
                return new EconomyResponse(amountToWithdraw, getBalance(playerUUID), EconomyResponse.ResponseType.FAILURE, "Insufficient funds");

            writeAccount(playerUUID, playerName, WriteMode.DELTA, -amountToWithdraw);
            newBalance = getBalance(playerUUID);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(playerUUID), new AccountID(), -amountToWithdraw, this, reason == null ? "Withdraw" : reason);
        return new EconomyResponse(amount, newBalance, EconomyResponse.ResponseType.SUCCESS, null);
    }

    @SuppressWarnings("unused")
    public EconomyResponse payPlayer(@NotNull UUID sender, @NotNull UUID receiver, double amount, @Nullable String reason) {
        String senderName = currenciesManager.getUsernameFromUUIDCache(sender);
        if (senderName == null)
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Account not found");
        //same for receiver
        String receiverName = currenciesManager.getUsernameFromUUIDCache(receiver);
        if (receiverName == null)
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Account not found");

        EconomyResponse response = transfer(sender, senderName, receiver, receiverName, amount);
        if (response.transactionSuccess()) {
            double amountToWithdraw = amount + (amount * transactionTax);
            currenciesManager.getExchange().saveTransaction(new AccountID(sender), new AccountID(receiver), -amountToWithdraw, this, reason == null ? "Payment" : reason);
            currenciesManager.getExchange().saveTransaction(new AccountID(receiver), new AccountID(sender), amount, this, reason == null ? "Payment" : reason);
        }
        return response;
    }

    public EconomyResponse payPlayer(@NotNull String senderName, @NotNull String receiverName, double amount) {
        final UUID sender = currenciesManager.getUUIDFromUsernameCache(senderName);
        final UUID receiver = currenciesManager.getUUIDFromUsernameCache(receiverName);
        if (sender == null || receiver == null)
            return new EconomyResponse(amount, getBalance(senderName), EconomyResponse.ResponseType.FAILURE, "Account not found");

        return transfer(sender, senderName, receiver, receiverName, amount);
    }

    /**
     * Moves the money from sender to receiver as one atomic step on this instance
     */
    private EconomyResponse transfer(@NotNull UUID sender, @NotNull String senderName, @NotNull UUID receiver, @NotNull String receiverName, double amount) {
        //Calculate the amount to withdraw with the transaction tax
        double amountToWithdraw = amount + (amount * transactionTax);
        if (!isValidAmount(amount) || !isValidAmount(amountToWithdraw))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");

        final double newBalance;
        synchronized (playerLedger) {
            if (!hasAccount(sender) || !hasAccount(receiver))
                return new EconomyResponse(amount, getBalance(sender), EconomyResponse.ResponseType.FAILURE, "Account not found");

            if (!has(sender, amountToWithdraw))
                return new EconomyResponse(0, getBalance(sender), EconomyResponse.ResponseType.FAILURE, "Insufficient funds");

            if (getBalance(receiver) + amount > getPlayerMaxBalance(receiver))
                return new EconomyResponse(0, getBalance(receiver), EconomyResponse.ResponseType.FAILURE, "The receiver has reached the maximum balance");

            writeAccount(sender, senderName, WriteMode.DELTA, -amountToWithdraw);
            writeAccount(receiver, receiverName, WriteMode.DELTA, amount);
            newBalance = getBalance(sender);
        }
        return new EconomyResponse(amount, newBalance, EconomyResponse.ResponseType.SUCCESS, null);
    }

    /**
     * Set the balance of a player
     *
     * @param player The player to set the balance of
     * @param amount The amount to set the balance to
     * @return The result of the operation
     */
    @SuppressWarnings("unused")
    public EconomyResponse setPlayerBalance(@NotNull OfflinePlayer player, double amount) {
        return setPlayerBalance(player.getUniqueId(), player.getName(), amount);
    }

    /**
     * Set the balance of a player
     *
     * @param playerUUID The player uuid to set the balance of
     * @param amount     The amount to set the balance to
     * @return The result of the operation
     */
    public EconomyResponse setPlayerBalance(@NotNull UUID playerUUID, @Nullable String playerName, double amount) {
        if (amount == Double.POSITIVE_INFINITY || amount == Double.NEGATIVE_INFINITY || Double.isNaN(amount))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");
        final double previousBalance;
        synchronized (playerLedger) {
            previousBalance = getBalance(playerUUID);
            writeAccount(playerUUID, playerName, WriteMode.SET, amount);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(playerUUID), new AccountID(), -previousBalance, this, "Reset balance");
        currenciesManager.getExchange().saveTransaction(new AccountID(playerUUID), new AccountID(), amount, this, "Set balance");
        return new EconomyResponse(amount, amount, EconomyResponse.ResponseType.SUCCESS, null);
    }

    /**
     * Revert a transaction
     *
     * @param transactionId The transaction id
     * @param transaction   The transaction to revert
     * @return The transaction id that reverted the initial transaction
     */
    public CompletionStage<Long> revertTransaction(long transactionId, @NotNull Transaction transaction) {
        String ownerName = transaction.getAccountIdentifier().isPlayer() ?//If the sender is a player
                currenciesManager.getUsernameFromUUIDCache(transaction.getAccountIdentifier().getUUID()) : //Get the username from the cache (with server uuid translation)
                transaction.getAccountIdentifier().toString(); //Else, it's a bank, so we get the bank id
        if (transaction.getAccountIdentifier().isPlayer()) {
            writeAccount(transaction.getAccountIdentifier().getUUID(), ownerName, WriteMode.DELTA, -transaction.getAmount());
        }
        RedisEconomyPlugin.debug("revert01a reverted on account " + transaction.getAccountIdentifier() + " amount " + transaction.getAmount());

        return currenciesManager.getExchange().saveTransaction(transaction.getAccountIdentifier(), transaction.getActor(), -transaction.getAmount(), this, "Revert #" + transactionId + ": " + transaction.getReason());
    }

    /**
     * Set the balance of a player
     *
     * @param playerName The player to set the balance of
     * @param amount     The amount to set the balance to
     * @return The result of the operation
     */
    public EconomyResponse setPlayerBalance(@NotNull String playerName, double amount) {
        UUID playerUUID = currenciesManager.getUUIDFromUsernameCache(playerName);
        if (playerUUID == null)
            return new EconomyResponse(amount, 0, EconomyResponse.ResponseType.FAILURE, "Player not found");
        return setPlayerBalance(playerUUID, playerName, amount);
    }

    public EconomyResponse depositPlayer(@NotNull String playerName, double amount, @Nullable String reason) {
        UUID playerUUID = currenciesManager.getUUIDFromUsernameCache(playerName);
        if (playerUUID == null)
            return new EconomyResponse(amount, 0, EconomyResponse.ResponseType.FAILURE, "Player not found");
        return depositPlayer(playerUUID, playerName, amount, reason);
    }

    public EconomyResponse depositPlayer(@NotNull UUID playerUUID, @Nullable String playerName, double amount, String reason) {
        if (!isValidAmount(amount))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");

        final double newBalance;
        synchronized (playerLedger) {
            if (!hasAccount(playerUUID))
                return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Account not found");

            if (getBalance(playerUUID) + amount > getPlayerMaxBalance(playerUUID))
                return new EconomyResponse(0, getBalance(playerUUID), EconomyResponse.ResponseType.FAILURE, "The player has reached the maximum balance");

            writeAccount(playerUUID, playerName, WriteMode.DELTA, amount);
            newBalance = getBalance(playerUUID);
        }
        currenciesManager.getExchange().saveTransaction(new AccountID(playerUUID), new AccountID(), amount, this, reason == null ? "Deposit" : reason);
        return new EconomyResponse(amount, newBalance, EconomyResponse.ResponseType.SUCCESS, null);
    }

    /**
     * Only update the balance on local memory
     *
     * @param uuid       The UUID of the player
     * @param playerName The name of the player, can be null if not known
     * @param balance    The new balance to set for the player
     */
    public void updateAccountLocal(@NotNull UUID uuid, @Nullable String playerName, double balance) {
        if (playerName != null)
            currenciesManager.updateNameUniqueId(playerName, uuid);
        playerLedger.update(uuid, balance, BalanceLedger.UNSEQUENCED);
    }

    /**
     * Reads the account from Redis into the local cache
     *
     * @param uuid       The UUID of the player
     * @param playerName The name of the player
     * @return The balance on Redis, null if Redis has no such account
     */
    public CompletionStage<Double> refreshAccountLocal(@NotNull UUID uuid, @Nullable String playerName) {
        return currenciesManager.getRedisManager().<List<Object>>getConnectionAsync(connection -> connection.eval(
                "return {redis.call('ZSCORE',KEYS[1],ARGV[1]) or '',redis.call('GET',KEYS[2]) or '0'}",
                ScriptOutputType.MULTI,
                new String[]{RedisKeys.BALANCE_PREFIX + currencyName, RedisKeys.BALANCE_SEQUENCE_PREFIX + currencyName},
                uuid.toString())
        ).thenApply(reply -> {
            final String balance = String.valueOf(reply.get(0));
            if (balance.isEmpty()) return null;
            if (playerName != null)
                currenciesManager.updateNameUniqueId(playerName, uuid);
            playerLedger.update(uuid, Double.parseDouble(balance), Long.parseLong(String.valueOf(reply.get(1))));
            return Double.parseDouble(balance);
        });
    }

    /**
     * Amounts passed to balance operations must be finite and not negative: a negative withdraw is a deposit
     */
    protected static boolean isValidAmount(double amount) {
        return Double.isFinite(amount) && amount >= 0;
    }

    /**
     * Applies a write on the local cache and queues it for Redis.
     * Calling it while holding the ledger keeps the queue in the same order as the local writes
     */
    private void writeAccount(@NotNull UUID uuid, @Nullable String playerName, @NotNull WriteMode mode, double value) {
        final double seed;
        synchronized (playerLedger) {
            seed = getBalance(uuid);
            if (playerName != null)
                currenciesManager.updateNameUniqueId(playerName, uuid);
            if (mode == WriteMode.DELTA) playerLedger.applyDelta(uuid, value);
            else playerLedger.applySet(uuid, value);

            final String name = playerName == null ? "" : playerName;
            enqueueBalanceWrite(new BalanceWrite<>(playerLedger, uuid, uuid.toString(),
                    RedisKeys.BALANCE_PREFIX + currencyName,
                    RedisKeys.UPDATE_PLAYER_CHANNEL_PREFIX + currencyName,
                    RedisEconomyPlugin.getInstanceUUID() + ";;" + uuid + ";;" + name + ";;",
                    name, mode, value, seed
            ), (int) uuid.getMostSignificantBits());
        }
    }

    /**
     * Queues a write for Redis. Every account always uses the same executor, to keep its writes in order
     *
     * @param write      The write
     * @param identifier The identifier of the account
     */
    protected <K> void enqueueBalanceWrite(@NotNull BalanceWrite<K> write, int identifier) {
        final int executorIndex = Math.floorMod(identifier, updateExecutors.size());
        final long writeId = lastWriteId.incrementAndGet();
        runOnExecutor(executorIndex, () -> writeUntilCommitted(write, writeId, executorIndex));
    }

    /**
     * Runs the task after the writes already queued for the executor
     */
    protected void runOnExecutor(int executorIndex, @NotNull Runnable task) {
        try {
            updateExecutors.get(executorIndex).execute(task);
        } catch (RejectedExecutionException e) {
            task.run(); //The currency has been replaced or the plugin is disabling: don't lose the write
        }
    }

    /**
     * Sends the write until Redis commits it, because the caller already got a success response.
     * It stops only when the executors are terminated: later writes on this executor wait for this one to stay in order
     */
    private <K> void writeUntilCommitted(@NotNull BalanceWrite<K> write, long writeId, int executorIndex) {
        final RedisEconomyPlugin plugin = RedisEconomyPlugin.getInstance();
        final boolean absolute = write.mode() != WriteMode.DELTA;
        final String[] keys = {
                write.balancesKey(),
                RedisKeys.NAME_UUID.toString(),
                RedisKeys.BALANCE_SEQUENCE_PREFIX + currencyName,
                RedisKeys.APPLIED_WRITE_PREFIX + currencyName + ":" + RedisEconomyPlugin.getInstanceUUID() + ":" + executorIndex
        };
        int tries = 0;
        while (true) {
            RedisEconomyPlugin.debugCache("01a Writing " + write.member() + " " + write.mode() + " " + write.value() + " currency " + currencyName);
            try {
                final List<Object> reply = currenciesManager.getRedisManager().getConnectionSync(connection -> connection.eval(
                        BALANCE_WRITE_SCRIPT, ScriptOutputType.MULTI, keys,
                        write.member(), write.mode().scriptArgument, String.valueOf(write.value()), String.valueOf(write.seed()),
                        String.valueOf(writeId), write.channel(), write.messagePrefix(), write.playerName()));
                write.ledger().acknowledge(write.key(), absolute, write.value(),
                        Double.parseDouble(String.valueOf(reply.get(0))), Long.parseLong(String.valueOf(reply.get(1))));
                RedisEconomyPlugin.debugCache("01c Committed " + write.member() + " to " + reply.get(0) + " currency " + currencyName);
                return;
            } catch (RuntimeException e) {
                tries++;
                RedisEconomyPlugin.debugCache("WARN! Failed to write account " + write.member() + " try: " + tries + " " + e);
                if (tries == plugin.settings().redis.getTryAgainCount()) {
                    plugin.getLogger().severe("Can't write " + write.member() + " on Redis after " + tries + " tries, still trying: " + e);
                    currenciesManager.getRedisManager().printPool();
                }
                if (terminated && tries >= plugin.settings().redis.getTryAgainCount() || Thread.currentThread().isInterrupted()) {
                    abandonBalanceWrite(write, absolute);
                    return;
                }
                try {
                    Thread.sleep(Math.min(tries * 100L, 2000L));
                } catch (InterruptedException interrupted) {
                    abandonBalanceWrite(write, absolute);
                    return;
                }
            }
        }
    }

    private <K> void abandonBalanceWrite(@NotNull BalanceWrite<K> write, boolean absolute) {
        write.ledger().abandon(write.key(), absolute, write.value());
        RedisEconomyPlugin.getInstance().getLogger().severe("LOST balance write, fix it manually: account " + write.member()
                + " " + write.mode() + " " + write.value() + " currency " + currencyName);
    }


    /**
     * Update the balances of all players and their nameuuids
     * Do not use this method unless you know what you are doing
     *
     * @param balances  The balances to update
     * @param nameUUIDs The name-uuids to update
     */
    @SuppressWarnings("unchecked")
    public void updateBulkAccountsCloudCache(@NotNull List<ScoredValue<String>> balances, @NotNull Map<String, String> nameUUIDs) {
        currenciesManager.getRedisManager().executeTransaction(commands -> {
            ScoredValue<String>[] balancesArray = new ScoredValue[balances.size()];
            balances.toArray(balancesArray);

            commands.zadd(RedisKeys.BALANCE_PREFIX + currencyName, balancesArray);
            if (!nameUUIDs.isEmpty())
                commands.hset(RedisKeys.NAME_UUID.toString(), nameUUIDs);
        }).ifPresent(result -> {
            Bukkit.getLogger().info("migration01 updated balances into " + RedisKeys.BALANCE_PREFIX + currencyName + " accounts. result " + result.get(0));
            if (!nameUUIDs.isEmpty())
                Bukkit.getLogger().info("migration02 updated nameuuids into " + RedisKeys.NAME_UUID + " accounts. result " + result.get(1));

        });
    }

    /**
     * Get ordered accounts from Redis
     * Redis uses ordered sets as data structures
     * This method has a time complexity of O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
     *
     * @param limit The number of accounts to return from the top 1
     * @return A list of accounts ordered by balance in Tuples of UUID and balance (UUID is stringified)
     */
    public CompletionStage<List<ScoredValue<String>>> getOrderedAccounts(int limit) {
        return currenciesManager.getRedisManager().getConnectionAsync(accounts ->
                accounts.zrevrangeWithScores(RedisKeys.BALANCE_PREFIX + currencyName, 0, limit));

    }

    public double getPlayerMaxBalance(UUID uuid) {
        return maxPlayerBalances.getOrDefault(uuid, maxBalance);
    }

    public void setPlayerMaxBalance(UUID uuid, double maxAmount) {
        if (maxAmount < getBalance(uuid)) {
            setPlayerBalance(uuid, null, maxAmount);
        }
        setPlayerMaxBalanceCloud(uuid, maxAmount);
        setPlayerMaxBalanceLocal(uuid, maxAmount);
    }

    private void setPlayerMaxBalanceLocal(UUID uuid, double amount) {
        maxPlayerBalances.put(uuid, amount);
    }

    private void setPlayerMaxBalanceCloud(UUID uuid, double amount) {
        currenciesManager.getRedisManager().getConnectionPipeline(asyncCommands -> {
            if (amount == maxBalance) {
                asyncCommands.hdel(RedisKeys.MAX_PLAYER_BALANCES + currencyName, uuid.toString());
            } else {
                asyncCommands.hset(RedisKeys.MAX_PLAYER_BALANCES + currencyName, uuid.toString(), String.valueOf(amount));
            }
            return asyncCommands.publish(RedisKeys.UPDATE_MAX_BAL_PREFIX + currencyName, RedisEconomyPlugin.getInstanceUUID().toString() + ";;" + uuid + ";;" + amount);
        });
    }

    public CompletionStage<Map<UUID, Double>> getPlayerMaxBalances() {
        return currenciesManager.getRedisManager().getConnectionAsync(accounts ->
                        accounts.hgetall(RedisKeys.MAX_PLAYER_BALANCES + currencyName))
                .thenApply(result -> {
                    final Map<UUID, Double> maxBalances = new HashMap<>();
                    result.forEach((key, value) -> maxBalances.put(UUID.fromString(key), Double.parseDouble(value)));
                    return maxBalances;
                });
    }

    /**
     * Get single ordered account from Redis
     *
     * @param uuid The UUID of the account
     * @return The balance associated with the UUID on Redis
     */
    public CompletionStage<Double> getAccountRedis(UUID uuid) {
        return currenciesManager.getRedisManager().getConnectionAsync(connection -> connection.zscore(RedisKeys.BALANCE_PREFIX + currencyName, uuid.toString()));
    }

    /**
     * Get all accounts in cache
     *
     * @return Unmodifiable map of accounts
     */
    @SuppressWarnings("unused")
    public final Map<UUID, Double> getAccounts() {
        return Collections.unmodifiableMap(accounts);
    }

    /**
     * Terminate all executors, after the queued balance writes have been sent to Redis
     */
    public void terminateExecutors() {
        updateExecutors.forEach(ExecutorService::shutdown);
        final long deadline = System.currentTimeMillis() + TERMINATION_TIMEOUT_MILLIS;
        try {
            for (ExecutorService executor : updateExecutors) {
                executor.awaitTermination(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        terminated = true;
        int notStarted = 0;
        for (ExecutorService executor : updateExecutors) {
            notStarted += executor.shutdownNow().size();
        }
        if (notStarted > 0) {
            RedisEconomyPlugin.getInstance().getLogger().severe("LOST " + notStarted + " balance writes of currency " + currencyName + ": Redis did not answer before the timeout");
        }
    }

}
