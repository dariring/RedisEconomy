package dev.unnm3d.rediseconomy.currency;

import dev.unnm3d.rediseconomy.RedisEconomyPlugin;
import dev.unnm3d.rediseconomy.config.CurrencySettings;
import dev.unnm3d.rediseconomy.redis.RedisKeys;
import dev.unnm3d.rediseconomy.redis.RedisManager;
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
    /**
     * Shared by all currencies and never reset: a reload replaces the Currency objects but keeps the instance id,
     * and Redis ignores a write id lower than the last one it applied for that instance and executor.
     */
    private static final AtomicLong LAST_WRITE_ID = new AtomicLong();
    /**
     * Stands for the executor index of writes that arrive after the executors were shut down
     */
    private static final int LATE_WRITES_INDEX = -1;
    private volatile boolean terminated = false;
    private static final long TERMINATION_TIMEOUT_MILLIS = 10_000;

    /**
     * Commits a balance write and publishes the result. Being a script, nothing can run in between.
     * KEYS: balances, name-uuids, sequence, last applied write of this executor, accounts put back after a data loss, data epoch
     * The update carries the data epoch: a server that hasn't noticed a data loss must not let it replace what it knows.
     * ARGV: account, D(elta) S(et) C(reate), value, balance if the account is not on Redis, write id, channel, message prefix, name,
     * sequence of that balance. If it is used, or the account was put back after a data loss, the deltas are noted there:
     * a server that knows a more recent balance may still replace it, and they go on top of it.
     * A retry after a timeout that actually committed is detected with the write id and not applied twice.
     * The sequence starts from the clock: if Redis loses the key (restart without persistence, eviction) it must
     * still be higher than what instances have seen, or they would ignore every later balance as old.
     */
    private static final String BALANCE_WRITE_SCRIPT = """
            if tonumber(redis.call('GET',KEYS[4]) or '0')>=tonumber(ARGV[5]) then
              return {redis.call('ZSCORE',KEYS[1],ARGV[1]) or '0',redis.call('GET',KEYS[3]) or '0'}
            end
            if redis.call('EXISTS',KEYS[3])==0 then
              redis.call('SET',KEYS[3],redis.call('TIME')[1]..'000')
            end
            if ARGV[2]=='D' then
              if redis.call('ZADD',KEYS[1],'NX',ARGV[4],ARGV[1])==1 then
                redis.call('HSETNX',KEYS[5],':floor',redis.call('GET',KEYS[3]))
                redis.call('HSET',KEYS[5],ARGV[1],ARGV[9])
                redis.call('HDEL',KEYS[5],ARGV[1]..':d')
                redis.call('EXPIRE',KEYS[5],3600)
              end
              redis.call('ZINCRBY',KEYS[1],ARGV[3],ARGV[1])
              if redis.call('HEXISTS',KEYS[5],ARGV[1])==1 then redis.call('HINCRBYFLOAT',KEYS[5],ARGV[1]..':d',ARGV[3]) end
            elseif ARGV[2]=='S' then
              redis.call('ZADD',KEYS[1],ARGV[3],ARGV[1])
              redis.call('HDEL',KEYS[5],ARGV[1],ARGV[1]..':d')
            else
              redis.call('ZADD',KEYS[1],'NX',ARGV[3],ARGV[1])
            end
            local b=redis.call('ZSCORE',KEYS[1],ARGV[1])
            if ARGV[8]~='' then redis.call('HSET',KEYS[2],ARGV[8],ARGV[1]) end
            redis.call('INCR',KEYS[3])
            local s=redis.call('GET',KEYS[3])
            redis.call('PUBLISH',ARGV[6],ARGV[7]..b..';;'..s..';;'..(redis.call('GET',KEYS[6]) or '-'))
            redis.call('SET',KEYS[4],ARGV[5],'EX',86400)
            return {b,s}""";

    /**
     * Commits a withdraw only if Redis holds the funds: another server may have spent them a moment ago.
     * A payment passes the receiver too and moves the money as one step, so a crash can't take it from the sender only.
     * KEYS: same as the balance write script
     * ARGV: account, F(orce, skip the funds check) or empty, amount, balance if the account is not on Redis, write id,
     * channel, message prefix, name, then for a payment: receiver, amount, balance if not on Redis, message prefix, name,
     * then the sequences of the two balances to use if an account is not on Redis
     * Returns O(k), I(nsufficient funds) or F(enced, see the next script), the balance, the sequence, the receiver balance
     */
    private static final String DEBIT_SCRIPT = """
            local function receiver() if ARGV[9]=='' then return '0' end return redis.call('ZSCORE',KEYS[1],ARGV[9]) or '0' end
            if tonumber(redis.call('GET',KEYS[4]) or '0')>=tonumber(ARGV[5]) then
              return {'F',redis.call('ZSCORE',KEYS[1],ARGV[1]) or '0',redis.call('GET',KEYS[3]) or '0',receiver()}
            end
            if redis.call('EXISTS',KEYS[3])==0 then
              redis.call('SET',KEYS[3],redis.call('TIME')[1]..'000')
            end
            local function seed(member,balance,sequence)
              if redis.call('ZADD',KEYS[1],'NX',balance,member)==1 then
                redis.call('HSETNX',KEYS[5],':floor',redis.call('GET',KEYS[3]))
                redis.call('HSET',KEYS[5],member,sequence)
                redis.call('HDEL',KEYS[5],member..':d')
                redis.call('EXPIRE',KEYS[5],3600)
              end
            end
            local function move(member,delta)
              local v=redis.call('ZINCRBY',KEYS[1],delta,member)
              if redis.call('HEXISTS',KEYS[5],member)==1 then redis.call('HINCRBYFLOAT',KEYS[5],member..':d',delta) end
              return v
            end
            seed(ARGV[1],ARGV[4],ARGV[14])
            if ARGV[2]~='F' and tonumber(redis.call('ZSCORE',KEYS[1],ARGV[1]))<tonumber(ARGV[3]) then
              return {'I',redis.call('ZSCORE',KEYS[1],ARGV[1]),redis.call('GET',KEYS[3]) or '0',receiver()}
            end
            local b=move(ARGV[1],'-'..ARGV[3])
            if ARGV[8]~='' then redis.call('HSET',KEYS[2],ARGV[8],ARGV[1]) end
            local r='0'
            if ARGV[9]~='' then
              seed(ARGV[9],ARGV[11],ARGV[15])
              r=move(ARGV[9],ARGV[10])
              if ARGV[13]~='' then redis.call('HSET',KEYS[2],ARGV[13],ARGV[9]) end
            end
            redis.call('INCR',KEYS[3])
            local s=redis.call('GET',KEYS[3])
            local e=redis.call('GET',KEYS[6]) or '-'
            redis.call('PUBLISH',ARGV[6],ARGV[7]..b..';;'..s..';;'..e)
            if ARGV[9]~='' then redis.call('PUBLISH',ARGV[6],ARGV[12]..r..';;'..s..';;'..e) end
            redis.call('SET',KEYS[4],ARGV[5],'EX',86400)
            return {'O',b,s,r}""";
    /**
     * Settles a debit whose reply never came: returns 1 if it was committed, and stops it from committing later.
     * It may run more than once for a write, when a reply is lost: the second key tells a write fenced by an earlier
     * run from a committed one. The executor sends nothing else until it's settled, so no later write can get in between.
     * KEYS: last applied write of this executor, last fenced write of this executor
     * ARGV: write id
     */
    private static final String DEBIT_FENCE_SCRIPT = """
            if tonumber(redis.call('GET',KEYS[1]) or '0')>=tonumber(ARGV[1]) then
              if redis.call('GET',KEYS[2])==ARGV[1] then return 0 end
              return 1
            end
            redis.call('SET',KEYS[1],ARGV[1],'EX',86400)
            redis.call('SET',KEYS[2],ARGV[1],'EX',86400)
            return 0""";
    /**
     * Puts back what Redis lost, without touching what it still has. Servers can disagree if one had missed updates, so
     * the balance with the highest sequence wins, in whatever order they come, plus the deltas written on top since then.
     * Sequences from the floor on come from after the loss: that balance already holds those deltas and has nothing to add.
     * The sequence must stay above what the servers have seen, or they would ignore every later balance as old.
     * KEYS: balances, sequence, accounts put back and the sequence of their balance
     * ARGV: highest sequence seen by this instance, then account, balance, sequence of the balance, ...
     */
    private static final String RESEED_SCRIPT = """
            local floor=math.max(tonumber(ARGV[1])+1,tonumber(redis.call('TIME')[1])*1000)
            if tonumber(redis.call('GET',KEYS[2]) or '0')<floor then
              redis.call('SET',KEYS[2],string.format('%.0f',floor))
            end
            redis.call('HSETNX',KEYS[3],':floor',redis.call('GET',KEYS[2]))
            local lost=tonumber(redis.call('HGET',KEYS[3],':floor'))
            for i=2,#ARGV,3 do
              local have=redis.call('ZSCORE',KEYS[1],ARGV[i])
              local put=redis.call('HGET',KEYS[3],ARGV[i])
              if not have then
                redis.call('ZADD',KEYS[1],ARGV[i+1],ARGV[i])
                redis.call('HSET',KEYS[3],ARGV[i],ARGV[i+2])
              elseif put and tonumber(ARGV[i+2])>tonumber(put) and tonumber(ARGV[i+2])<lost then
                local d=redis.call('HGET',KEYS[3],ARGV[i]..':d') or '0'
                redis.call('ZADD',KEYS[1],tostring(tonumber(ARGV[i+1])+tonumber(d)),ARGV[i])
                redis.call('HSET',KEYS[3],ARGV[i],ARGV[i+2])
              end
            end
            redis.call('EXPIRE',KEYS[3],3600)
            return 1""";
    /**
     * Lifts the sequence after Redis went back in time, so the servers that saw higher ones accept its balances again.
     * KEYS: sequence
     * ARGV: highest sequence seen by this instance
     */
    private static final String RAISE_SEQUENCE_SCRIPT = """
            if tonumber(redis.call('GET',KEYS[1]) or '0')<tonumber(ARGV[1]) then
              redis.call('SET',KEYS[1],ARGV[1])
            end
            return 1""";
    private static final int RESEED_BATCH_SIZE = 1000;
    /**
     * Stands for the executor index of the debits, which have their own thread
     */
    private static final int DEBIT_LANE_INDEX = -2;
    /**
     * Debits wait for their answer here and not behind the queued writes of the executor: on Redis deposits and debits
     * can be applied in any order as long as the funds are there, and the answer doesn't depend on unrelated accounts.
     */
    private final ExecutorService debitLane = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
    private static final String RESEED_KEY_SUFFIX = ":reseeded";
    protected static final long RESYNC_TIMEOUT_SECONDS = 30;
    protected static final String DEBIT_UNAVAILABLE_MESSAGE = "Redis did not confirm the funds, try again";

    protected enum DebitOutcome {
        COMMITTED, INSUFFICIENT_FUNDS, UNAVAILABLE
    }

    /**
     * @param balance the balance on Redis, NaN if it didn't answer
     */
    protected record DebitResult(DebitOutcome outcome, double balance) {
    }

    /**
     * The receiver of a payment
     */
    private record Credit<K>(K key, String member, String messagePrefix, String playerName, double amount, double seed, long seedSequence) {
    }

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
     * @param seedSequence  the sequence of that balance
     */
    protected record BalanceWrite<K>(BalanceLedger<K> ledger, K key, String member, String balancesKey, String channel,
                                     String messagePrefix, String playerName, WriteMode mode, double value,
                                     double seed, long seedSequence) {
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

        try { //Wait to avoid API calls before accounts are loaded
            final BalancesSnapshot players = readBalances(RedisKeys.BALANCE_PREFIX + currencyName);
            players.balances().forEach((member, balance) -> playerLedger.update(UUID.fromString(member), balance, players.sequence()));
            if (!accounts.isEmpty()) {
                RedisEconomyPlugin.debug("start1 Loaded " + accounts.size() + " accounts for currency " + currencyName);
            }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException("Can't load the accounts of currency " + currencyName, e);
        }

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
            if (arguments.length > 4 && !currenciesManager.acceptsDataEpoch(arguments[4])) return;
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
        synchronized (decimalFormat) { //DecimalFormat isn't thread safe and this is called from async threads
            return decimalFormat.format(amount) + (amount == 1 ? currencySingular : currencyPlural);
        }
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

        synchronized (playerLedger) {
            if (!hasAccount(playerUUID))
                return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Account not found");

            if (!has(playerUUID, amountToWithdraw))
                return new EconomyResponse(amountToWithdraw, getBalance(playerUUID), EconomyResponse.ResponseType.FAILURE, "Insufficient funds");
        }
        final DebitResult debit = debitAccount(playerUUID, playerName, amountToWithdraw);
        if (debit.outcome() == DebitOutcome.INSUFFICIENT_FUNDS)
            return new EconomyResponse(amountToWithdraw, debit.balance(), EconomyResponse.ResponseType.FAILURE, "Insufficient funds");
        if (debit.outcome() == DebitOutcome.UNAVAILABLE)
            return new EconomyResponse(amountToWithdraw, getBalance(playerUUID), EconomyResponse.ResponseType.FAILURE, DEBIT_UNAVAILABLE_MESSAGE);

        currenciesManager.getExchange().saveTransaction(new AccountID(playerUUID), new AccountID(), -amountToWithdraw, this, reason == null ? "Withdraw" : reason);
        return new EconomyResponse(amount, debit.balance(), EconomyResponse.ResponseType.SUCCESS, null);
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
     * Moves the money from sender to receiver in one step on Redis, which also decides if the sender has the funds
     */
    private EconomyResponse transfer(@NotNull UUID sender, @NotNull String senderName, @NotNull UUID receiver, @NotNull String receiverName, double amount) {
        //Calculate the amount to withdraw with the transaction tax
        double amountToWithdraw = amount + (amount * transactionTax);
        if (!isValidAmount(amount) || !isValidAmount(amountToWithdraw))
            return new EconomyResponse(0, 0, EconomyResponse.ResponseType.FAILURE, "Invalid decimal amount format");

        synchronized (playerLedger) {
            if (!hasAccount(sender) || !hasAccount(receiver))
                return new EconomyResponse(amount, getBalance(sender), EconomyResponse.ResponseType.FAILURE, "Account not found");

            if (!has(sender, amountToWithdraw))
                return new EconomyResponse(0, getBalance(sender), EconomyResponse.ResponseType.FAILURE, "Insufficient funds");

            if (getBalance(receiver) + amount > getPlayerMaxBalance(receiver))
                return new EconomyResponse(0, getBalance(receiver), EconomyResponse.ResponseType.FAILURE, "The receiver has reached the maximum balance");
        }
        final double senderSeed;
        final double receiverSeed;
        synchronized (playerLedger) {
            senderSeed = getBalance(sender);
            receiverSeed = getBalance(receiver);
            currenciesManager.updateNameUniqueId(senderName, sender);
            currenciesManager.updateNameUniqueId(receiverName, receiver);
        }
        final Credit<UUID> credit = new Credit<>(receiver, receiver.toString(),
                RedisEconomyPlugin.getInstanceUUID() + ";;" + receiver + ";;" + receiverName + ";;", receiverName, amount, receiverSeed, playerLedger.sequenceOf(receiver));
        final DebitResult debit = debit(new BalanceWrite<>(playerLedger, sender, sender.toString(),
                RedisKeys.BALANCE_PREFIX + currencyName,
                RedisKeys.UPDATE_PLAYER_CHANNEL_PREFIX + currencyName,
                RedisEconomyPlugin.getInstanceUUID() + ";;" + sender + ";;" + senderName + ";;",
                senderName, WriteMode.DELTA, -amountToWithdraw, senderSeed, playerLedger.sequenceOf(sender)
        ), (int) sender.getMostSignificantBits(), credit, (int) receiver.getMostSignificantBits());
        if (debit.outcome() == DebitOutcome.INSUFFICIENT_FUNDS)
            return new EconomyResponse(0, debit.balance(), EconomyResponse.ResponseType.FAILURE, "Insufficient funds");
        if (debit.outcome() == DebitOutcome.UNAVAILABLE)
            return new EconomyResponse(0, getBalance(sender), EconomyResponse.ResponseType.FAILURE, DEBIT_UNAVAILABLE_MESSAGE);

        return new EconomyResponse(amount, debit.balance(), EconomyResponse.ResponseType.SUCCESS, null);
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
                    name, mode, value, seed, playerLedger.sequenceOf(uuid)
            ), (int) uuid.getMostSignificantBits());
        }
    }

    private DebitResult debitAccount(@NotNull UUID uuid, @Nullable String playerName, double amount) {
        final double seed;
        synchronized (playerLedger) {
            seed = getBalance(uuid);
            if (playerName != null)
                currenciesManager.updateNameUniqueId(playerName, uuid);
        }
        final String name = playerName == null ? "" : playerName;
        return debit(new BalanceWrite<>(playerLedger, uuid, uuid.toString(),
                RedisKeys.BALANCE_PREFIX + currencyName,
                RedisKeys.UPDATE_PLAYER_CHANNEL_PREFIX + currencyName,
                RedisEconomyPlugin.getInstanceUUID() + ";;" + uuid + ";;" + name + ";;",
                name, WriteMode.DELTA, -amount, seed, playerLedger.sequenceOf(uuid)
        ), (int) uuid.getMostSignificantBits());
    }

    /**
     * Asks Redis to take the money and waits for the answer, for a bounded time. It goes through the executor of the
     * account: the writes queued before it, like a deposit, must count.
     * Don't call it holding the ledger: the executor needs it to finish those writes.
     *
     * @param write      The write, with the negative amount as value
     * @param identifier The identifier of the account
     */
    protected <K> DebitResult debit(@NotNull BalanceWrite<K> write, int identifier) {
        return debit(write, identifier, null, 0);
    }

    /**
     * @param credit           The receiver of a payment, moved in the same step
     * @param creditIdentifier The identifier of the receiver
     */
    private <K> DebitResult debit(@NotNull BalanceWrite<K> write, int identifier, @Nullable Credit<K> credit, int creditIdentifier) {
        final RedisManager redisManager = currenciesManager.getRedisManager();
        final DebitResult unavailable = new DebitResult(DebitOutcome.UNAVAILABLE, Double.NaN);
        if (!redisManager.canAttemptDebit()) return unavailable;

        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1, RedisEconomyPlugin.getInstance().settings().debitTimeoutMillis));
        //A set overwrites: only here the order with the queued writes matters
        if (write.ledger().hasPendingSet(write.key()) && !awaitQueuedWrites(identifier, deadline)) return unavailable;
        if (credit != null && write.ledger().hasPendingSet(credit.key()) && !awaitQueuedWrites(creditIdentifier, deadline)) return unavailable;

        final DebitResult first = sendThroughLane(write, credit, deadline, true);
        //Redis doesn't have the deposits still queued here yet. Only then the debit has to wait for them
        if (first.outcome() != DebitOutcome.INSUFFICIENT_FUNDS || !write.ledger().hasPendingWrites(write.key())) return first;
        if (!awaitQueuedWrites(identifier, deadline)) return unavailable;
        //A timeout here may only mean the wait took most of the time. It says nothing about Redis
        return sendThroughLane(write, credit, deadline, false);
    }

    /**
     * Waits until the writes queued now have been sent
     *
     * @return false if they were not sent before the deadline
     */
    private boolean awaitQueuedWrites(int identifier, long deadlineNanos) {
        final CountDownLatch sent = new CountDownLatch(1);
        runOnExecutor(Math.floorMod(identifier, updateExecutors.size()), sent::countDown);
        try {
            return sent.await(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private <K> DebitResult sendThroughLane(@NotNull BalanceWrite<K> write, @Nullable Credit<K> credit, long deadlineNanos, boolean timeoutMeansRedis) {
        final DebitResult unavailable = new DebitResult(DebitOutcome.UNAVAILABLE, Double.NaN);
        final CompletableFuture<DebitResult> answer = new CompletableFuture<>();
        try {
            debitLane.execute(() -> sendDebit(write, credit, answer, LAST_WRITE_ID.incrementAndGet()));
        } catch (RejectedExecutionException e) {
            return unavailable; //The currency has been replaced or the plugin is disabling
        }
        try {
            return answer.get(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            if (timeoutMeansRedis) currenciesManager.getRedisManager().debitNotAnswered();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            RedisEconomyPlugin.getInstance().getLogger().severe("Debit failed on " + write.member() + ": " + e.getCause());
        }
        //From here on the lane sees the refusal. If it was faster its answer is the valid one
        answer.complete(unavailable);
        return answer.join();
    }

    private <K> List<Object> evalDebit(@NotNull BalanceWrite<K> write, @Nullable Credit<K> credit, boolean force, long writeId) {
        final String[] keys = balanceWriteKeys(write, DEBIT_LANE_INDEX);
        final List<String> arguments = new ArrayList<>(List.of(
                write.member(), force ? "F" : "", String.valueOf(-write.value()), String.valueOf(write.seed()),
                String.valueOf(writeId), write.channel(), write.messagePrefix(), write.playerName()));
        if (credit == null) arguments.addAll(List.of("", "", "", "", ""));
        else arguments.addAll(List.of(credit.member(), String.valueOf(credit.amount()), String.valueOf(credit.seed()), credit.messagePrefix(), credit.playerName()));
        arguments.add(String.valueOf(write.seedSequence()));
        arguments.add(credit == null ? "0" : String.valueOf(credit.seedSequence()));
        return currenciesManager.getRedisManager().getConnectionSync(connection -> connection.eval(
                DEBIT_SCRIPT, ScriptOutputType.MULTI, keys, arguments.toArray(new String[0])));
    }

    private <K> void applyDebitReply(@NotNull BalanceWrite<K> write, @Nullable Credit<K> credit, @NotNull List<Object> reply) {
        final long sequence = Long.parseLong(String.valueOf(reply.get(2)));
        write.ledger().update(write.key(), Double.parseDouble(String.valueOf(reply.get(1))), sequence);
        if (credit != null) write.ledger().update(credit.key(), Double.parseDouble(String.valueOf(reply.get(3))), sequence);
    }

    private <K> void sendDebit(@NotNull BalanceWrite<K> write, @Nullable Credit<K> credit, @NotNull CompletableFuture<DebitResult> answer, long writeId) {
        if (answer.isDone()) return; //Refused before it was sent
        final RedisManager redisManager = currenciesManager.getRedisManager();
        final List<Object> reply;
        try {
            reply = evalDebit(write, credit, false, writeId);
        } catch (RuntimeException e) {
            //It may have been committed anyway, and the caller will be told it was not
            redisManager.debitNotAnswered();
            answer.complete(new DebitResult(DebitOutcome.UNAVAILABLE, Double.NaN));
            settleUnansweredDebit(write, credit, writeId);
            return;
        }
        redisManager.debitAnswered();
        applyDebitReply(write, credit, reply);
        final double balance = Double.parseDouble(String.valueOf(reply.get(1)));
        if (!String.valueOf(reply.get(0)).equals("O")) {
            answer.complete(new DebitResult(DebitOutcome.INSUFFICIENT_FUNDS, balance));
            return;
        }
        RedisEconomyPlugin.debugCache("01c Debited " + write.member() + " to " + balance + " currency " + currencyName);
        if (!answer.complete(new DebitResult(DebitOutcome.COMMITTED, balance))) refundDebit(write, credit);
    }

    /**
     * Blocks later debits until Redis tells what happened to this one: the fence needs the write ids to go up in order
     */
    private <K> void settleUnansweredDebit(@NotNull BalanceWrite<K> write, @Nullable Credit<K> credit, long writeId) {
        final RedisEconomyPlugin plugin = RedisEconomyPlugin.getInstance();
        final String appliedKey = balanceWriteKeys(write, DEBIT_LANE_INDEX)[3];
        final String[] fenceKeys = {appliedKey, appliedKey + ":fenced"};
        int tries = 0;
        while (true) {
            try {
                final Long committed = currenciesManager.getRedisManager().getConnectionSync(connection -> connection.eval(
                        DEBIT_FENCE_SCRIPT, ScriptOutputType.INTEGER, fenceKeys, String.valueOf(writeId)));
                if (committed != null && committed == 1) refundDebit(write, credit);
                return;
            } catch (RuntimeException e) {
                tries++;
                if (terminated && tries >= plugin.settings().redis.getTryAgainCount() || Thread.currentThread().isInterrupted()) {
                    plugin.getLogger().severe("A refused debit may have been committed, check it manually: account " + write.member()
                            + " amount " + -write.value() + " currency " + currencyName);
                    return;
                }
                try {
                    Thread.sleep(Math.min(tries * 100L, 2000L));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * The caller was told the debit failed but Redis committed it: give the money back
     */
    private <K> void refundDebit(@NotNull BalanceWrite<K> write, @Nullable Credit<K> credit) {
        RedisEconomyPlugin.getInstance().getLogger().warning("Refunding a debit Redis committed after it was refused: account "
                + write.member() + " amount " + -write.value() + " currency " + currencyName);
        if (credit == null) {
            write.ledger().applyDelta(write.key(), -write.value());
            writeUntilCommitted(new BalanceWrite<>(write.ledger(), write.key(), write.member(), write.balancesKey(), write.channel(),
                    write.messagePrefix(), write.playerName(), WriteMode.DELTA, -write.value(), write.seed(), write.seedSequence()), LAST_WRITE_ID.incrementAndGet(), DEBIT_LANE_INDEX);
            return;
        }
        //The same payment the other way round. The receiver was never told, but may have spent it: this can leave it negative
        final BalanceWrite<K> back = new BalanceWrite<>(write.ledger(), credit.key(), credit.member(), write.balancesKey(), write.channel(),
                credit.messagePrefix(), credit.playerName(), WriteMode.DELTA, -credit.amount(), credit.seed(), credit.seedSequence());
        final Credit<K> toSender = new Credit<>(write.key(), write.member(), write.messagePrefix(), write.playerName(), -write.value(), write.seed(), write.seedSequence());
        final long writeId = LAST_WRITE_ID.incrementAndGet();
        int tries = 0;
        while (true) {
            try {
                applyDebitReply(back, toSender, evalDebit(back, toSender, true, writeId));
                return;
            } catch (RuntimeException e) {
                tries++;
                if (terminated && tries >= RedisEconomyPlugin.getInstance().settings().redis.getTryAgainCount() || Thread.currentThread().isInterrupted()) {
                    RedisEconomyPlugin.getInstance().getLogger().severe("LOST refund, fix it manually: " + write.member() + " should get "
                            + -write.value() + " back from " + credit.member() + " currency " + currencyName);
                    return;
                }
                try {
                    Thread.sleep(Math.min(tries * 100L, 2000L));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
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
        try {
            //The id is taken when the write starts: player and bank writes share the executors but not a lock,
            //so ids taken before queueing could reach the executor out of order and the lower one would be ignored
            updateExecutors.get(executorIndex).execute(() -> writeUntilCommitted(write, LAST_WRITE_ID.incrementAndGet(), executorIndex));
        } catch (RejectedExecutionException e) {
            //The currency has been replaced or the plugin is disabling: don't lose the write
            synchronized (LAST_WRITE_ID) {
                writeUntilCommitted(write, LAST_WRITE_ID.incrementAndGet(), LATE_WRITES_INDEX);
            }
        }
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
        final String[] keys = balanceWriteKeys(write, executorIndex);
        int tries = 0;
        while (true) {
            RedisEconomyPlugin.debugCache("01a Writing " + write.member() + " " + write.mode() + " " + write.value() + " currency " + currencyName);
            try {
                final List<Object> reply = currenciesManager.getRedisManager().getConnectionSync(connection -> connection.eval(
                        BALANCE_WRITE_SCRIPT, ScriptOutputType.MULTI, keys,
                        write.member(), write.mode().scriptArgument, String.valueOf(write.value()), String.valueOf(write.seed()),
                        String.valueOf(writeId), write.channel(), write.messagePrefix(), write.playerName(), String.valueOf(write.seedSequence())));
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

    private <K> String[] balanceWriteKeys(@NotNull BalanceWrite<K> write, int executorIndex) {
        return new String[]{
                write.balancesKey(),
                RedisKeys.NAME_UUID.toString(),
                RedisKeys.BALANCE_SEQUENCE_PREFIX + currencyName,
                RedisKeys.APPLIED_WRITE_PREFIX + currencyName + ":" + RedisEconomyPlugin.getInstanceUUID() + ":" + executorIndex,
                write.balancesKey() + RESEED_KEY_SUFFIX,
                RedisKeys.DATA_EPOCH.toString()
        };
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
     * Reads the balances from Redis into the local cache, as after a restart, for the updates this instance missed.
     * It blocks: don't call it on the main thread
     */
    public void resyncLocal() throws ExecutionException, InterruptedException, TimeoutException {
        final BalancesSnapshot players = readBalances(RedisKeys.BALANCE_PREFIX + currencyName);
        players.balances().forEach((member, balance) -> playerLedger.update(UUID.fromString(member), balance, players.sequence()));

        final Map<UUID, Double> maxBalances = getPlayerMaxBalances().toCompletableFuture().get(RESYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        maxPlayerBalances.keySet().retainAll(maxBalances.keySet());
        maxPlayerBalances.putAll(maxBalances);
    }

    /**
     * Redis lost its data: puts back the accounts this instance knows, or those nobody writes would stay missing
     * and be lost for good when the servers restart. It blocks: don't call it on the main thread
     */
    public void reseedAfterDataLoss() throws ExecutionException, InterruptedException, TimeoutException {
        final Map<String, BalanceLedger.Committed> balances = new HashMap<>();
        playerLedger.committedBalances().forEach((uuid, committed) -> balances.put(uuid.toString(), committed));
        reseedBalances(RedisKeys.BALANCE_PREFIX + currencyName, balances);

        if (maxPlayerBalances.isEmpty()) return;
        final Map<String, String> maxBalances = new HashMap<>();
        maxPlayerBalances.forEach((uuid, maxBalance) -> maxBalances.put(uuid.toString(), String.valueOf(maxBalance)));
        currenciesManager.getRedisManager().getConnectionAsync(connection ->
                        connection.hset(RedisKeys.MAX_PLAYER_BALANCES + currencyName, maxBalances))
                .toCompletableFuture().get(RESYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    protected void reseedBalances(@NotNull String balancesKey, @NotNull Map<String, BalanceLedger.Committed> balances) throws ExecutionException, InterruptedException, TimeoutException {
        final String[] keys = {balancesKey, RedisKeys.BALANCE_SEQUENCE_PREFIX + currencyName, balancesKey + RESEED_KEY_SUFFIX};
        final String highest = String.valueOf(highestSeenSequence());
        final List<String> batch = new ArrayList<>(List.of(highest));
        final Iterator<Map.Entry<String, BalanceLedger.Committed>> iterator = balances.entrySet().iterator();
        //Also when there is nothing to put back: the sequence must go up anyway
        do {
            if (iterator.hasNext()) {
                final Map.Entry<String, BalanceLedger.Committed> entry = iterator.next();
                batch.add(entry.getKey());
                batch.add(String.valueOf(entry.getValue().balance()));
                batch.add(String.valueOf(entry.getValue().sequence()));
            }
            if (batch.size() <= RESEED_BATCH_SIZE * 3 && iterator.hasNext()) continue;
            final String[] arguments = batch.toArray(new String[0]);
            batch.clear();
            batch.add(highest);
            currenciesManager.getRedisManager().<Long>getConnectionAsync(connection -> connection.eval(
                            RESEED_SCRIPT, ScriptOutputType.INTEGER, keys, arguments))
                    .toCompletableFuture().get(RESYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } while (iterator.hasNext());
    }

    /**
     * The sequence on Redis must never be below this
     */
    public long highestSeenSequence() {
        return playerLedger.highestSequence();
    }

    /**
     * @return the key of the sequence of this currency, to compare it with {@link #highestSeenSequence()}
     */
    public String sequenceKey() {
        return RedisKeys.BALANCE_SEQUENCE_PREFIX + currencyName;
    }

    /**
     * Redis went back in time and it's the one to trust: its older balances replace the local ones in the next resync.
     * It blocks: don't call it on the main thread
     */
    public void acceptRollback() throws ExecutionException, InterruptedException, TimeoutException {
        final String highest = String.valueOf(highestSeenSequence());
        currenciesManager.getRedisManager().<Long>getConnectionAsync(connection -> connection.eval(
                        RAISE_SEQUENCE_SCRIPT, ScriptOutputType.INTEGER, new String[]{sequenceKey()}, highest))
                .toCompletableFuture().get(RESYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    protected record BalancesSnapshot(long sequence, Map<String, Double> balances) {
    }

    /**
     * Reads a balances sorted set and its sequence in one step, so an update that arrives later wins only if it's newer
     */
    protected BalancesSnapshot readBalances(@NotNull String balancesKey) throws ExecutionException, InterruptedException, TimeoutException {
        final List<Object> reply = currenciesManager.getRedisManager().<List<Object>>getConnectionAsync(connection -> connection.eval(
                "return {redis.call('GET',KEYS[2]) or '0',redis.call('ZRANGE',KEYS[1],0,-1,'WITHSCORES')}",
                ScriptOutputType.MULTI,
                balancesKey, RedisKeys.BALANCE_SEQUENCE_PREFIX + currencyName)
        ).toCompletableFuture().get(RESYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        final Map<String, Double> balances = new HashMap<>();
        final List<?> flat = (List<?>) reply.get(1);
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            balances.put(String.valueOf(flat.get(i)), Double.parseDouble(String.valueOf(flat.get(i + 1))));
        }
        return new BalancesSnapshot(Long.parseLong(String.valueOf(reply.get(0))), balances);
    }

    /**
     * Terminate all executors, after the queued balance writes have been sent to Redis
     */
    public void terminateExecutors() {
        debitLane.shutdown();
        updateExecutors.forEach(ExecutorService::shutdown);
        final long deadline = System.currentTimeMillis() + TERMINATION_TIMEOUT_MILLIS;
        try {
            debitLane.awaitTermination(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            for (ExecutorService executor : updateExecutors) {
                executor.awaitTermination(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        terminated = true;
        debitLane.shutdownNow();
        int notStarted = 0;
        for (ExecutorService executor : updateExecutors) {
            notStarted += executor.shutdownNow().size();
        }
        if (notStarted > 0) {
            RedisEconomyPlugin.getInstance().getLogger().severe("LOST " + notStarted + " balance writes of currency " + currencyName + ": Redis did not answer before the timeout");
        }
    }

}
