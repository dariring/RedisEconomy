package dev.unnm3d.rediseconomy.currency;

import dev.unnm3d.rediseconomy.RedisEconomyPlugin;
import dev.unnm3d.rediseconomy.api.RedisEconomyAPI;
import dev.unnm3d.rediseconomy.config.ConfigManager;
import dev.unnm3d.rediseconomy.config.CurrencySettings;
import dev.unnm3d.rediseconomy.redis.RedisKeys;
import dev.unnm3d.rediseconomy.redis.RedisManager;
import dev.unnm3d.rediseconomy.transaction.EconomyExchange;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import net.milkbowl.vault.economy.Economy;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.ServicePriority;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static dev.unnm3d.rediseconomy.redis.RedisKeys.*;


public class CurrenciesManager extends RedisEconomyAPI implements Listener {
    private final RedisEconomyPlugin plugin;
    @Getter
    private final CompletableFuture<Void> completeMigration;
    private final ConfigManager configManager;
    @Getter
    private final RedisManager redisManager;
    private final EconomyExchange exchange;
    private final ConcurrentHashMap<String, Currency> currencies;
    @Getter
    private final ConcurrentHashMap<String, UUID> nameUniqueIds;
    private final ConcurrentHashMap<UUID, List<UUID>> lockedAccounts;
    /**
     * The value this instance knows of a key that only disappears when Redis loses its data
     */
    private volatile String dataEpoch;
    private static final long RESYNC_RETRY_MILLIS = 2000;
    /**
     * How often to look for a Redis that lost its data or went back in time with the connections up
     */
    private static final long DATA_CHECK_MILLIS = 5000;
    /**
     * Bump it when instances of different versions can't share Redis safely
     */
    private static final int PROTOCOL_VERSION = 3;
    private static final int INSTANCE_TTL_SECONDS = 30;
    private static final long INSTANCE_RECHECK_MILLIS = 7000;
    private static final long OLD_INSTANCE_WARNING_MILLIS = 60_000;
    private final Map<String, Long> instancesChecked = new ConcurrentHashMap<>();


    public CurrenciesManager(RedisManager redisManager, RedisEconomyPlugin plugin, ConfigManager configManager) {
        INSTANCE = this;
        this.completeMigration = new CompletableFuture<>();
        this.redisManager = redisManager;
        this.exchange = new EconomyExchange(plugin);
        this.plugin = plugin;
        this.configManager = configManager;
        this.currencies = new ConcurrentHashMap<>();
        try {
            this.nameUniqueIds = loadRedisNameUniqueIds().toCompletableFuture().get(plugin.getConfigManager().getSettings().redis.timeout(), TimeUnit.MILLISECONDS);
            this.lockedAccounts = loadLockedAccounts().toCompletableFuture().get(plugin.getConfigManager().getSettings().redis.timeout(), TimeUnit.MILLISECONDS);
            this.dataEpoch = readDataEpoch().toCompletableFuture().get(plugin.getConfigManager().getSettings().redis.timeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        loadCurrencySystem(false);

        if (plugin.settings().migrationEnabled) return;

        //Register default currency (which is skipped when loadCurrencySystem) into Vault API
        if (plugin.getServer().getServicesManager().getRegistrations(net.milkbowl.vault.economy.Economy.class)
                .stream().anyMatch(registration -> registration.getPlugin().getName().equals("RedisEconomy"))) {
            plugin.getLogger().severe("Reloading the plugin with a PluginManager may cause issues with Vault-registered currencies! Please restart the server instead.");
        }
        loadDefaultCurrency(plugin.getVaultPlugin());

        registerPayMsgChannel();
        registerBlockAccountChannel();
        registerUpdateChannelPattern();
        redisManager.setReconnectTask(this::resync);
        checkEvictionPolicy();
        registerInstance();
        redisManager.scheduleRepeating(this::checkDataState, DATA_CHECK_MILLIS);
    }

    /**
     * Redis can't tell which balances an eviction removed, so it must not evict at all
     */
    private void checkEvictionPolicy() {
        try {
            final Map<String, String> config = redisManager.<Map<String, String>>getConnectionAsync(connection -> connection.configGet("maxmemory-policy"))
                    .toCompletableFuture().get(plugin.settings().redis.timeout(), TimeUnit.MILLISECONDS);
            final String policy = config.get("maxmemory-policy");
            if (policy == null) {
                plugin.getLogger().warning("Can't read maxmemory-policy from Redis. It must be noeviction, or balances may disappear");
            } else if (!policy.equals("noeviction")) {
                plugin.getLogger().severe("Redis maxmemory-policy is " + policy + ": Redis may evict balances and RedisEconomy can't notice it. Set it to noeviction");
            }
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            plugin.getLogger().warning("Can't read maxmemory-policy from Redis (CONFIG may be denied), carrying on. It must be noeviction, or balances may disappear: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void registerInstance() {
        redisManager.getConnectionAsync(connection -> connection.setex(
                INSTANCE_PREFIX + RedisEconomyPlugin.getInstanceUUID().toString(), INSTANCE_TTL_SECONDS, String.valueOf(PROTOCOL_VERSION)));
    }

    /**
     * An instance older than this one doesn't register itself, or registers a lower version.
     * The key may be missing for a moment after a data loss, so a missing key is looked at again later.
     */
    private void checkInstance(String instanceId, boolean secondLook) {
        final long now = System.currentTimeMillis();
        if (!secondLook) {
            final Long last = instancesChecked.get(instanceId);
            if (last != null && now - last < OLD_INSTANCE_WARNING_MILLIS) return;
            instancesChecked.put(instanceId, now);
        }
        redisManager.getConnectionAsync(connection -> connection.get(INSTANCE_PREFIX + instanceId)).thenAccept(version -> {
            if (version == null && !secondLook) {
                redisManager.scheduleOnce(() -> checkInstance(instanceId, true), INSTANCE_RECHECK_MILLIS);
            } else if (version == null || Integer.parseInt(version) < PROTOCOL_VERSION) {
                warnOldInstance(instanceId);
            }
        });
    }

    private void warnOldInstance(String instanceId) {
        plugin.getLogger().severe("A server with an older RedisEconomy is sharing this Redis (instance " + instanceId + "). It can overdraw accounts and, "
                + "if older still, overwrite balances. Upgrade all servers to the same version NOW");
    }

    /**
     * An update from after a data loss this instance hasn't noticed yet would replace the balance it has to put back,
     * maybe with an older one from a server that had missed updates. The resync that follows reads it anyway.
     */
    boolean acceptsDataEpoch(String epoch) {
        if (epoch.equals(dataEpoch)) return true;
        redisManager.scheduleReconnectTask(0);
        return false;
    }

    /**
     * Notices a Redis that lost its data or went back to an older snapshot, even if no connection dropped.
     * Runs off the main thread.
     */
    private void checkDataState() {
        registerInstance();
        final List<Currency> currencyList = new ArrayList<>(getCurrencies());
        final Map<Currency, Long> seen = new HashMap<>();
        //Read before Redis: what this instance has seen by now can't be above Redis unless Redis went back
        currencyList.forEach(currency -> seen.put(currency, currency.highestSeenSequence()));
        final String[] keys = new String[currencyList.size() + 1];
        keys[0] = DATA_EPOCH.toString();
        for (int i = 0; i < currencyList.size(); i++) keys[i + 1] = currencyList.get(i).sequenceKey();
        final List<Object> reply;
        try {
            reply = redisManager.<List<Object>>getConnectionAsync(connection -> connection.eval(
                            "local r={redis.call('GET',KEYS[1]) or ''} for i=2,#KEYS do r[i]=redis.call('GET',KEYS[i]) or '0' end return r",
                            ScriptOutputType.MULTI, keys))
                    .toCompletableFuture().get(plugin.settings().redis.timeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            return; //Redis is unreachable: the reconnection will start a resync
        }
        boolean changed = !String.valueOf(reply.get(0)).equals(dataEpoch);
        for (int i = 0; i < currencyList.size(); i++) {
            changed |= Long.parseLong(String.valueOf(reply.get(i + 1))) < seen.get(currencyList.get(i));
        }
        if (changed) redisManager.scheduleReconnectTask(0);
    }

    public void loadCurrencySystem(boolean isReload) {
        for (CurrencySettings currencySettings : configManager.getSettings().currencies) {
            //Do not register the default currency twice on Vault or the plugins that rely on it will malfunction
            if (isReload && currencySettings.getCurrencyName().equals(configManager.getSettings().defaultCurrencyName))
                continue;

            //Send the queued writes of the old currency first, so the new one loads them from Redis
            Optional.ofNullable(currencies.get(currencySettings.getCurrencyName()))
                    .ifPresent(Currency::terminateExecutors);

            Currency currency;
            if (currencySettings.isBankEnabled()) {
                currency = new CurrencyWithBanks(this, currencySettings);
            } else {
                currency = new Currency(this, currencySettings);
            }

            //Replace existing currency with updated settings
            currencies.put(currencySettings.getCurrencyName(), currency);
        }
        //Remove currencies that are not in the config anymore
        currencies.entrySet().removeIf(entry->{
            if (configManager.getSettings().currencies.stream().noneMatch(cs -> cs.getCurrencyName().equals(entry.getKey()))) {
                entry.getValue().terminateExecutors();
                return true;
            }
            return false;
        });
        if (currencies.get(configManager.getSettings().defaultCurrencyName) == null) {
            currencies.put(configManager.getSettings().defaultCurrencyName, new Currency(this,
                    new CurrencySettings(configManager.getSettings().defaultCurrencyName,
                            "€", "€",
                            "#.##", "en-US",
                            0.0, Double.POSITIVE_INFINITY,
                            0.0, true,
                            -1, true,
                            false, 2)));
        }
    }

    /**
     * Loads the default currency into the vault economy provider
     * Unregisters the existent economy provider
     *
     * @param vaultPlugin the vault plugin
     */
    public void loadDefaultCurrency(Plugin vaultPlugin) {
        for (RegisteredServiceProvider<Economy> registration : plugin.getServer().getServicesManager().getRegistrations(Economy.class)) {
            plugin.getServer().getServicesManager().unregister(Economy.class, registration.getProvider());
        }
        plugin.getServer().getServicesManager().register(Economy.class, getDefaultCurrency(), vaultPlugin, ServicePriority.High);
        plugin.getLogger().info("Successfully registered RedisEconomy default currency '" + getDefaultCurrency().getCurrencyName() + "' as economy provider in Vault");
    }

    @Override
    public @Nullable Currency getCurrencyByName(@NotNull String name) {
        return currencies.get(name);
    }

    @Override
    public @NotNull EconomyExchange getExchange() {
        return exchange;
    }

    @Override
    public @NotNull Collection<Currency> getCurrencies() {
        return currencies.values();
    }

    @Override
    public @NotNull Map<String, Currency> getCurrenciesWithNames() {
        return Collections.unmodifiableMap(currencies);
    }

    @Override
    public @NotNull Currency getDefaultCurrency() {
        return currencies.get(configManager.getSettings().defaultCurrencyName);
    }

    void updateNameUniqueId(String name, UUID uuid) {
        nameUniqueIds.put(name, uuid);
    }

    /**
     * Removes all players with the given name pattern
     *
     * @param namePattern  the pattern to match
     * @param resetBalance if true, the balance of the removed players will be set to 0
     * @return a map of removed players Name-UUID
     */
    public HashMap<String, UUID> removeNamePattern(String namePattern, boolean resetBalance) {
        HashMap<String, UUID> removed = new HashMap<>();
        for (Map.Entry<String, UUID> entry : nameUniqueIds.entrySet()) {
            if (entry.getKey().matches(namePattern)) {
                removed.put(entry.getKey(), entry.getValue());
            }
        }
        nameUniqueIds.entrySet().removeAll(removed.entrySet());
        if (!removed.isEmpty()) {
            removeRedisNameUniqueIds(removed);
            if (resetBalance) {
                for (Currency currency : currencies.values()) {
                    //Without the name: it would store the name-uuid just removed again
                    removed.forEach((name, uuid) -> currency.setPlayerBalance(uuid, null, 0.0));
                }
            }
        }
        return removed;
    }

    /**
     * Resets the balance of all players with the given name pattern
     *
     * @param namePattern   the pattern to match
     * @param currencyReset the currency to be reset
     * @return a map of reset players
     */
    public HashMap<String, UUID> resetBalanceNamePattern(String namePattern, Currency currencyReset) {
        HashMap<String, UUID> removed = new HashMap<>();
        currencyReset.getOrderedAccounts(Integer.MAX_VALUE).thenAccept(accounts -> {
            for (ScoredValue<String> account : accounts) {
                UUID uuid = UUID.fromString(account.getValue());
                if (!nameUniqueIds.containsValue(uuid)) {
                    currencyReset.setPlayerBalance(uuid, null, 0.0);
                }
            }
        });

        for (Map.Entry<String, UUID> entry : nameUniqueIds.entrySet()) {
            if (entry.getKey().matches(namePattern)) {
                removed.put(entry.getKey(), entry.getValue());
            }
        }
        if (!removed.isEmpty()) {
            removed.forEach((name, uuid) -> currencyReset.setPlayerBalance(uuid, name, 0.0));
        }
        return removed;
    }

    @Override
    public @Nullable UUID getUUIDFromUsernameCache(@NotNull String username) {
        return nameUniqueIds.get(username);
    }

    @Override
    public @Nullable String getUsernameFromUUIDCache(@NotNull UUID uuid) {
        if (uuid.equals(RedisKeys.getServerUUID())) return "Server";
        return nameUniqueIds.entrySet().stream()
                .filter(e -> e.getValue().equals(uuid))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseGet(() -> {
                    RedisEconomyPlugin.debug("Couldn't find username for UUID " + uuid + " in cache!");
                    return null;
                });
    }

    @Override
    public @NotNull String getCaseSensitiveName(@NotNull String caseInsensitiveName) {
        for (Map.Entry<String, UUID> entry : nameUniqueIds.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(caseInsensitiveName))
                return entry.getKey();
        }
        return caseInsensitiveName;
    }

    @Override
    public @Nullable Currency getCurrencyBySymbol(@NotNull String symbol) {
        for (Currency currency : currencies.values()) {
            if (currency.getCurrencySingular().equals(symbol) || currency.getCurrencyPlural().equals(symbol))
                return currency;
        }
        return null;
    }

    @EventHandler
    private void onJoin(PlayerJoinEvent e) {
        getCurrencies().forEach(currency ->
                currency.refreshAccountLocal(e.getPlayer().getUniqueId(), e.getPlayer().getName())
                        .thenAccept(balance -> {
                            RedisEconomyPlugin.debug("00 Loaded " + e.getPlayer().getName() + "'s balance of " + balance + " " + currency.getCurrencyName());

                            if (balance == null) {
                                currency.createPlayerAccount(e.getPlayer());
                            }
                        }).exceptionally(ex -> {
                            ex.printStackTrace();
                            return null;
                        }));
    }

    private CompletionStage<ConcurrentHashMap<String, UUID>> loadRedisNameUniqueIds() {
        return redisManager.getConnectionAsync(connection ->
                connection.hgetall(NAME_UUID.toString())
                        .thenApply(result -> {
                            ConcurrentHashMap<String, UUID> nameUUIDs = new ConcurrentHashMap<>();
                            result.forEach((name, uuid) -> nameUUIDs.put(name, UUID.fromString(uuid)));
                            RedisEconomyPlugin.debug("start0 Loaded " + nameUUIDs.size() + " name-uuid pairs");
                            return nameUUIDs;
                        })
        );
    }

    private CompletionStage<ConcurrentHashMap<UUID, List<UUID>>> loadLockedAccounts() {
        return redisManager.getConnectionAsync(connection ->
                connection.hgetall(LOCKED_ACCOUNTS.toString())
                        .thenApply(result -> {
                            ConcurrentHashMap<UUID, List<UUID>> lockedAccounts = new ConcurrentHashMap<>();
                            result.forEach((uuid, uuidList) ->
                                    lockedAccounts.put(UUID.fromString(uuid),
                                            new ArrayList<>(Arrays.stream(uuidList.split(","))
                                                    .filter(stringUUID -> stringUUID.length() >= 32)
                                                    .map(UUID::fromString).toList())
                                    )
                            );
                            return lockedAccounts;
                        })
        );
    }

    private void removeRedisNameUniqueIds(Map<String, UUID> toRemove) {
        String[] toRemoveArray = toRemove.keySet().toArray(new String[0]);
        for (int i = 0; i < toRemoveArray.length; i++) {
            if (toRemoveArray[i] == null) {
                toRemoveArray[i] = "null";
            }
        }
        redisManager.getConnectionAsync(connection ->
                connection.hdel(NAME_UUID.toString(), toRemoveArray).thenAccept(integer ->
                        RedisEconomyPlugin.debug("purge0 Removed " + integer + " name-uuid pairs")));
    }

    private void registerPayMsgChannel() {
        StatefulRedisPubSubConnection<String, String> connection = redisManager.getPubSubConnection();
        connection.addListener(new RedisEconomyListener() {
            @Override
            public void message(String channel, String message) {
                String[] args = message.split(";;");
                String sender = args[0];
                String target = args[1];
                String currencyAmount = args[2];
                Player online = plugin.getServer().getPlayerExact(target);
                if (online != null) {
                    if (online.isOnline()) {
                        configManager.getLangs().send(online, configManager.getLangs().payReceived
                                .replace("%player%", sender)
                                .replace("%amount%", currencyAmount));
                        RedisEconomyPlugin.debug("02b Received pay message to " + online.getName() + " timestamp: " + System.currentTimeMillis());

                    }
                }
            }
        });
        connection.async().subscribe(MSG_CHANNEL.toString());
        RedisEconomyPlugin.debug("start2 Registered pay message channel");

    }

    private CompletionStage<String> readDataEpoch() {
        return redisManager.<String>getConnectionAsync(connection -> connection.eval(
                "local e=redis.call('GET',KEYS[1]) if e then return e end redis.call('SET',KEYS[1],ARGV[1]) return ARGV[1]",
                ScriptOutputType.VALUE, new String[]{DATA_EPOCH.toString()}, UUID.randomUUID().toString()));
    }

    /**
     * Brings this instance up to date with Redis after a time it wasn't listening, and puts back what Redis lost.
     * Writes queued meanwhile are safe: the ledger keeps them on top of the balances read here.
     */
    private void resync() {
        try {
            final long timeout = plugin.settings().redis.timeout();
            final String epoch = readDataEpoch().toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            boolean othersMustResync = false;
            if (!epoch.equals(dataEpoch)) {
                plugin.getLogger().severe("Redis lost its data: restoring the balances known by this server. Check them with a backup");
                for (Currency currency : getCurrencies()) currency.reseedAfterDataLoss();
                reseedNamesAndLocks();
                dataEpoch = epoch;
                registerInstance();
                othersMustResync = true;
            } else {
                for (Currency currency : getCurrencies()) {
                    final long seen = currency.highestSeenSequence();
                    final String sequence = redisManager.<String>getConnectionAsync(connection -> connection.get(currency.sequenceKey()))
                            .toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                    if (sequence == null ? seen > 0 : Long.parseLong(sequence) < seen) {
                        plugin.getLogger().severe("Redis went back to an older state on currency " + currency.getCurrencyName()
                                + ": taking its balances as they are, later changes are gone. Check them with a backup");
                        currency.acceptRollback();
                        othersMustResync = true;
                    }
                }
            }
            nameUniqueIds.putAll(loadRedisNameUniqueIds().toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS));
            final Map<UUID, List<UUID>> locked = loadLockedAccounts().toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            lockedAccounts.keySet().retainAll(locked.keySet());
            lockedAccounts.putAll(locked);
            for (Currency currency : getCurrencies()) currency.resyncLocal();
            plugin.getLogger().info("Balances synchronized with Redis");
            if (othersMustResync) publishResync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            plugin.getLogger().warning("Can't synchronize balances with Redis, trying again: " + e);
            redisManager.scheduleReconnectTask(RESYNC_RETRY_MILLIS);
        }
    }

    private void reseedNamesAndLocks() throws ExecutionException, InterruptedException, TimeoutException {
        final long timeout = plugin.settings().redis.timeout();
        if (!nameUniqueIds.isEmpty()) {
            final Map<String, String> names = new HashMap<>();
            nameUniqueIds.forEach((name, uuid) -> names.put(name, uuid.toString()));
            redisManager.getConnectionAsync(connection -> connection.hset(NAME_UUID.toString(), names))
                    .toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        }
        if (!lockedAccounts.isEmpty()) {
            final Map<String, String> locks = new HashMap<>();
            lockedAccounts.forEach((uuid, locked) -> locks.put(uuid.toString(), locked.stream().map(UUID::toString).collect(Collectors.joining(","))));
            redisManager.getConnectionAsync(connection -> connection.hset(LOCKED_ACCOUNTS.toString(), locks))
                    .toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Tells all instances, this one included, to read the balances again: Redis was changed without publishing updates
     */
    public void publishResync() {
        redisManager.getConnectionAsync(connection -> connection.publish(RESYNC_CHANNEL.toString(), "resync"));
    }

    /**
     * Switches the currency accounts of two currencies
     *
     * @param currency    The currency to switch
     * @param newCurrency The new currency to switch to
     */
    public void switchCurrency(Currency currency, Currency newCurrency) {
        if (currency.getCurrencyName().equals(newCurrency.getCurrencyName())) return;
        final String currencyKey = RedisKeys.BALANCE_PREFIX + currency.getCurrencyName();
        //In one script: a key that doesn't exist must not leave the balances in both currencies or half swapped
        redisManager.<Long>getConnectionAsync(asyncCommands -> asyncCommands.eval(
                "local a=redis.call('EXISTS',KEYS[1])local b=redis.call('EXISTS',KEYS[2])" +
                        "if a==1 then redis.call('RENAME',KEYS[1],KEYS[3])end " +
                        "if b==1 then redis.call('RENAME',KEYS[2],KEYS[1])end " +
                        "if a==1 then redis.call('RENAME',KEYS[3],KEYS[2])end return a+b",
                ScriptOutputType.INTEGER,
                new String[]{currencyKey, RedisKeys.BALANCE_PREFIX + newCurrency.getCurrencyName(), currencyKey + "_switching"}
        )).whenComplete((swapped, throwable) -> {
            if (throwable != null) plugin.getLogger().severe("Failed to switch currencies: " + throwable.getMessage());
            else RedisEconomyPlugin.debug("Switch0 - Swapped currency accounts: " + swapped);
        });
    }

    /**
     * Toggles the lock status of an account
     *
     * @param uuid   The uuid of the player
     * @param target The uuid of the target
     * @return completable future true if the account is locked, false if it is unlocked
     */
    public CompletableFuture<Boolean> toggleAccountLock(@NotNull UUID uuid, UUID target) {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        List<UUID> locked = lockedAccounts.getOrDefault(uuid, new ArrayList<>());
        boolean isLocked = locked.contains(target);

        if (isLocked) {
            locked.remove(target);
        } else {
            locked.add(target);
        }

        //The string to be stored into Redis
        String redisString = locked.stream().map(UUID::toString).collect(Collectors.joining(","));

        redisManager.getConnectionPipeline(connection -> {
                    connection.hset(LOCKED_ACCOUNTS.toString(),
                            uuid.toString(),
                            redisString
                    );
                    connection.publish(UPDATE_LOCKED_ACCOUNTS.toString(), uuid + "," + redisString)
                            .thenAccept(integer -> {
                                RedisEconomyPlugin.debug("Lock0 - Published update locked accounts message: " + integer);

                                lockedAccounts.put(uuid, locked);
                                future.complete(!isLocked);
                            });
                    return null;
                }
        );

        return future;
    }

    public boolean isAccountLocked(UUID uuid, UUID target) {
        return getLockedAccounts(uuid).contains(target) ||
                getLockedAccounts(uuid).contains(RedisKeys.getAllAccountUUID());
    }

    /**
     * Formats the amount string to a double
     * Parses suffixes (10k, 10M for 10 thousand and 10 million)
     * Parses percentages ("10%" for 10% of the accountOwner balance)
     *
     * @param targetName The account that has to be modified
     * @param currency   the currency to format the amount
     * @param amount     the amount to format
     * @return the formatted amount
     */
    public double formatAmountString(String targetName, Currency currency, String amount) {
        if (amount.isEmpty()) return -1;
        try {
            //Check if last char of amount is number
            if (Character.isDigit(amount.charAt(amount.length() - 1))) {
                return Double.parseDouble(amount);
            }
            double parsedAmount = Double.parseDouble(amount.substring(0, amount.length() - 1));
            return amount.endsWith("%") && plugin.settings().allowPercentagePayments ?
                    //Percentage 20%
                    parsedAmount / 100 * currency.getBalance(targetName) :
                    //Parse suffixes from map
                    parsedAmount * plugin.langs().getSuffixes()
                            .getOrDefault(amount.substring(amount.length() - 1), -1L);
        } catch (NumberFormatException e) {
            plugin.langs().send(Bukkit.getConsoleSender(), plugin.langs().invalidAmount);
        }
        return -1;
    }

    public List<UUID> getLockedAccounts(UUID uuid) {
        return lockedAccounts.getOrDefault(uuid, new ArrayList<>());
    }

    private void registerBlockAccountChannel() {
        StatefulRedisPubSubConnection<String, String> connection = redisManager.getPubSubConnection();
        connection.addListener(new RedisEconomyListener() {
            @Override
            public void message(String channel, String message) {
                String[] args = message.split(",");
                try {
                    UUID account = UUID.fromString(args[0]);
                    if (args.length == 1 || args[1].isEmpty()) {
                        lockedAccounts.remove(account);
                        return;
                    }
                    List<UUID> newLockedAccounts = new ArrayList<>();
                    for (int i = 1; i < args.length; i++) {
                        newLockedAccounts.add(UUID.fromString(args[i]));
                    }
                    lockedAccounts.put(account, newLockedAccounts);
                    RedisEconomyPlugin.debug("Lockupdate Registered locked accounts for " + account);

                } catch (IllegalArgumentException e) {
                    Bukkit.getLogger().warning("Lockupdate Received invalid uuid: " + args[0]);
                }
            }
        });
        connection.async().subscribe(UPDATE_LOCKED_ACCOUNTS.toString());
        RedisEconomyPlugin.debug("Lockch Registered locked accounts channel");

    }

    private void registerUpdateChannelPattern() {
        StatefulRedisPubSubConnection<String, String> connection = redisManager.getPubSubConnection();
        connection.addListener(new RedisEconomyListener() {
            @Override
            public void message(String channel, String message) {
                if (channel.equals(RESYNC_CHANNEL.toString())) redisManager.scheduleReconnectTask(0);
            }

            @Override
            public void message(String pattern, String channel, String message) {
                String[] split = message.split(";;");
                if (split.length < 3) {
                    Bukkit.getLogger().severe("Invalid message received from RedisEco channel, consider updating RedisEconomy");
                    return;
                }
                //0 instanceID, 1 playerUUID, 2 playerName/maxBal, 3 balance/emtpty/empty (if applicable)
                if (split[0].equals(RedisEconomyPlugin.getInstanceUUID().toString())) return;
                checkInstance(split[0], false);
                final String[] updateArgs = Arrays.copyOfRange(split, 1, split.length);
                for (Currency currency : getCurrencies()) {
                    String name = currency.getCurrencyName();
                    if (!channel.endsWith(name)) continue;
                    //"dollar" also ends with "lar": the rest of the channel must be a prefix, or the update is lost
                    final String prefix = channel.substring(0, channel.length() - name.length());
                    if (prefix.equals(UPDATE_PLAYER_CHANNEL_PREFIX.toString()) || prefix.equals(UPDATE_MAX_BAL_PREFIX.toString())
                            || prefix.equals(UPDATE_BANK_CHANNEL_PREFIX.toString()) || prefix.equals(UPDATE_BANK_OWNER_CHANNEL_PREFIX.toString())) {
                        currency.processUpdateMessage(prefix, updateArgs);
                        return;
                    }
                }
            }
        });
        connection.async().psubscribe(RedisKeys.UPDATE_PLAYER_CHANNEL_PREFIX.wildcard(), RedisKeys.UPDATE_MAX_BAL_PREFIX.wildcard(),
                RedisKeys.UPDATE_BANK_CHANNEL_PREFIX.wildcard(), UPDATE_BANK_OWNER_CHANNEL_PREFIX.wildcard());
        connection.async().subscribe(RESYNC_CHANNEL.toString());
        RedisEconomyPlugin.debug("start1b Listening to RedisEco channel " + RedisKeys.UPDATE_PLAYER_CHANNEL_PREFIX.wildcard());

    }

    public void terminate() {
        currencies.values().forEach(Currency::terminateExecutors);
        exchange.terminate();
    }
}
