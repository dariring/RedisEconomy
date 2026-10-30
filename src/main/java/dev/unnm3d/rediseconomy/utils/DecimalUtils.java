package dev.unnm3d.rediseconomy.utils;

import dev.unnm3d.rediseconomy.RedisEconomyPlugin;
import lombok.experimental.UtilityClass;

import java.text.DecimalFormat;

@UtilityClass
public class DecimalUtils {

    public static String shortAmount(double amount, DecimalFormat decimalFormat) {
        synchronized (decimalFormat) { //The format is shared and this is called from async threads
            return shortAmountUnsafe(amount, decimalFormat);
        }
    }

    private static String shortAmountUnsafe(double amount, DecimalFormat decimalFormat) {
        if (amount >= 1000000000000.0) {
            return decimalFormat.format(amount / 1000000000000.0) +
                    RedisEconomyPlugin.getInstance().getConfigManager().getLangs().unitSymbols.trillion();
        } else if (amount >= 1000000000.0) {
            return decimalFormat.format(amount / 1000000000.0) +
                    RedisEconomyPlugin.getInstance().getConfigManager().getLangs().unitSymbols.billion();
        } else if (amount >= 1000000.0) {
            return decimalFormat.format(amount / 1000000.0) +
                    RedisEconomyPlugin.getInstance().getConfigManager().getLangs().unitSymbols.million();
        } else if (amount >= 1000.0) {
            return decimalFormat.format(amount / 1000.0) +
                    RedisEconomyPlugin.getInstance().getConfigManager().getLangs().unitSymbols.thousand();
        }
        return decimalFormat.format(amount);
    }
}
