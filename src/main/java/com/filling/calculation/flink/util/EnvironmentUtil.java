package com.filling.calculation.flink.util;

import com.filling.calculation.common.CheckResult;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class EnvironmentUtil {

    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentUtil.class);

    public static void setRestartStrategy(JSONObject config, ExecutionConfig executionConfig) {
        try {
            if (config.containsKey(ConfigKeyName.RESTART_STRATEGY)) {
                String restartStrategy = config.getString(ConfigKeyName.RESTART_STRATEGY);
                switch (restartStrategy.toLowerCase()) {
                    case "no":
                        executionConfig.setRestartStrategy(RestartStrategies.noRestart());
                        break;
                    case "fixed-delay":
                        int attempts = config.getInteger(ConfigKeyName.RESTART_ATTEMPTS);
                        long delay = config.getLong(ConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS);
                        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(attempts, delay));
                        break;
                    case "failure-rate":
                        long failureInterval = config.getLong(ConfigKeyName.RESTART_FAILURE_INTERVAL);
                        int rate = config.getInteger(ConfigKeyName.RESTART_FAILURE_RATE);
                        long delayInterval = config.getLong(ConfigKeyName.RESTART_DELAY_INTERVAL);
                        executionConfig.setRestartStrategy(RestartStrategies.failureRateRestart(rate,
                                Time.of(failureInterval, TimeUnit.MILLISECONDS),
                                Time.of(delayInterval, TimeUnit.MILLISECONDS)));
                        break;
                    default:
                        LOG.warn("set restart.strategy failed, unknown restart.strategy [{}],only support no,fixed-delay,failure-rate", restartStrategy);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static CheckResult checkRestartStrategy(JSONObject config){
        if (config.containsKey(ConfigKeyName.RESTART_STRATEGY)) {
            String restartStrategy = config.getString(ConfigKeyName.RESTART_STRATEGY);
            switch (restartStrategy.toLowerCase()) {
                case "fixed-delay":
                    if (!(config.containsKey(ConfigKeyName.RESTART_ATTEMPTS)
                            && config.containsKey(ConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS))) {
                        return new CheckResult(false, String.format("fixed-delay restart strategy must set [%s],[%s]"
                                , ConfigKeyName.RESTART_ATTEMPTS, ConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS));
                    }
                    break;
                case "failure-rate":
                    if (!(config.containsKey(ConfigKeyName.RESTART_FAILURE_INTERVAL)
                            && config.containsKey(ConfigKeyName.RESTART_FAILURE_RATE)
                            && config.containsKey(ConfigKeyName.RESTART_DELAY_INTERVAL))) {
                        return new CheckResult(false, String.format("failure-rate restart strategy must set [%s],[%s],[%s]"
                                , ConfigKeyName.RESTART_FAILURE_INTERVAL, ConfigKeyName.RESTART_FAILURE_RATE, ConfigKeyName.RESTART_DELAY_INTERVAL));
                    }
                    break;
                default:
                    return new CheckResult(true, "");
            }
        }
        return new CheckResult(true,"");
    }
}
