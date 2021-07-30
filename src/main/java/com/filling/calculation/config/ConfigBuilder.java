package com.filling.calculation.config;

import com.filling.calculation.env.Execution;
import com.filling.calculation.env.RuntimeEnv;
import com.filling.calculation.flink.FlinkEnvironment;
import com.filling.calculation.flink.batch.FlinkBatchExecution;
import com.filling.calculation.flink.stream.FlinkStreamExecution;
import com.filling.calculation.flink.util.Engine;
import com.filling.calculation.flink.util.PluginType;
import com.filling.calculation.plugin.Plugin;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class ConfigBuilder {

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private String configFile;
    private Engine engine;
    private ConfigPackage configPackage = new ConfigPackage(Engine.FLINK.getEngine());
    private JSONObject config;
    private boolean streaming;
    private JSONObject envConfig;
    private RuntimeEnv env;
    private String stringConfig;

    public ConfigBuilder(String stringConfig, Engine engine) {
        this.engine = engine;
        this.stringConfig = stringConfig;
        this.config = setConfig(stringConfig);
        this.env = createEnv();
    }

    public ConfigBuilder(String configFile) {
        this.configFile = configFile;
        this.engine = Engine.NULL;
        this.env = createEnv();
        this.config = setConfig(stringConfig);
    }


    private JSONObject setConfig(String jsonConfig) {

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        System.out.println("[INFO] parsed config file: " + JSONObject.parseObject(jsonConfig).toJSONString());
        return JSONObject.parseObject(jsonConfig);
    }


    public JSONObject getEnvConfigs() {
        return envConfig;
    }

    public RuntimeEnv getEnv() {
        return env;
    }

    private boolean checkIsStreaming() {
        JSONArray sourceConfigList = config.getJSONArray((PluginType.SOURCE.getType()));
        return sourceConfigList.getJSONObject(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream");
    }

    /**
     * Get full qualified class name by reflection api, ignore case.
     **/
    private String buildClassFullQualifier(String name, PluginType classType) throws Exception {

        if (name.split("\\.").length == 1) {
            String packageName = null;
            Iterable<? extends Plugin> plugins = null;
            switch (classType) {
                case SOURCE:
                    packageName = configPackage.getSourcePackage();
                    Class baseSource = Class.forName(configPackage.getBaseSourcePackage());
                    plugins = ServiceLoader.load(baseSource);
                    break;
                case TRANSFORM:
                    packageName = configPackage.getTransformPackage();
                    Class baseTransform = Class.forName(configPackage.getBaseTransformPackage());
                    plugins = ServiceLoader.load(baseTransform);
                    break;
                case SINK:
                    packageName = configPackage.getSinkPackage();
                    Class baseSink = Class.forName(configPackage.getBaseSinkPackage());
                    plugins = ServiceLoader.load(baseSink);
                    break;
                default:
                    break;
            }

            String qualifierWithPackage = packageName + "." + name;
            for (Plugin plugin : plugins) {
                Class serviceClass = plugin.getClass();
                String serviceClassName = serviceClass.getName();
                String clsNameToLower = serviceClassName.toLowerCase();
                if (clsNameToLower.equals(qualifierWithPackage.toLowerCase())) {
                    return serviceClassName;
                }
            }
            return qualifierWithPackage;
        } else {
            return name;
        }
    }


    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        this.createEnv();
        this.createPlugins(PluginType.SOURCE);
        this.createPlugins(PluginType.TRANSFORM);
        this.createPlugins(PluginType.SINK);
    }

    public <T extends Plugin> List<T> createPlugins(PluginType type) {

        List<T> basePluginList = new ArrayList<>();

        JSONArray configList = config.getJSONArray(type.getType());

        configList.forEach(plugin -> {
            try {
                JSONObject jsonObjectplugin = JSONObject.parseObject(plugin.toString());
                final String className = buildClassFullQualifier(jsonObjectplugin.getString(PLUGIN_NAME_KEY), type);
                T t =  (T) Class.forName(className).newInstance();
                t.setConfig(jsonObjectplugin);
                basePluginList.add(t);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return basePluginList;
    }

    private RuntimeEnv createEnv() {
        envConfig = config.getJSONObject("env");
        streaming = checkIsStreaming();
        RuntimeEnv env = null;
        env = new FlinkEnvironment();
        ((FlinkEnvironment) env).setConfig(envConfig);
        env.prepare(streaming);
        return env;
    }


    public Execution createExecution() {


        Execution execution = null;
        switch (engine) {
            case SPARK:
//                SparkEnvironment sparkEnvironment = (SparkEnvironment) env;
//                if (streaming) {
//                    execution = new SparkStreamingExecution(sparkEnvironment);
//                } else {
//                    execution = new SparkBatchExecution(sparkEnvironment);
//                }
                break;
            case FLINK:
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) env;
                if (streaming) {
                    execution = new FlinkStreamExecution(flinkEnvironment);
                } else {
                    execution = new FlinkBatchExecution(flinkEnvironment);
                }
                break;
            default:
                break;
        }
        return execution;
    }


}
