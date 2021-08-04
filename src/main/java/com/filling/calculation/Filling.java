package com.filling.calculation;


import com.alibaba.fastjson.JSONObject;
import com.filling.calculation.apis.BaseSink;
import com.filling.calculation.apis.BaseSource;
import com.filling.calculation.apis.BaseTransform;
import com.filling.calculation.config.ConfigBuilder;
import com.filling.calculation.domain.PreviewResult;
import com.filling.calculation.domain.RunModel;
import com.filling.calculation.env.Execution;
import com.filling.calculation.env.RuntimeEnv;
import com.filling.calculation.flink.util.Engine;
import com.filling.calculation.flink.util.PluginType;
import com.filling.calculation.plugin.Plugin;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Base64;
import java.util.List;


public class Filling {

//    public static void main(String[] args) {
//        OptionParser<CommandLineArgs> flinkParser = CommandLineUtils.flinkParser();
//        run(flinkParser, Engine.FLINK, args);
//    }
//
//    public static void run(OptionParser<CommandLineArgs> parser,Engine engine,String[] args){
//        Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(args).iterator()).asScala().toSeq();
//        Option<CommandLineArgs> option = parser.parse(seq, new CommandLineArgs("client", "application.conf", false));
//        if (option.isDefined()) {
//            CommandLineArgs commandLineArgs = option.get();
//            Common.setDeployMode(commandLineArgs.deployMode());
//            String configFilePath = getConfigFilePath(commandLineArgs, engine);
//            boolean testConfig = commandLineArgs.testConfig();
//            if (testConfig) {
//                new ConfigBuilder(configFilePath).checkConfig();
//                System.out.println("config OK !");
//            } else {
//                try {
//                    entryPoint(configFilePath, engine);
//                } catch (ConfigRuntimeException e) {
//                    showConfigError(e);
//                }catch (Exception e){
//                    showFatalError(e);
//                }
//            }
//        }
//    }
//
//    private static String getConfigFilePath(CommandLineArgs cmdArgs, Engine engine) {
//        String path = null;
//        path = cmdArgs.configFile();
//        return path;
//    }

//    private static void entryPoint(String configFile, Engine engine) {
//
//        ConfigBuilder configBuilder = new ConfigBuilder(configFile, engine);
//        List<BaseSource> sources = configBuilder.createPlugins(PluginType.SOURCE);
//        List<BaseTransform> transforms = configBuilder.createPlugins(PluginType.TRANSFORM);
//        List<BaseSink> sinks = configBuilder.createPlugins(PluginType.SINK);
//        Execution execution = configBuilder.createExecution();
//        baseCheckConfig(sources, transforms, sinks);
//        prepare(configBuilder.getEnv(), sources, transforms, sinks);
//        showWaterdropAsciiLogo();
//
//        execution.start(sources, transforms, sinks);
//    }

    public static List<PreviewResult> entryPoint(String jsonObject, Engine engine, RunModel runModel) throws Exception {

        ConfigBuilder configBuilder = new ConfigBuilder(jsonObject, engine);
        List<BaseSource> sources = configBuilder.createPlugins(PluginType.SOURCE);
        List<BaseTransform> transforms = configBuilder.createPlugins(PluginType.TRANSFORM);
        List<BaseSink> sinks = configBuilder.createPlugins(PluginType.SINK);
        Execution execution = configBuilder.createExecution();

//        prepare(configBuilder.getEnv(), sources, transforms, sinks);

        return execution.start(sources, transforms, sinks, runModel);
    }




    private static void showFatalError(Throwable throwable) {
        System.out.println(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        System.out.println("Fatal Error, \n");
        System.out.println("Reason: " + errorMsg + "\n");
        System.out.println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable));
        System.out.println(
                "\n===============================================================================\n\n\n");
    }

    private static void prepare(RuntimeEnv env, List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            pluginList.forEach(plugin -> plugin.prepare(env));
        }

    }

    public static void main(String[] args) throws Exception {


        if(args.length < 1) {
            System.out.println("for example: {'dd': 'sa'}");
        } else {
            RunModel runModel;

            if(args.length == 2) {
                runModel = RunModel.valueOf(args[1]);
            } else {
                runModel = RunModel.DEV;
            }

            final Base64.Decoder decoder = Base64.getDecoder();

            String jsonStr = new String(decoder.decode(args[0]), "UTF-8");

            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            System.out.println("json: " + jsonObject.toJSONString());
            System.out.println("model: " + runModel.toString());
            entryPoint(jsonStr, Engine.FLINK, runModel);
        }
    }
}
