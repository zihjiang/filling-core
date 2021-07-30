package com.filling.calculation.flink.util;

public enum PluginType {
    SOURCE("source"),TRANSFORM("transform"),SINK("sink");

    private String type;
    private PluginType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
