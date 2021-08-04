package com.filling.calculation.domain;

/**
 * datagen参数实体类
 */
public class DataGenField {

    private String Kind;
    private String type;
    private Integer min;
    private Integer max;
    private Integer length;
    private Integer start;
    private Integer end;

    public String getKind() {
        return "sequence".equals(Kind) ? "sequence" : "random";
    }

    public void setKind(String kind) {
        Kind = kind;
    }

    public Integer getMin() {
        return (min == null || min == 0) ? 1 : min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }

    public Integer getMax() {
        return (max == null || max == 0) ? Integer.MIN_VALUE - 1 : max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }

    public Integer getLength() {
        return (length == null || length == 0) ? 100 : length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getStart() {
        return (start == null || start == 0) ? 1 : start;
    }

    public void setStart(Integer start) {
        this.start = start;
    }

    public Integer getEnd() {
        return (end == null || end == 0) ? 100 : end;
    }

    public void setEnd(Integer end) {
        this.end = end;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "DataGenField{" +
                "Kind='" + Kind + '\'' +
                ", type='" + type + '\'' +
                ", min=" + min +
                ", max=" + max +
                ", length=" + length +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
