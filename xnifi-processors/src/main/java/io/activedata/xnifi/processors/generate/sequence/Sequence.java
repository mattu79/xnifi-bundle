package io.activedata.xnifi.processors.generate.sequence;


import io.activedata.xnifi.expression.Dates;

import java.util.Date;

/**
 * Created by MattU on 2017/12/14.
 */
public class Sequence {
    /**
     * 用于确定唯一的序列代码
     */
    protected String code;
    /**
     * 序列生成策略：
     * 数字序列：SEQ_NUMBER
     * 日期序列：SEQ_DATE
     */
    protected String strategy;

    /**
     * 序列步长
     * 数字序列的步长为数字，
     * 日期序列的步长为数字+单位，单位可以为Y/M/D对应年/月/日
     */
    protected String step;
    /**
     * 序列值的格式
     * 在为日期序列时有效，指定日期格式
     */
    protected String format;
    /**
     * 序列的初始化值
     */
    protected String initValue;
    /**
     * 序列最大值，可选，新生成的序列值不能超过该定义的大小
     */
    protected String maxValue;
    /**
     * 该序列本次的值
     */
    protected String value;
    /**
     * 该序列上次的值
     */
    protected String prevValue;
    /**
     * 最后一次更新时间
     */
    protected Date lastUpdateTime = new Date();

    public Sequence(String code, String strategy, String step, String format, String initValue, String maxValue, String value, String prevValue, Date lastUpdateTime) {
        this.code = code;
        this.strategy = strategy;
        this.step = step;
        this.format = format;
        this.initValue = initValue;
        this.maxValue = maxValue;
        this.value = value;
        this.prevValue = prevValue;
        this.lastUpdateTime = lastUpdateTime;
    }

    public Sequence() {
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getInitValue() {
        return initValue;
    }

    public void setInitValue(String initValue) {
        this.initValue = initValue;
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getPrevValue() {
        return prevValue;
    }

    public void setPrevValue(String prevValue) {
        this.prevValue = prevValue;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getLastUpdateTime() {
        return Dates.toStdTime(lastUpdateTime);
    }

    public Date dateLastUpdateTime(){
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public String toString() {
        return "Sequence{" +
                "code='" + code + '\'' +
                ", strategy='" + strategy + '\'' +
                ", step='" + step + '\'' +
                ", format='" + format + '\'' +
                ", initValue='" + initValue + '\'' +
                ", maxValue='" + maxValue + '\'' +
                ", value='" + value + '\'' +
                ", prevValue='" + prevValue + '\'' +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}
