package io.activedata.xnifi.processors.generate.sequence;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.*;

/**
 * 序列生成器，包装对序列的计算
 * Created by MattU on 2017/12/14.
 */
public class SequenceGenerator {
    private Map<String, SequenceStrategy> strategyMap = new LinkedHashMap<>();

    public void registy(SequenceStrategy strategy){
        Validate.notNull(strategy);
        strategyMap.put(strategy.getName(), strategy);
    }

    public List<String> getStrategyNames(){
        List<String> strtegyNames = new ArrayList<>();
        strtegyNames.addAll(strategyMap.keySet());
        return strtegyNames;
    }

    public Sequence next(Sequence seq){
        Validate.notNull(seq);

        String strategyName = seq.getStrategy();
        SequenceStrategy strategy = strategyMap.get(strategyName);
        Validate.notNull(strategy, "[" + strategyName + "]对应的策略对象不存在。");

        String valueText = seq.getValue();
        if (StringUtils.isNotBlank(valueText)){
            Object value = strategy.toValue(valueText, seq.getFormat());
            Object maxValue = strategy.toValue(seq.getMaxValue(), seq.getFormat());
            Object nextValue = strategy.next(value, seq.getStep());
            if (strategy.lte(nextValue, maxValue)){
                String nextValueText = strategy.toString(nextValue, seq.getFormat());
                return createSequence(seq, nextValueText,valueText);
            }else{
                return null; //当生成的序列值大于指定的MAX值时，返回空
            }
        }else{
            // 如果value为空时，代表该序列第一次初始化，直接使用初始化的值
            Object initValue = strategy.toValue(seq.getInitValue(), seq.getFormat());
            if (initValue == null){
                initValue = strategy.defaultValue(); //如果用户设置的初始值为空，则使用策略中定义的默认值
            }
            String initValueText = strategy.toString(initValue, seq.getFormat());
            seq.setValue(initValueText);
            seq.setPrevValue(null);
            seq.setLastUpdateTime(new Date());
            return seq;
        }
    }

    protected Sequence createSequence(Sequence ref, String value,String prevValue){
        Sequence seq = new Sequence();
        seq.setCode(ref.getCode());
        seq.setInitValue(ref.getInitValue());
        seq.setStep(ref.getStep());
        seq.setMaxValue(ref.getMaxValue());
        seq.setValue(value);
        seq.setFormat(ref.getFormat());
        seq.setStrategy(ref.getStrategy());
        seq.setPrevValue(prevValue);
        return seq;
    }
}
