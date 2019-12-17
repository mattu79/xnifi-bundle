package io.activedata.xnifi.processors.generate.sequence.strategy;

import io.activedata.xnifi.expression.Strings;
import io.activedata.xnifi.processors.generate.sequence.SequenceStrategy;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * Created by MattU on 2017/12/14.
 */
public class NumberSequenceStrategy implements SequenceStrategy<Long> {
    public static final String NAME = "SEQ_NUMBER";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Long defaultValue() {
        return 1L;
    }

    @Override
    public Long toValue(String value, String format) {
        return NumberUtils.toLong(value, 1L);
    }

    @Override
    public String toString(Long value, String format) {
        return Strings.toString(value);
    }

    @Override
    public Long next(Long value, String step) {
        long stepValue = NumberUtils.toLong(step, 1L);
        return value + stepValue;
    }

    @Override
    public boolean lte(Long value1, Long value2) {
        return value2.compareTo(value1) > 0;
    }
}
