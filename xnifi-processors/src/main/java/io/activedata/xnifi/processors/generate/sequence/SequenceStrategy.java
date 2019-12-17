package io.activedata.xnifi.processors.generate.sequence;

/**
 * Created by MattU on 2017/12/14.
 */
public interface SequenceStrategy<T> {
    String getName();

    T defaultValue();

    T toValue(String value, String format);

    String toString(T value, String format);

    T next(T value, String step);

    /**
     * value1是否小于等于value2
     * @param value1
     * @param value2
     * @return
     */
    boolean lte(T value1, T value2);
}
