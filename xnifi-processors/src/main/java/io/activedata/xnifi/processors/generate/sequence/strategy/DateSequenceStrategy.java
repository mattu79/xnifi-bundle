package io.activedata.xnifi.processors.generate.sequence.strategy;

import io.activedata.xnifi.expression.Dates;
import io.activedata.xnifi.processors.generate.sequence.SequenceStrategy;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by MattU on 2017/12/14.
 */
public class DateSequenceStrategy implements SequenceStrategy<Date> {
    private static final String NAME = "SEQ_DATE";
    private static final String DEFAULT_PATTERN = "yyyy-MM-dd";
    private static final String[] DATE_PATTERNS = new String[]{DEFAULT_PATTERN, "yyyyMMdd"};

    Pattern RGX_STEP = Pattern.compile("(\\d+)(M|D|Y)?", Pattern.CASE_INSENSITIVE);


    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Date defaultValue() {
        Date defaultValue = new Date();
        try {
            defaultValue = DateUtils.parseDateStrictly("19800101", DATE_PATTERNS);
        } catch (ParseException e) {
        }
        return defaultValue;
    }

    @Override
    public Date toValue(String value, String format) {
        if (value == null)
            return null;

        try {
            if (StringUtils.isBlank(format)){
                return DateUtils.parseDateStrictly(value, DATE_PATTERNS);
            }else {
                return DateUtils.parseDateStrictly(value, format);
            }
        } catch (ParseException e) {
            throw new RuntimeException(ExceptionUtils.getMessage(e));
        }
    }

    @Override
    public String toString(Date value, String format) {
        if (StringUtils.isBlank(format))
            format = DEFAULT_PATTERN;

        return DateFormatUtils.format(value, format);
    }

    @Override
    public Date next(Date value, String step) {
        return Dates.after(value, step);
    }

    @Override
    public boolean lte(Date value1, Date value2) {
        return !value1.after(value2);
    }
}