package io.activedata.xnifi.expression;

import org.apache.commons.lang3.LocaleUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 日期对象相关工具
 *
 * @author Matt.U
 */
public class Dates {

    private static Pattern RGX_PERIOD = Pattern.compile("(\\d+)(M|D|Y)?", Pattern.CASE_INSENSITIVE);

    private static final Logger LOG = LoggerFactory.getLogger(Dates.class);
    /**
     * 标准日期格式
     */
    private static final String DATE_FORMAT_STD = "yyyy-MM-dd";

    /**
     * 欧美日期格式
     */
    private static final String DATE_FORMAT_US = "yyyy/MM/dd";

    /**
     * 中国日期格式
     */
    private static final String DATE_FORMAT_CN = "yyyy年MM月dd日";

    /**
     * 中国日期格式2
     */
    private static final String DATE_FORMAT_CN2 = "yy年MM月dd日";

    /**
     * 全数字日期格式
     */
    private static final String DATE_FORMAT_DIGITAL = "yyyyMMdd";

    /**
     * UTC日期格式
     */
    private static final String DATE_FORMAT_UTC = "yyyy-MM-dd'T'";

    /**
     * 标准时间格式
     */
    private static final String TIME_FORMAT_STD = "yyyy-MM-dd HH:mm:ss";

    /**
     * 全数字时间格式
     */
    private static final String TIME_FORMAT_DIGITAL = "yyyyMMddHHmmss";

    private static final String TIME_FORMAT_DIGITAL12 = "yyyyMMddHHmm";

    private static final String TIME_FORMAT_DIGITAL3 = "yyyyMM";

    /**
     * 用于转换日期到年份yyyy
     */
    private static final String DATE_FORMAT_YEAR = "yyyy";

    /**
     * 用于转换日期到星期yyyyww
     */
    private static final String DATE_FORMAT_WEEK = "yyyyww";

    /**
     * UTC时间格式，带时区
     */
    private static final String TIME_FORMAT_UTC = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * UTC时间格式，带时区
     */
    private static final String TIME_FORMAT_UTC1 = "yyyyMMdd'T'HHmmss'Z'";

    /**
     * UTC时间格式，带时区
     */
    private static final String TIME_FORMAT_UTC2 = "yyyyMMdd'T'HHmmss";

    /**
     * UTC时间格式，带时区
     */
    private static final String TIME_FORMAT_UTC3 = "yyyy-MM-dd'T'HH:mm:ss";

    /**
     * UTC时间格式，带时区
     */
    private static final String TIME_FORMAT_UTC4 = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final String[] DATE_OR_TIME_PATTERNS = new String[]{
            DATE_FORMAT_DIGITAL, DATE_FORMAT_STD, DATE_FORMAT_UTC,
            DATE_FORMAT_US, DATE_FORMAT_CN, DATE_FORMAT_CN2,
            TIME_FORMAT_DIGITAL, TIME_FORMAT_STD, TIME_FORMAT_UTC, TIME_FORMAT_UTC1, TIME_FORMAT_UTC2, TIME_FORMAT_UTC3, TIME_FORMAT_UTC4,
            TIME_FORMAT_DIGITAL12, TIME_FORMAT_DIGITAL3};

    private static final String[] DATE_PATTERNS = new String[]{
            DATE_FORMAT_DIGITAL, DATE_FORMAT_STD, DATE_FORMAT_UTC,
            DATE_FORMAT_US, DATE_FORMAT_CN, DATE_FORMAT_CN2};

    private static final String[] TIME_PATTERNS = new String[]{
            TIME_FORMAT_DIGITAL, TIME_FORMAT_STD, TIME_FORMAT_UTC, TIME_FORMAT_UTC1, TIME_FORMAT_UTC2, TIME_FORMAT_UTC3,
            TIME_FORMAT_DIGITAL12, TIME_FORMAT_DIGITAL3};

    private static final String[] UTC_PATTERNS = new String[]{
            TIME_FORMAT_UTC, TIME_FORMAT_UTC1, TIME_FORMAT_UTC2, TIME_FORMAT_UTC3, TIME_FORMAT_UTC4
    };

    private static final Date DEFAULT_DATE = fromText("1800-01-01");
    public static final String INVALID_AGE = "-1";

    /**
     * 将日期（时间）文本转换成Date对象，如果格式不对则返回null
     *
     * @param dateOrTimeAsText
     * @return
     */
    public static final Date fromText(String dateOrTimeAsText) {
        if (StringUtils.isBlank(dateOrTimeAsText)) {
            return null;
        }
        try {
            return DateUtils.parseDateStrictly(dateOrTimeAsText, DATE_OR_TIME_PATTERNS);
        } catch (Exception e) {
            LOG.warn("日期类型[{}]转换时出错！", dateOrTimeAsText);
        }

        return null;
    }

    /**
     * 将日期（时间）文本转换成Date对象，如果格式不对则返回默认值
     *
     * @param dateOrTimeAsText
     * @param defaultValue
     * @return
     */
    public static final Date fromText(String dateOrTimeAsText, Date defaultValue) {
        if (StringUtils.isBlank(dateOrTimeAsText))
            return defaultValue;
        try {
            return DateUtils.parseDateStrictly(dateOrTimeAsText, DATE_OR_TIME_PATTERNS);
        } catch (Exception e) {
            LOG.warn("日期类型[{}]转换时出错！", dateOrTimeAsText);
        }

        return defaultValue;
    }

    /**
     * 将日期文本转换成Date对象，如果格式不对则返回null
     *
     * @param dateAsText
     * @return
     */
    public static final Date fromDateText(String dateAsText) {
        if (StringUtils.isBlank(dateAsText))
            return null;

        try {
            return DateUtils.parseDateStrictly(dateAsText, DATE_PATTERNS);
        } catch (Exception e) {
            LOG.warn("日期类型[{}]转换时出错！", dateAsText);
        }

        return null;
    }

    /**
     * 将日期文本转换成Date对象，如果格式不对则返回默认值
     *
     * @param dateAsText
     * @param defaultValue
     * @return
     */
    public static final Date fromDateText(String dateAsText, Date defaultValue) {
        try {
            return DateUtils.parseDateStrictly(dateAsText, DATE_PATTERNS);
        } catch (Exception e) {
            LOG.warn("日期类型[{}]转换时出错！", dateAsText);
        }

        return defaultValue;
    }

    /**
     * 将时间文本转换成Date对象，如果格式不对则返回null
     *
     * @param timeAsText
     * @return
     */
    public static final Date fromTimeText(String timeAsText) {
        if (StringUtils.isBlank(timeAsText))
            return null;
        try {
            return DateUtils.parseDateStrictly(timeAsText, TIME_PATTERNS);
        } catch (Exception e) {
            LOG.warn("日期类型[{}]转换时出错！", timeAsText);
        }

        return null;
    }

    /**
     * 将日期（时间）文本转换成Date对象，如果格式不对则返回默认值
     *
     * @param timeAsText
     * @param defaultValue
     * @return
     */
    public static final Date fromTimeText(String timeAsText, Date defaultValue) {
        if (StringUtils.isBlank(timeAsText))
            return defaultValue;

        try {
            return DateUtils.parseDateStrictly(timeAsText, TIME_PATTERNS);
        } catch (Exception e) {
            LOG.warn("日期类型[{}]转换时出错！", timeAsText);
        }

        return defaultValue;
    }

    public static final Date fromUtc(String utcTime){
        if (StringUtils.isBlank(utcTime))
            return null;

        try {
            for (String pattern : UTC_PATTERNS){
                SimpleDateFormat sdf = new SimpleDateFormat(pattern);
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

                try {
                    Date date = sdf.parse(utcTime);
                    return date;
                }
                catch (Exception e){
                    // 忽略错误
                }
            }

            throw new RuntimeException("无法解析日期值" + utcTime); // 多种格式都无法解析，则抛出异常
        } catch (Exception e) {
            LOG.warn("日期类型[{}]转换时出错！", utcTime);
        }

        return null;
    }

    /**
     * 将Date转换成标准日期字符串，如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toStdDate(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_STD);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成标准日期字符串，如果date为null，则返回默认值
     *
     * @param date
     * @return
     */
    public static final String toStdDate(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_STD);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将Date转换成标准日期字符串，如果date为null，则返回默认值
     *
     * @param dateText
     * @return
     */
    public static final String toStdDate(String dateText, String defaultDate) {
        Date date = fromText(dateText);
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_STD);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将Date转换成标准时间字符串，如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toStdTime(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_STD);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成标准时间字符串，如果date为null，则返回默认值
     *
     * @param date
     * @return
     */
    public static final String toStdTime(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_STD);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将Date转换成标准时间字符串，如果date为null，则返回默认值
     *
     * @param timeAsText
     * @return
     */
    public static final String toStdTime(String timeAsText, String defaultDate) {
        Date date = fromText(timeAsText);
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_STD);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将时间文本转换成标准时间字符串，如果时间文本不合法，则返回空串
     *
     * @param timeText
     * @return
     */
    public static final String toStdTime(String timeText) {
        Date date = fromText(timeText);
        return toStdTime(date);
    }

    /**
     * 将Date转换成包含日期的数值型日期字符串，格式为：yyyyMMdd<br>
     * 如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toDigitalDate(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_DIGITAL);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成包含日期的数值型日期字符串，格式为：yyyyMMdd<br>
     * 如果date为null，则返回默认日期文本
     *
     * @param date
     * @return
     */
    public static final String toDigitalDate(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_DIGITAL);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将日期文本转换成包含日期的数值型日期字符串，格式为：yyyyMMdd<br>
     * 如果日期文本不合法，则返回空串
     *
     * @param dateText
     * @return
     */
    public static final String toDigitalDate(String dateText) {
        Date date = fromText(dateText);
        return toDigitalDate(date);
    }

    /**
     * 将日期文本转换成包含日期的数值型日期字符串，格式为：yyyyMMdd<br>
     * 如果日期文本不合法，则返回默认日期文本
     *
     * @param dateText
     * @return
     */
    public static final String toDigitalDate(String dateText, String defaultDate) {
        Date date = fromText(dateText);
        return toDigitalDate(date, defaultDate);
    }

    /**
     * 将Date转换成包含日期的数值型日期字符串，格式为：yyyyMM<br>
     * 如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toDigitalMonth(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_DIGITAL3);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成包含日期的数值型日期字符串，格式为：yyyyMM<br>
     * 如果date为null，则返回默认值
     *
     * @param date
     * @return
     */
    public static final String toDigitalMonth(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_DIGITAL3);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将日期文本转换成包含日期的数值型日期字符串，格式为：yyyyMM<br>
     * 如果日期文本不合法，则返回空串
     *
     * @param dateText 日期文本
     * @return
     */
    public static final String toDigitalMonth(String dateText) {
        Date date = fromText(dateText);
        return toDigitalMonth(date);
    }

    /**
     * 将日期文本转换成包含日期的数值型日期字符串，格式为：yyyyMM<br>
     * 如果日期文本不合法，则返回默认日期文本
     *
     * @param dateText 日期文本
     * @return
     */
    public static final String toDigitalMonth(String dateText, String defaultDate) {
        Date date = fromText(dateText);
        return toDigitalMonth(date, defaultDate);
    }

    /**
     * 将Date转换成包含星期的数值型日期字符串，格式为yyyyww<br>
     * 如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toDigitalWeek(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_WEEK);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成包含星期的数值型日期字符串，格式为yyyyww<br>
     * 如果date为null，则返回默认日期文本
     *
     * @param date
     * @return
     */
    public static final String toDigitalWeek(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_WEEK);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将日期文本转换成包含星期的数值型日期字符串，格式为yyyyww<br>
     * 如果日期文本不合法，则返回空串
     *
     * @param dateText
     * @return
     */
    public static final String toDigitalWeek(String dateText) {
        Date date = fromText(dateText);
        return toDigitalWeek(date);
    }

    /**
     * 将日期文本转换成包含星期的数值型日期字符串，格式为yyyyww<br>
     * 如果日期文本不合法，则返回默认日期文本
     *
     * @param dateText
     * @return
     */
    public static final String toDigitalWeek(String dateText, String defaultDate) {
        Date date = fromText(dateText);
        return toDigitalWeek(date, defaultDate);
    }

    /**
     * 将Date转换成包含年份的数值型日期字符串，格式为yyyy<br>
     * 如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toDigitalYear(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_YEAR);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成包含年份的数值型日期字符串，格式为yyyy<br>
     * 如果date为null，则返回默认日期文本
     *
     * @param date
     * @return
     */
    public static final String toDigitalYear(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_YEAR);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将日期文本转换成包含年份的数值型日期字符串，格式为yyyy<br>
     * 如果日期文本不合法，则返回空串
     *
     * @param dateText 日期文本
     * @return
     */
    public static final String toDigitalYear(String dateText) {
        Date date = fromText(dateText);
        return toDigitalYear(date);
    }

    /**
     * 将日期文本转换成包含年份的数值型日期字符串，格式为yyyy<br>
     * 如果日期文本不合法，则返回默认日期文本
     *
     * @param dateText 日期文本
     * @return
     */
    public static final String toDigitalYear(String dateText, String defaultDate) {
        Date date = fromText(dateText);
        return toDigitalYear(date, defaultDate);
    }

    /**
     * 将Date转换成数值型时间字符串，如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toDigitalTime(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_DIGITAL);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成数值型时间字符串，如果date为null，则默认日期文本
     *
     * @param date
     * @return
     */
    public static final String toDigitalTime(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_DIGITAL);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将时间文本转换成数值型时间字符串，如果日期文本不合法，则返回空串
     *
     * @param timeText 时间文本
     * @return
     */
    public static final String toDigitalTime(String timeText) {
        Date date = fromText(timeText);
        return toDigitalTime(date);
    }

    /**
     * 将时间文本转换成数值型时间字符串，如果日期文本不合法，则返回默认值
     *
     * @param timeText 时间文本
     * @return
     */
    public static final String toDigitalTime(String timeText, String defaultDate) {
        Date date = fromText(timeText);
        return toDigitalTime(date, defaultDate);
    }

    /**
     * 将Date转换成UTC型日期字符串，如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toUtcDate(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_UTC);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成UTC型日期字符串，如果date为null，则返回默认值
     *
     * @param date
     * @return
     */
    public static final String toUtcDate(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_UTC);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将Date转换成UTC型时间字符串，如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toUtcTime(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_UTC);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换成UTC型时间字符串，如果date为null，则返回默认值
     *
     * @param date
     * @return
     */
    public static final String toUtcTime(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_UTC);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将Date转换为中国日期格式字符串，如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toCnDate(Date date) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_CN);
            return sdf.format(date);
        } else
            return "";
    }

    /**
     * 将Date转换为中国日期格式字符串，如果date为null，则返回空串
     *
     * @param date
     * @return
     */
    public static final String toCnDate(Date date, String defaultDate) {
        if (date != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_CN);
            return sdf.format(date);
        } else
            return defaultDate;
    }

    /**
     * 将日期文本转换为中国日期格式字符串，如果日期文本不合法则返回空串
     *
     * @param dateText
     * @return
     */
    public static final String toCnDate(String dateText) {
        Date date = fromText(dateText);
        return toCnDate(date);
    }

    /**
     * 将日期文本转换为中国日期格式字符串，如果日期文本不合法则返回默认日期文本
     *
     * @param dateText
     * @return
     */
    public static final String toCnDate(String dateText, String defaultDate) {
        Date date = fromText(dateText);
        return toCnDate(date, defaultDate);
    }

    /**
     * 取得Date的timestamp值（毫秒）
     * @param date
     * @return
     */
    public static long toValue(Date date){
        if (date != null)
            return date.getTime();
        else
            return 0;
    }

    /**
     * 将Date转成timestamp字符串
     * @param date
     * @return
     */
    public static String toTimestamp(Date date){
        if (date != null)
            return Strings.toString(date.getTime());
        else
            return "0";
    }

    /**
     * 根据给出的生日计算现在的年龄
     *
     * @param birthDate 出生日期
     * @return 年龄
     */
    public static final String age(Date birthDate) // throws Exception
    {
        if (birthDate != null) {
            Date now = new Date();
            String age = DurationFormatUtils.formatPeriod(birthDate.getTime(), now.getTime(), "y");
            return age;
        } else {
            return INVALID_AGE;
        }
    }

    /**
     * 根据给出的出生日期文本计算现在的年龄
     *
     * @param birthDateText 出生日期文本
     * @return 年龄
     */
    public static final String age(String birthDateText) {
        Date birthdate = fromText(birthDateText);
        if (birthdate != null) {
            return age(birthdate);
        } else {
            return INVALID_AGE;
        }
    }

    /**
     * 根据给出的出生时间和结束时间计算年龄
     *
     * @param birthDate 出生日期
     * @param endDate   截止时间
     * @return 年龄
     */
    public static final String age(Date birthDate, Date endDate) {
        if (birthDate != null && endDate != null) {
            try {
                String age = DurationFormatUtils.formatPeriod(birthDate.getTime(), endDate.getTime(), "y");
                return age;
            } catch (Exception e) {
                //忽略异常
            }
        }

        return INVALID_AGE;
    }

    /**
     * 根据给出的出生日期文本和结束时间文本计算年龄
     *
     * @param birthDateText 出生日期文本
     * @param endDateText   截止时间文本
     * @return 年龄
     */
    public static final String age(String birthDateText, String endDateText) {
        Date birthDate = fromText(birthDateText);
        Date endDate = fromText(endDateText);
        if (birthDate != null && endDate != null) {
            return age(birthDate, endDate);
        } else {
            return INVALID_AGE;
        }
    }

    public static java.sql.Date toSqlDate(String dateAsText) {
        Date date = fromText(dateAsText);
        if (date != null)
            return new java.sql.Date(date.getTime());
        else
            return null;
    }

    public static Date now() {
        return new Date();
    }

    public static Timestamp timestamp(){
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * 计算某时间段（N天/月/年）之前的时间。
     * @param value
     * @param peroid 时间段，数字+单位，单位可以是D代表日，M代表月，Y代表年
     * @return
     */
    public static Date before(Date value, String peroid) {
        Validate.notBlank(peroid);
        Matcher matcher = RGX_PERIOD.matcher(peroid);
        if (matcher.matches()) {
            if (matcher.groupCount() == 2) {
                int amount = NumberUtils.toInt(matcher.group(1), 1);
                String unit = matcher.group(2);
                if ("Y".equalsIgnoreCase(unit))
                    return DateUtils.addYears(value, -amount);
                else if ("M".equalsIgnoreCase(unit))
                    return DateUtils.addMonths(value, -amount);
                else
                    return DateUtils.addDays(value, -amount);
            }
        }

        throw new RuntimeException("非法的时间段[" + peroid + "]，请使用数字+D/M/Y形式。");
    }

    /**
     * 计算某时间段（N天/月/年）之后的时间。
     * @param value
     * @param peroid 时间段，数字+单位，单位可以是D代表日，M代表月，Y代表年
     * @return
     */
    public static Date after(Date value, String peroid) {
        Validate.notBlank(peroid);
        Matcher matcher = RGX_PERIOD.matcher(peroid);
        if (matcher.matches()) {
            if (matcher.groupCount() == 2) {
                int amount = NumberUtils.toInt(matcher.group(1), 1);
                String unit = matcher.group(2);
                if ("Y".equalsIgnoreCase(unit))
                    return DateUtils.addYears(value, amount);
                else if ("M".equalsIgnoreCase(unit))
                    return DateUtils.addMonths(value, amount);
                else
                    return DateUtils.addDays(value, amount);
            }
        }

        throw new RuntimeException("非法的时间段[" + peroid + "]，请使用数字+D/M/Y形式。");
    }

    public static Date before(String peroid) {
        return before(new Date(), peroid);
    }

    public static Date after(String peroid) {
        return after(new Date(), peroid);
    }
}

