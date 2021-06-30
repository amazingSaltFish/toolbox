package com.amazingsaltfish.utils.date;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.TextUtils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;


public class DateUtil {

    /**
     * 获取当前日期
     *
     * @return yyyy-MM-dd
     */
    public static String getNow() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
    }

    /**
     * 获取今天的起始时间
     *
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String getStart() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime());
    }

    /**
     * 获取今天的结束时间
     *
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String getEnd() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime());
    }

    /**
     * 获取昨天零点时间：yyyy-MM-dd HH:mm:ss
     *
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String getYestoryStart() {
        Calendar cal = Calendar.getInstance();
        //TODO 明天时间：cal.add(Calendar.DATE,1);后天：后天就是把1改成2 ，以此类推。
        cal.add(Calendar.DATE, -1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime());
    }

    /**
     * 获取昨天结束时间：yyyy-MM-dd HH:mm:ss
     *
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String getYesterdayEnd() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime());
    }

    /**
     * Java将Unix时间戳转换成指定格式日期字符串
     *
     * @param timestampString 时间戳 如："1473048265";
     * @param formats         要格式化的格式 默认："yyyy-MM-dd HH:mm:ss";
     * @return 返回结果 如："2016-09-05 16:06:42";
     */
    public static String TimeStamp2Date(String timestampString, String formats) {
        if (StringUtils.isBlank(timestampString)) {
            return null;
        }
        if (TextUtils.isEmpty(formats)) {
            formats = "yyyy-MM-dd HH:mm:ss";
        }

        Long timestamp = null;
        if (timestampString.length() == 13) {
            timestamp = Long.parseLong(timestampString);
        } else {
            timestamp = Long.parseLong(timestampString) * 1000;
        }

        return new SimpleDateFormat(formats, Locale.CHINA).format(new Date(timestamp));
    }

    /**
     * 获取指定格式的当前时间
     * example:
     *
     * @param formats Example: yyyy-MM-dd HH:mm:ss
     * @return String
     */
    public static String getCurrentFormatDate(String formats) {
        if (TextUtils.isEmpty(formats)) {
            formats = "yyyy-MM-dd HH:mm:ss";
        }
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat dateFormat = new SimpleDateFormat(formats);
        return dateFormat.format(date);
    }

    /**
     * 获取指定格式的当前时间--返回Timestamp类型
     *
     * @param formats
     * @return Timestamp
     */
    public static Timestamp getCurrentTimeStampFormat(String formats) {
        if (TextUtils.isEmpty(formats)) {
            formats = "yyyy-MM-dd HH:mm:ss";
        }
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat dateFormat = new SimpleDateFormat(formats);
        return Timestamp.valueOf(dateFormat.format(date));
    }

    /**
     * 获取两个时间的时间差，返回差距多少秒
     *
     * @param startTime 开始时间时间戳
     * @param endTime   结束时间时间戳
     * @return seconds
     * @throws ParseException 解析异常
     */
    public static int getTimeDiffSecond(Timestamp startTime, Timestamp endTime) throws ParseException {

        long diff = startTime.getTime() - endTime.getTime();
        return (int) (diff / (1000));
    }

    /**
     * 获取某日期往前多少天的日期
     *
     * @param nowDate   起始日期
     * @param beforeNum 往前的天数
     * @return
     * @CreateTime 2016-1-13
     */
    public static Date getBeforeDate(Date nowDate, Integer beforeNum) {
        Calendar calendar = Calendar.getInstance(); // 得到日历
        calendar.setTime(nowDate);// 把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, -beforeNum); // 设置为前beforeNum天
        return calendar.getTime(); // 得到前beforeNum天的时间
    }

    /**
     * 返回需要的日期形式
     * </p>
     * 如：style=“yyyy年MM月dd日 HH时mm分ss秒SSS毫秒”。 返回“xxxx年xx月xx日xx时xx分xx秒xxx毫秒”
     * </p>
     *
     * @param date
     * @param style
     * @return
     */
    public static String getNeededDateStyle(Date date, String style) {
        if (date == null) {
            date = new Date();
        }
        if (StringUtils.isEmpty(style)) {
            style = "yyyy年MM月dd日";
        }

        SimpleDateFormat sdf = new SimpleDateFormat(style);
        return sdf.format(date);
    }

    /**
     * 获取过去12个月的月份
     *
     * @return
     */
    public static String[] getLast12Months() {

        String[] last12Months = new String[12];

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MONTH, cal.get(Calendar.MONTH) + 1); //要先+1,才能把本月的算进去</span>
        for (int i = 0; i < 12; i++) {
            cal.set(Calendar.MONTH, cal.get(Calendar.MONTH) - 1); //逐次往前推1个月
            last12Months[11 - i] = cal.get(Calendar.YEAR) + String.format("%02d", (cal.get(Calendar.MONTH) + 1));
        }
        return last12Months;
    }

    /**
     * 更改时间格式
     *
     * @param time
     * @param format
     * @return
     */
    public static String formatTime(Timestamp time, String format) {
        if (TextUtils.isEmpty(format)) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        if (time != null) {
            return dateFormat.format(time);
        } else {
            return "";
        }

    }


    /**
     * 将时间格式字符串转换为时间 yyyy-MM-dd
     *
     * @param strDate
     * @return
     */
    public static Date strToDateFormat(String strDate) {
        Date date = null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            date = formatter.parse(strDate);
        } catch (Exception e) {
            return null;
        }
        return date;
    }

    /**
     * 获取某月最后一天
     *
     * @param date
     * @return
     */
    public static String getLastDayOfMonth(Date date) {
        int year = Integer.valueOf(getNeededDateStyle(date, "yyyy"));
        int month = Integer.valueOf(getNeededDateStyle(date, "MM"));

        Calendar cal = Calendar.getInstance();
        //设置年份
        cal.set(Calendar.YEAR, year);
        //设置月份
        cal.set(Calendar.MONTH, month - 1);
        //获取某月最大天数
        int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        //设置日历中月份的最大天数
        cal.set(Calendar.DAY_OF_MONTH, lastDay);
        //格式化日期
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String lastDayOfMonth = sdf.format(cal.getTime());
        return lastDayOfMonth;
    }

    /**
     * 将时间戳转为时间字符串
     *
     * @param millis  毫秒时间戳
     * @param pattern 时间格式
     * @return 时间字符串
     */
    public static String millis2String(long millis, String pattern) {
        return new SimpleDateFormat(pattern, Locale.CHINA).format(new Date(millis));
    }

    /**
     * 日期格式字符串转时间戳
     *
     * @param date    日期字符串
     * @param pattern 时间格式
     * @return 毫秒时间戳
     */
    public static long string2millis(String date, String pattern) throws ParseException {
        return new SimpleDateFormat(pattern, Locale.CHINA).parse(date).getTime();
    }


    public static String formatDateAsPattern(Date date, String parttern, Locale locale) {
        DateFormat dateFormat = new SimpleDateFormat(parttern, locale);
        return dateFormat.format(date);
    }


    public static String getMonthName(Date date, Locale locale) {
        DateFormat dateFormat = new SimpleDateFormat("MMM", locale);
        return dateFormat.format(date);
    }

    public static int getQuarter(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int month = calendar.get(Calendar.MONTH);

        String quarter = null;
        if (month <= 3) {
            return 1;
        } else if (month <= 6) {
            return 2;
        } else if (month <= 9) {
            return 3;
        } else {
            return 4;
        }

    }

    public static String getQuarterName(Date date, Locale locale) {
        int quarter = getQuarter(date);
        if (locale == Locale.CHINA) {
            switch (quarter) {
                case 1:
                    return "第一季";
                case 2:
                    return "第二季";
                case 3:
                    return "第三季";
                default:
                    return "第四季";
            }
        } else {
            switch (quarter) {
                case 1:
                    return "first quarter";
                case 2:
                    return "second quarter";
                case 3:
                    return "third quarter";
                default:
                    return "fourth quarter";
            }
        }

    }

    public static int getYearQuarter(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        String yearQuarter = calendar.get(Calendar.YEAR) + "0" + getQuarter(date);
        return Integer.parseInt(yearQuarter);
    }

    public static String getYearQuarterName(Date date, Locale locale) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR) + " " + getQuarterName(date, locale);
    }

    public static int getDayPart(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_YEAR);
    }

    public static int getWeekday(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_WEEK);
    }

    public static String getWeekdayName(Date date, Locale locale) {
        DateFormat dateFormat = new SimpleDateFormat("EEEE", locale);
        return dateFormat.format(date);
    }

    public static int isWeekend(Date date) {
        int dayofweek = getWeekday(date);
        if (dayofweek == 1 || dayofweek == 7) {
            return 1;
        } else {
            return 0;
        }

    }

    public static int isLastDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        Date lastDate = calendar.getTime();
        if (date.compareTo(lastDate) == 0) {
            return 1;
        } else {
            return 0;
        }

    }

    public static Date addOneDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, 1);
        return calendar.getTime();
    }


    /**
     * 时间转字符串
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String formatString(Date date, String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(date);
    }

    /**
     * 字符串转时间
     *
     * @param date
     * @return
     */
    public static Date formatDate(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            return dateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 时区转换
     *
     * @param date
     * @return
     */
    public static String parseTimeZone(String date) {
        date = date.replace("Z", " UTC");
        SimpleDateFormat timeZone = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z");
        Date lastDate = new Date();
        try {
            lastDate = timeZone.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(lastDate);
    }

    /**
     * 获取月份英文、中文
     *
     * @param language
     * @return
     */
    public static String getMonthName(String month, String language) {
        DateFormat dateFormat = new SimpleDateFormat("MM");
        try {
            Date date = dateFormat.parse(month);
            //中文月份
            if ("cn".equals(language)) {
                return month + "月";
            } else if ("en".equals(language)) {
                //英文月份
                dateFormat = new SimpleDateFormat("MMMMM", Locale.US);
                return dateFormat.format(date);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return month;
    }

    /**
     * 获取数字、中文、英文季度
     *
     * @param month
     * @param language
     * @return
     */
    public static String getQuarter(int month, String language) {
        String returnQuarter = "";
        if (month <= 3) {
            if ("num".equals(language)) {
                returnQuarter = "1";
            } else if ("cn".equals(language)) {
                returnQuarter = "第一季度";
            } else if ("en".equals(language)) {
                returnQuarter = "Q1";
            }
        } else if (month <= 6) {
            if ("num".equals(language)) {
                returnQuarter = "2";
            } else if ("cn".equals(language)) {
                returnQuarter = "第二季度";
            } else if ("en".equals(language)) {
                returnQuarter = "Q2";
            }
        } else if (month <= 9) {
            if ("num".equals(language)) {
                returnQuarter = "3";
            } else if ("cn".equals(language)) {
                returnQuarter = "第三季度";
            } else if ("en".equals(language)) {
                returnQuarter = "Q3";
            }
        } else if (month > 9) {
            if ("num".equals(language)) {
                returnQuarter = "4";
            } else if ("cn".equals(language)) {
                returnQuarter = "第四季度";
            } else if ("en".equals(language)) {
                returnQuarter = "Q4";
            }
        }
        return returnQuarter;
    }

    /**
     * 星期中文转换为阿拉伯数字
     *
     * @param weekDay
     * @return
     */
    public static int getWeekdayNum(String weekDay) {
        if ("一".equals(weekDay)) {
            return 1;
        } else if ("二".equals(weekDay)) {
            return 2;
        } else if ("三".equals(weekDay)) {
            return 3;
        } else if ("四".equals(weekDay)) {
            return 4;
        } else if ("五".equals(weekDay)) {
            return 5;
        } else if ("六".equals(weekDay)) {
            return 6;
        } else {
            return 7;
        }
    }

    /**
     * 是否月末
     *
     * @param dateStr
     * @return
     */
    public static int isLastDayOfMonth(String dateStr) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = dateFormat.parse(dateStr);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
            Date lastDate = calendar.getTime();
            if (date.compareTo(lastDate) == 0) {
                return 1;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 是否是周末
     *
     * @param weekDay
     * @return
     */
    public static int isWeekend(String weekDay) {
        if ("六".equals(weekDay) || "日".equals(weekDay)) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 日期类型 0为工作日，1为休息日，2为节假日
     * 如果不是假日，，是正常工作日不会有desc、value、status这三个字段
     * 如果是休息日但是要补班，status的值会为2
     * "type"="i"：是法定节日，但不是假日，不放假，比如：艾滋病日、腊八节等
     *
     * @param weekDay 星期几
     * @param status  正常上班和正常周末的日期没有该字段，1：节假日  2：补班
     * @return
     */
    public static int getDateType(String weekDay, String status) {
        if ("1".equals(status)) {
            return 2;
        } else if ("2".equals(status)) {
            return 0;
        } else if ("六".equals(weekDay) || "日".equals(weekDay)) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 日期类型名称
     *
     * @param weekDay
     * @param status
     * @return
     */
    public static String getDateTypeName(String weekDay, String status) {
        int dateType = getDateType(weekDay, status);
        if (0 == dateType) {
            return "工作日";
        } else if (1 == dateType) {
            return "休息日";
        } else if (2 == dateType) {
            return "节假日";
        } else {
            return "错误";
        }
    }

    /**
     * 节假日名称
     *
     * @param date
     * @param desc
     * @return
     */
    public static String getHolidayName(String date, String desc) {
        if (null == desc) {
            if ("11-11".equals(date)) {
                desc = "双十一";
            } else if ("06-18".equals(date)) {
                desc = "京东618";
            } else if ("12-12".equals(date)) {
                desc = "双十二";
            }
        }
        return desc;
    }

    /**
     * 获取一年的第几周（每周从周一开始算）
     */
    public static Integer getWeekOfYear(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setTime(date);
        return calendar.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * 获取指定日期的前/后N天
     *
     * @param millis 毫秒时间戳
     * @param amount 负数表示前N天，正数表示后N天
     * @return 毫秒时间戳
     */
    public static long getLastOrNextNDay(long millis, int amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        calendar.add(Calendar.DAY_OF_MONTH, amount);
        return calendar.getTimeInMillis();
    }

    /**
     * 获取指定日期的前/后N月
     *
     * @param millis 毫秒时间戳
     * @param amount 负数表示前N月，正数表示后N月
     * @return 毫秒时间戳
     */
    public static long getLastOrNextNMth(long millis, int amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        calendar.add(Calendar.MONTH, amount);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        return calendar.getTimeInMillis();
    }


    public static void main(String[] args) {
        System.out.println(getLastDayOfMonth(strToDateFormat("2018-02-03")));
    }

}
