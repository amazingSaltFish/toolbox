package com.amazingfish.utils.time;

public class TimeUtil {

    /**
     * 时间段
     * 00:00~04:00
     * 04:00~06:00
     * 06:00~12:00
     * 12:00~13:00
     * 13:00~18:00
     * 18:00~24:00
     *
     * @param hours 小时
     * @return
     */
    public static String getTimePeriodStd(int hours, String language) {
        String timePeroidStd = null;
        if (hours >= 0 && hours < 4) {
            if ("en".equals(language)) {
                timePeroidStd = "wee hours";
            } else if ("cn".equals(language)) {
                timePeroidStd = "凌晨";
            } else if ("num".equals(language)) {
                timePeroidStd = "00:00~04:00";
            }
        } else if (hours >= 4 && hours < 6) {
            if ("en".equals(language)) {
                timePeroidStd = "early morning";
            } else if ("cn".equals(language)) {
                timePeroidStd = "清晨";
            } else if ("num".equals(language)) {
                timePeroidStd = "04:00~06:00";
            }
        } else if (hours >= 6 && hours < 12) {
            if ("en".equals(language)) {
                timePeroidStd = "morning";
            } else if ("cn".equals(language)) {
                timePeroidStd = "上午";
            } else if ("num".equals(language)) {
                timePeroidStd = "06:00~12:00";
            }
        } else if (hours >= 12 && hours < 13) {
            if ("en".equals(language)) {
                timePeroidStd = "midday";
            } else if ("cn".equals(language)) {
                timePeroidStd = "中午";
            } else if ("num".equals(language)) {
                timePeroidStd = "12:00~13:00";
            }
        } else if (hours >= 13 && hours < 18) {
            if ("en".equals(language)) {
                timePeroidStd = "afternoon";
            } else if ("cn".equals(language)) {
                timePeroidStd = "下午";
            } else if ("num".equals(language)) {
                timePeroidStd = "13:00~18:00";
            }
        } else {
            if ("en".equals(language)) {
                timePeroidStd = "night";
            } else if ("cn".equals(language)) {
                timePeroidStd = "晚上";
            } else if ("num".equals(language)) {
                timePeroidStd = "18:00~24:00";
            }
        }
        return timePeroidStd;
    }

    /**
     * 12时间段
     * 23:00~01:00
     * 01:00~03:00
     * 03:00~05:00
     * 05:00~07:00
     * 07:00~09:00
     * 09:00~11:00
     * 11:00~13:00
     * 13:00~15:00
     * 15:00~17:00
     * 17:00~19:00
     * 19:00~21:00
     * 21:00~23:00
     *
     * @param hours
     * @return
     */
    public static String getTimePeriod(int hours, String language) {
        String timePeriod = null;
        if (hours >= 23 || hours < 1) {
            if ("num".equals(language)) {
                timePeriod = "23:00~01:00";
            } else if ("cn".equals(language)) {
                timePeriod = "半夜";
            }
        } else if (hours >= 1 && hours < 3) {
            if ("num".equals(language)) {
                timePeriod = "01:00~03:00";
            } else if ("cn".equals(language)) {
                timePeriod = "凌晨";
            }
        } else if (hours >= 3 && hours < 5) {
            if ("num".equals(language)) {
                timePeriod = "03:00~05:00";
            } else if ("cn".equals(language)) {
                timePeriod = "黎明";
            }
        } else if (hours >= 5 && hours < 7) {
            if ("num".equals(language)) {
                timePeriod = "05:00~07:00";
            } else if ("cn".equals(language)) {
                timePeriod = "清晨";
            }
        } else if (hours >= 7 && hours < 9) {
            if ("num".equals(language)) {
                timePeriod = "07:00~09:00";
            } else if ("cn".equals(language)) {
                timePeriod = "早上";
            }
        } else if (hours >= 9 && hours < 11) {
            if ("num".equals(language)) {
                timePeriod = "09:00~11:00";
            } else if ("cn".equals(language)) {
                timePeriod = "上午";
            }
        } else if (hours >= 11 && hours < 13) {
            if ("num".equals(language)) {
                timePeriod = "11:00~13:00";
            } else if ("cn".equals(language)) {
                timePeriod = "中午";
            }
        } else if (hours >= 13 && hours < 15) {
            if ("num".equals(language)) {
                timePeriod = "13:00~15:00";
            } else if ("cn".equals(language)) {
                timePeriod = "午后";
            }
        } else if (hours >= 15 && hours < 17) {
            if ("num".equals(language)) {
                timePeriod = "15:00~17:00";
            } else if ("cn".equals(language)) {
                timePeriod = "下午";
            }
        } else if (hours >= 17 && hours < 19) {
            if ("num".equals(language)) {
                timePeriod = "17:00~19:00";
            } else if ("cn".equals(language)) {
                timePeriod = "傍晚";
            }
        } else if (hours >= 19 && hours < 21) {
            if ("num".equals(language)) {
                timePeriod = "19:00~21:00";
            } else if ("cn".equals(language)) {
                timePeriod = "晚上";
            }
        } else if (hours >= 21 && hours < 23) {
            if ("num".equals(language)) {
                timePeriod = "21:00~23:00";
            } else if ("cn".equals(language)) {
                timePeriod = "深夜";
            }
        }
        return timePeriod;
    }

    /**
     * 是否属于工作时间范围
     *
     * @param hours
     * @return 0：否  1：是
     */
    public static int isWorkingTime(int hours, int minutes) {
        double h = hours + ((double) minutes) / 60;
        if (h >= 9 && h <= 18) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 01-10 10
     * 11-20 20
     * 21-30 30
     * 31-40 40
     * 41-50 50
     * 51-00 00
     *
     * @param minutes
     * @return
     */
    public static String getMinPeroid(int minutes, int hours) {

//        String h = "" + hours;
//
//        if (hours < 10) {
//            h = "0" + hours;
//        }
//
//        if (minutes == 0) {
//            return h + ":00";
//        } else if (minutes < 10) {
//            return h + ":10";
//        } else if (minutes >= 10 && minutes < 20) {
//            return h + ":20";
//        } else if (minutes >= 20 && minutes < 30) {
//            return h + ":30";
//        } else if (minutes >= 30 && minutes < 40) {
//            return h + ":40";
//        } else if (minutes >= 40 && minutes < 50) {
//            return h + ":50";
//        }
//        // (minutes > 50)
//        else {
//            if (hours != 23) {
//                if (hours < 9) {
//                    return "0" + (hours + 1) + ":00";
//                }
//                return (hours + 1) + ":00";
//            } else {
//                return "24:00";
//            }
//        }
//        下面一行等同于上面的代码作用
//        00:00<=time<00:10  结果为:00:10
//        00:10<=time<00:20  结果为:00:20
//        00:20<=time<00:30  结果为:00:30
//        00:30<=time<00:40  结果为:00:40
//        00:40<=time<00:50  结果为:00:50
//        00:50<=time<01:00  结果为:01:00
//
        return String.format("%02d:%d0", (hours == 23 && (minutes / 10 + 1 == 6)) ? 24 : hours, (minutes / 10 + 1) == 6 ? 0 : (minutes / 10 + 1));

    }

    /**
     * 是否是经销商工作时间
     */
    public static int isDealerWorkTime(int hours, int minutes) {
        double h = hours + ((double) minutes) / 60;
        if (h >= 9 && h <= 17) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 每5分钟切片
     */
    public static String timePeroid5Min(String hours, int minutes) {
        switch (minutes / 5) {
            case 0:
                return hours + ":00~" + hours + ":05";
            case 1:
                return hours + ":05~" + hours + ":10";
            case 2:
                return hours + ":10~" + hours + ":15";
            case 3:
                return hours + ":15~" + hours + ":20";
            case 4:
                return hours + ":20~" + hours + ":25";
            case 5:
                return hours + ":25~" + hours + ":30";
            case 6:
                return hours + ":30~" + hours + ":35";
            case 7:
                return hours + ":35~" + hours + ":40";
            case 8:
                return hours + ":40~" + hours + ":45";
            case 9:
                return hours + ":45~" + hours + ":50";
            case 10:
                return hours + ":50~" + hours + ":55";
            case 11:
                return hours + ":55~" + String.format("%02d", (Integer.valueOf(hours) + 1)) + ":00";
            default:
                return null;
        }
    }

    /**
     * 每10分钟切片
     */
    public static String timePeroid10Min(String hours, int minutes) {
        switch (minutes / 10) {
            case 0:
                return hours + ":00~" + hours + ":10";
            case 1:
                return hours + ":10~" + hours + ":20";
            case 2:
                return hours + ":20~" + hours + ":30";
            case 3:
                return hours + ":30~" + hours + ":40";
            case 4:
                return hours + ":40~" + hours + ":50";
            case 5:
                return hours + ":50~" + String.format("%02d", (Integer.valueOf(hours) + 1)) + ":00";
            default:
                return null;
        }
    }
}
