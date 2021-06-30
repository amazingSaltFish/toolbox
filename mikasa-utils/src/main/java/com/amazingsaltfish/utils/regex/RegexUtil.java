package com.amazingsaltfish.utils.regex;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author: wmh
 * Create Time: 2021/6/30 22:29
 *
 * This class for String regex mapping
 */

public class RegexUtil {

    /**
     * 判断字符串是否满足正则表达式
     *
     * @param regex 正则表达式
     * @param str 待检测字符串
     * @param flag Patter index 默认为0
     * @return 只要部分匹配就满足就返回真
     */
    public static Boolean findStr(String regex, String str,Integer...flag) {
        return getMatcher(regex, str, flag).find();
    }

    /**
     * 完全匹配
     *
     * @param regex 正则表达式
     * @param str 待检测字符串
     * @param flag Patter index 默认为0
     * @return 需要字符串完全匹配就返回真
     */
    public static Boolean wholeMatchStr(String regex, String str, Integer... flag) {
        return getMatcher(regex, str, flag).matches();
    }

    /**
     * 找出线性列表中满足正则表达式的结果集
     * @param regex 正则表达式
     * @param list 待检测的线性列表
     * @param flag Patter index 默认为0
     * @return 满足不完全匹的线性匹配表达式结果集
     */
    public static List<String> findList( String regex,List<String> list, Integer... flag) {
        return list.stream().filter(x -> findStr(regex, x, flag)).collect(Collectors.toList());
    }

    /**
     *  找出完全匹配的线性表结果集
     * @param regex 正则表达式
     * @param list 待检测线性列表
     * @param flag Pattern index 默认为0
     * @return 满足完全匹配的线性表达式
     */
    public static List<String> wholeMatchList(String regex, List<String> list, Integer... flag) {
        return list.stream().filter(x -> wholeMatchStr(regex, x, flag)).collect(Collectors.toList());
    }

    /**
     * 返回字符串中的满足正则表达式的group
     * @param regex 正则表达式
     * @param str 待检测的字符串
     * @param flag Pattern index 默认为0
     * @return 返回满足正则表达式中group的字符串
     */
    public static String findGroup(String regex, String str, Integer... flag) {
        Matcher matcher = getMatcher(regex, str, flag);
        matcher.find();
        return matcher.group(1);
    }

    /**
     * 找出字符串中满足正则表达式group的索引的正则表达式
     * @param regex 正则表达式
     * @param str 待检测字符串
     * @param groupIndex 正则表达式group组的索引
     * @param flag Pattern index 默认为0
     * @return 返回正则表达式中指定索引组的字符串
     */
    public static String findGroupByIndex(String regex, String str,Integer groupIndex, Integer... flag) {
        Matcher matcher = getMatcher(regex, str, flag);
        matcher.find();
        return matcher.group(groupIndex);
    }


    /**
     * 初始化生成正则匹配表达式的matcher，对象，其中flag可以指定
     * 使用:
     *     getMatcher("\\d","12345678");
     *     getMatcher("\\d","12346578",Pattern.COMMENTS);
     * @param regex 正则表达式
     * @param str 待检测字符串
     * @param flag Pattern index 默认为0
     * @return matcher 对象
     */
    public static Matcher getMatcher(String regex, String str, Integer...flag)  {
        int pflag = 0;
        try {
            if (flag.length!=0) {
                if(flag.length==1){
                    if (flag[0] instanceof Integer) {
                        pflag = flag[0];
                    }else {
                        throw new ClassCastException("param flag type cast error");
                    }
                }else {
                    throw new Exception("Please check param flag");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Pattern.compile(regex, pflag).matcher(str);
    }
}
