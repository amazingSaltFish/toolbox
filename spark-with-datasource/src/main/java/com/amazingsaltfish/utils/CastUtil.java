package com.amazingsaltfish.utils;


import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: wmh
 * Create Time: 2021/6/27 1:04
 */
public class CastUtil {
    public static <T> List<T> castList(Object obj, Class<T> clazz) {
        if (obj instanceof List<?>) {
           return ((List<?>) obj).stream().map(x -> clazz.cast(x)).collect(Collectors.toList());
        }
        return null;
    }
}
