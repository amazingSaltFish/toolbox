package com.amazingfish.base;

import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.spark.kudu.KuduWriteOptions;

/**
 * @Author: wmh
 * Create Time: 2021/6/21 2:06
 */
public class DefaultOptions {
    public static KuduWriteOptions defaultWriteOptions = new KuduWriteOptions(false, false, false, false, false);
}
