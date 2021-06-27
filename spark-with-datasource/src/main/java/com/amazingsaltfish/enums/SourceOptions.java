package com.amazingsaltfish.enums;

/**
 * @author: wmh
 * Create Time: 2021/6/25 8:29
 */
public class SourceOptions {
    public static class Kudu{
        public final static String KUDU_MASTER = "kudu.master";
        public final static String KUDU_TABLE = "kudu.table";
    }

    /**
     * copy from spark jdbc options
     */
    public static class JDBC{
        public final static String URL = "url";
        public final static String USER = "user";
        public final static String PASSWORD = "password";
        public final static String DRIVER = "driver";
    }
}
