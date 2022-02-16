package com.mingzhi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LogUtils {

    public static final Logger logger;

    static {
        logger = Logger.getLogger("logger");
        if (ConfigUtils.IS_TEST) {
            logger.setLevel(Level.ALL);
        } else {
            logger.setLevel(Level.OFF);
        }
    }


    public static void out(String msg) {
        out(LogUtils.class, msg);
    }

    public static <T> void out(Class<T> callerClz, String msg) {

        if (logger.isInfoEnabled()) {
            System.out.println(callerClz.getName() + ":" + msg);
        }
    }
}
