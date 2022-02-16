package com.mingzhi.common.utils;

public class StringUtil {

    public static void assertNotBlank(String filed,String msg){
        if(org.apache.commons.lang3.StringUtils.isBlank(filed)){

            throw new IllegalArgumentException(msg);
        }

    }
}
