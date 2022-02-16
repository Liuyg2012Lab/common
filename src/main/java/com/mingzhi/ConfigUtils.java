package com.mingzhi;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class ConfigUtils {
    private static final String fileName = "application.properties";
    private static Config configInfo = ConfigFactory.load(fileName);

    public static Boolean IS_TEST;

    public static String IOT_ES_HOST;

    public static String WFS_MYSQL_HOST;
    public static String WFS_MYSQL_USERNAME;
    public static String WFS_MYSQL_PWD;

    public static String ABI_MYSQL_HOST;
    public static String ABI_MYSQL_USERNAME;
    public static String ABI_MYSQL_PWD;


    public static String SEGI_MYSQL_HOST;
    public static String SEGI_MYSQL_USERNAME;
    public static String SEGI_MYSQL_PWD;

    public static String UAC_MYSQL_HOST;
    public static String UAC_MYSQL_USERNAME;
    public static String UAC_MYSQL_PWD;

    public static String IOT_MYSQL_HOST;
    public static String IOT_MYSQL_USERNAME;
    public static String IOT_MYSQL_PWD;

    public static String SDS_MYSQL_HOST;
    public static String SDS_MYSQL_USERNAME;
    public static String SDS_MYSQL_PWD;


    static {
        load();
    }

    public ConfigUtils() {
        if (configInfo == null) {
            configInfo = ConfigFactory.load(fileName);
        }
        getProperties();
    }

    public static void load() {
        if (configInfo == null) {
            configInfo = ConfigFactory.load(fileName);
        }
        getProperties();
    }

    public static void getProperties() {
        IS_TEST = configInfo.getBoolean("isTest");

        IOT_ES_HOST = configInfo.getString("iot.es.host");

        WFS_MYSQL_HOST = configInfo.getString("wfs.mysql.host");
        WFS_MYSQL_USERNAME = configInfo.getString("wfs.mysql.username");
        WFS_MYSQL_PWD = configInfo.getString("wfs.mysql.pwd");

        ABI_MYSQL_HOST = configInfo.getString("abi.mysql.host");
        ABI_MYSQL_USERNAME = configInfo.getString("abi.mysql.username");
        ABI_MYSQL_PWD = configInfo.getString("abi.mysql.pwd");

        SEGI_MYSQL_HOST = configInfo.getString("segi.mysql.host");
        SEGI_MYSQL_USERNAME = configInfo.getString("segi.mysql.username");
        SEGI_MYSQL_PWD = configInfo.getString("segi.mysql.pwd");

        UAC_MYSQL_HOST = configInfo.getString("uac.mysql.host");
        UAC_MYSQL_USERNAME = configInfo.getString("uac.mysql.username");
        UAC_MYSQL_PWD = configInfo.getString("uac.mysql.pwd");

        IOT_MYSQL_HOST = configInfo.getString("iot.mysql.host");
        IOT_MYSQL_USERNAME = configInfo.getString("iot.mysql.username");
        IOT_MYSQL_PWD = configInfo.getString("iot.mysql.pwd");

        SDS_MYSQL_HOST = configInfo.getString("sds.mysql.host");
        SDS_MYSQL_USERNAME = configInfo.getString("sds.mysql.username");
        SDS_MYSQL_PWD = configInfo.getString("sds.mysql.pwd");
    }


    public static void main(String[] args) {
        System.out.println(ConfigUtils.IS_TEST);
        System.out.println(ConfigUtils.IOT_ES_HOST);
        System.out.println(ConfigUtils.UAC_MYSQL_HOST);
        System.out.println(ConfigUtils.IOT_MYSQL_HOST);
        System.out.println(ConfigUtils.SDS_MYSQL_HOST);
    }

}