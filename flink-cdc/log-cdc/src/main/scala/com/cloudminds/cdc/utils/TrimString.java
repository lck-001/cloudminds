package com.cloudminds.cdc.utils;

public class TrimString {
    /*
     * 删除开头字符串
     */
    public static String trimstart(String inStr, String prefix) {
        if (inStr.startsWith(prefix)) {
            return (inStr.substring(prefix.length()));
        }
        return inStr;
    }

    /*
     * 删除末尾字符串
     */
    public static String trimend(String inStr, String suffix) {
        if (inStr.endsWith(suffix)) {
            return (inStr.substring(0,inStr.length()-suffix.length()));
        }
        return inStr;
    }
}
