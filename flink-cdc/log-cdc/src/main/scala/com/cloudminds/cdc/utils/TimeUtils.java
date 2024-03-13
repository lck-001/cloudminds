package com.cloudminds.cdc.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;

public class TimeUtils {
    private static String[] parsePatterns = {"yyyy-MM-dd","yyyy年MM月dd日",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy/MM/dd",
            "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm", "yyyyMMdd"};

    public static Date parseDate(String str) {
        if (str == null) {
            return null;
        }
        try {
            return DateUtils.parseDate(str, parsePatterns);
        } catch (ParseException e) {
            return null;
        }
    }

    public static void main(String[] args) {
        Date date = parseDate("2022-02-24T11:08:16Z");
        System.out.println(date);
    }
}
