package com.cloudminds.cdc.utils;

import org.apache.commons.lang.text.StrSubstitutor;

import java.util.Map;
/**
 * 替换
 * @param source 源内容
 * @param parameter 占位符参数
 * @param prefix 占位符前缀 例如:${
 * @param suffix 占位符后缀 例如:}
 * @param enableSubstitutionInVariables 是否在变量名称中进行替换 例如:${system-${版本}}
 *
 * 转义符默认为'$'。如果这个字符放在一个变量引用之前，这个引用将被忽略，不会被替换 如$${a}将直接输出${a}
 * @return
 */

public class Placeholder {
    public static String replace(String source, Map<String, Object> parameter, String prefix, String suffix, boolean enableSubstitutionInVariables){
        //StrSubstitutor不是线程安全的类
        StrSubstitutor strSubstitutor = new StrSubstitutor(parameter,prefix, suffix);
        //是否在变量名称中进行替换
        strSubstitutor.setEnableSubstitutionInVariables(enableSubstitutionInVariables);
        return strSubstitutor.replace(source);
    }
}
